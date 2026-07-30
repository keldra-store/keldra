//! Durable, scope-free transaction sessions for the public MVCC API path.

use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, IteratorMode, Options, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    mvcc_node_runtime::{CommitOutcome, MvccNodeRuntime},
    mvcc_transaction::{
        BundleReplicator, CertificationResult, CommitVersion, DurabilityLevel, LogicalKey,
        ObjectShardManifestReference, PointObservation, PreparedBundleStore, RangeObservation,
        ReadConsistency, TransactionBundle, TransactionBundleBuilder, WriteOperation,
    },
};

pub const MVCC_TRANSACTION_COLUMN_FAMILIES: [&str; 2] =
    ["mvcc_open_transactions", "mvcc_transaction_idempotency"];
const CF_TRANSACTIONS: &str = MVCC_TRANSACTION_COLUMN_FAMILIES[0];
const CF_IDEMPOTENCY: &str = MVCC_TRANSACTION_COLUMN_FAMILIES[1];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionHandle {
    pub cluster_id: String,
    pub transaction_id: String,
    pub snapshot_version: CommitVersion,
    pub expires_at_unix_ms: u64,
    pub durability: DurabilityLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionRegistryStatus {
    pub cluster_id: String,
    pub transaction_id: String,
    pub snapshot_version: CommitVersion,
    pub expires_at_unix_ms: u64,
    pub state: &'static str,
    pub result: Option<CertificationResult>,
    pub durability: DurabilityLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionBinding {
    pub cluster_id: String,
    pub durability: DurabilityLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedLogicalMutation {
    pub key: LogicalKey,
    pub observed_version: Option<CommitVersion>,
    pub value: Option<Vec<u8>>,
}

#[async_trait]
pub trait TransactionRuntime: Send + Sync {
    async fn transaction_snapshot(&self, consistency: ReadConsistency) -> Result<CommitVersion>;
    async fn commit_transaction_bundle(
        &self,
        bundle: TransactionBundle,
        durability: DurabilityLevel,
    ) -> Result<CommitOutcome>;
    fn apply_transaction_decision(
        &self,
        bundle: TransactionBundle,
        result: CertificationResult,
    ) -> Result<CommitOutcome>;
}

#[async_trait]
impl<S, R, C> TransactionRuntime for MvccNodeRuntime<S, R, C>
where
    S: PreparedBundleStore,
    R: BundleReplicator,
    C: anvil_mvcc_consensus::Consensus,
{
    async fn transaction_snapshot(&self, consistency: ReadConsistency) -> Result<CommitVersion> {
        self.snapshot(consistency).await
    }

    async fn commit_transaction_bundle(
        &self,
        bundle: TransactionBundle,
        durability: DurabilityLevel,
    ) -> Result<CommitOutcome> {
        self.commit(bundle, durability).await
    }

    fn apply_transaction_decision(
        &self,
        bundle: TransactionBundle,
        result: CertificationResult,
    ) -> Result<CommitOutcome> {
        self.apply_certification(bundle, result)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Draft {
    cluster_id: String,
    transaction_id: String,
    idempotency_key: String,
    principal: String,
    snapshot_version: CommitVersion,
    expires_at_unix_ms: u64,
    durability: DurabilityLevel,
    state: DraftState,
    mutations: DraftMutations,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum DraftState {
    Open,
    Committing {
        bundle: TransactionBundle,
    },
    Resolved {
        bundle: TransactionBundle,
        result: CertificationResult,
    },
    RolledBack,
    Expired,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DraftMutations {
    points: Vec<PointObservation>,
    ranges: Vec<RangeObservation>,
    predicates: Vec<crate::mvcc_transaction::ExplicitPredicate>,
    #[serde(default)]
    assignment_predicates: Vec<crate::mvcc_transaction::AssignmentPredicate>,
    writes: Vec<WriteOperation>,
    manifests: Vec<ObjectShardManifestReference>,
    events: Vec<Vec<u8>>,
    jobs: Vec<Vec<u8>>,
    #[serde(default)]
    idempotency_results: Vec<crate::mvcc_transaction::IdempotencyResult>,
}

pub struct OpenTransactionRegistry {
    db: Arc<DB>,
    transition: Mutex<()>,
    snapshot_gc_gate: Arc<tokio::sync::Mutex<()>>,
}

pub(crate) struct RecoverableTransactionBundle {
    pub bundle: TransactionBundle,
    pub require_complete_evidence: bool,
}

impl OpenTransactionRegistry {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let descriptors = MVCC_TRANSACTION_COLUMN_FAMILIES
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
        Ok(Self {
            db: Arc::new(
                DB::open_cf_descriptors(&options, path.as_ref(), descriptors).with_context(
                    || {
                        format!(
                            "open MVCC transaction registry at {}",
                            path.as_ref().display()
                        )
                    },
                )?,
            ),
            transition: Mutex::new(()),
            snapshot_gc_gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub fn from_db(db: Arc<DB>) -> Result<Self> {
        for name in MVCC_TRANSACTION_COLUMN_FAMILIES {
            if db.cf_handle(name).is_none() {
                bail!("missing transaction registry column family {name}");
            }
        }
        Ok(Self {
            db,
            transition: Mutex::new(()),
            snapshot_gc_gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Serializes selection and durable publication of a new snapshot with
    /// local application of a cluster garbage-collection watermark.
    ///
    /// Cluster reports let the leader avoid proposing a watermark that crosses
    /// a remote pin, but a report can become stale while `begin` is selecting
    /// its snapshot. The node that owns the durable transaction registry is the
    /// final safety boundary: either the draft is published first and local GC
    /// observes its pin, or GC completes before a new snapshot is selected.
    pub(crate) fn snapshot_gc_gate(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.snapshot_gc_gate.clone()
    }

    /// Returns every snapshot still pinned by a live or committing durable
    /// transaction session.
    ///
    /// Expiry is evaluated from persisted session data, so a crashed process
    /// cannot lose an active-snapshot pin merely because an in-memory guard
    /// disappeared.
    pub fn active_snapshot_pins(&self, now_unix_ms: u64) -> Result<BTreeSet<CommitVersion>> {
        let cf = self.cf(CF_TRANSACTIONS)?;
        let mut snapshots = BTreeSet::new();
        for row in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (_, value) = row?;
            let draft: Draft = serde_json::from_slice(&value)?;
            // TTL closes a draft which has not started committing. Once the
            // durable state is `Committing`, certification owns resolution
            // and the snapshot must stay pinned regardless of elapsed client
            // TTL; otherwise GC can discard conflict/MVCC history underneath
            // an in-flight commit.
            if matches!(&draft.state, DraftState::Committing { .. })
                || (matches!(&draft.state, DraftState::Open)
                    && draft.expires_at_unix_ms > now_unix_ms)
            {
                snapshots.insert(draft.snapshot_version);
            }
        }
        Ok(snapshots)
    }

    /// Returns transaction identities whose canonical bundle may already have
    /// been prepared but is not yet represented by locally applied
    /// post-commit work.
    ///
    /// `commit` persists `Committing` before handing the bundle to the runtime.
    /// Prepared-bundle GC must retain these identities: otherwise a concurrent
    /// GC pass can unlink the bundle after local persistence but before the
    /// compact consensus decision and its materialisation jobs are applied.
    pub fn prepared_bundle_transaction_pins(&self) -> Result<BTreeSet<String>> {
        let cf = self.cf(CF_TRANSACTIONS)?;
        let mut transactions = BTreeSet::new();
        for row in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (_, value) = row?;
            let draft: Draft = serde_json::from_slice(&value)?;
            if matches!(draft.state, DraftState::Committing { .. }) {
                transactions.insert(draft.transaction_id);
            }
        }
        Ok(transactions)
    }

    pub async fn begin(
        &self,
        runtime: &impl TransactionRuntime,
        cluster_id: impl Into<String>,
        principal: impl Into<String>,
        idempotency_key: impl Into<String>,
        ttl: Duration,
        durability: DurabilityLevel,
        consistency: ReadConsistency,
        now_unix_ms: u64,
    ) -> Result<TransactionHandle> {
        let snapshot_gc_gate = self.snapshot_gc_gate.clone();
        let _snapshot_gc_guard = snapshot_gc_gate.lock().await;
        let cluster_id = cluster_id.into();
        let principal = principal.into();
        let idempotency_key = idempotency_key.into();
        if cluster_id.trim().is_empty()
            || principal.trim().is_empty()
            || idempotency_key.trim().is_empty()
        {
            bail!("cluster ID, principal and idempotency key must be non-empty");
        }
        let ttl_ms = u64::try_from(ttl.as_millis()).context("transaction TTL exceeds u64")?;
        if ttl_ms == 0 {
            bail!("transaction TTL must be non-zero");
        }

        if let Some(existing) = self.find_by_idempotency(&cluster_id, &idempotency_key)? {
            return self.retry_handle(existing, &principal);
        }
        let snapshot_version = runtime.transaction_snapshot(consistency).await?;
        let expires_at_unix_ms = now_unix_ms
            .checked_add(ttl_ms)
            .context("transaction expiry overflow")?;
        let transaction_id = transaction_id(&cluster_id, &principal, &idempotency_key);
        let draft = Draft {
            cluster_id,
            transaction_id: transaction_id.clone(),
            idempotency_key: idempotency_key.clone(),
            principal,
            snapshot_version,
            expires_at_unix_ms,
            durability,
            state: DraftState::Open,
            mutations: DraftMutations::default(),
        };

        let _guard = self.transition.lock().unwrap();
        if let Some(existing) = self.find_by_idempotency(&draft.cluster_id, &idempotency_key)? {
            return self.retry_handle(existing, &draft.principal);
        }
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.cf(CF_TRANSACTIONS)?,
            transaction_id.as_bytes(),
            serde_json::to_vec(&draft)?,
        );
        batch.put_cf(
            self.cf(CF_IDEMPOTENCY)?,
            idempotency_index_key(&draft.cluster_id, &idempotency_key),
            transaction_id.as_bytes(),
        );
        self.db.write_opt(batch, &durable_write_options())?;
        Ok(handle(&draft))
    }

    pub fn observe_point(
        &self,
        transaction_id: &str,
        owning_cluster_id: &str,
        key: LogicalKey,
        observed_version: Option<CommitVersion>,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.mutate(transaction_id, now_unix_ms, |draft| {
            ensure_owning_cluster(draft, owning_cluster_id)?;
            draft.mutations.points.push(PointObservation {
                key,
                observed_version,
            });
            Ok(())
        })
    }

    pub fn observe_range(
        &self,
        transaction_id: &str,
        owning_cluster_id: &str,
        table_id: u16,
        start_application_key: Option<Vec<u8>>,
        end_application_key: Option<Vec<u8>>,
        observed_range_stamp: Option<CommitVersion>,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.mutate(transaction_id, now_unix_ms, |draft| {
            ensure_owning_cluster(draft, owning_cluster_id)?;
            let mut builder = TransactionBundleBuilder::new(
                &draft.cluster_id,
                &draft.transaction_id,
                draft.snapshot_version,
                &draft.principal,
                Default::default(),
            );
            builder.observe_scan(
                table_id,
                start_application_key.clone(),
                end_application_key.clone(),
                observed_range_stamp,
            )?;
            let observation = builder.build()?.range_observations.remove(0);
            draft.mutations.ranges.push(observation);
            Ok(())
        })
    }

    pub fn add_predicate(
        &self,
        transaction_id: &str,
        owning_cluster_id: &str,
        key: LogicalKey,
        kind: crate::mvcc_transaction::PredicateKind,
        observed_version: Option<CommitVersion>,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.mutate(transaction_id, now_unix_ms, |draft| {
            ensure_owning_cluster(draft, owning_cluster_id)?;
            let predicate = crate::mvcc_transaction::ExplicitPredicate {
                key,
                kind,
                observed_version,
            };
            if !draft.mutations.predicates.contains(&predicate) {
                draft.mutations.predicates.push(predicate);
            }
            Ok(())
        })
    }

    pub fn require_assignment(
        &self,
        transaction_id: &str,
        principal: &str,
        predicate: crate::mvcc_transaction::AssignmentPredicate,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.mutate_for_principal(transaction_id, principal, now_unix_ms, |draft| {
            if !draft.mutations.assignment_predicates.contains(&predicate) {
                draft.mutations.assignment_predicates.push(predicate);
            }
            Ok(())
        })
    }

    pub fn put(
        &self,
        transaction_id: &str,
        owning_cluster_id: &str,
        key: LogicalKey,
        value: Vec<u8>,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.mutate(transaction_id, now_unix_ms, |draft| {
            ensure_owning_cluster(draft, owning_cluster_id)?;
            draft
                .mutations
                .writes
                .push(WriteOperation::Put { key, value });
            Ok(())
        })
    }

    pub fn delete(
        &self,
        transaction_id: &str,
        owning_cluster_id: &str,
        key: LogicalKey,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.mutate(transaction_id, now_unix_ms, |draft| {
            ensure_owning_cluster(draft, owning_cluster_id)?;
            draft.mutations.writes.push(WriteOperation::Delete { key });
            Ok(())
        })
    }

    pub fn add_manifest(
        &self,
        transaction_id: &str,
        owning_cluster_id: &str,
        manifest: ObjectShardManifestReference,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.mutate(transaction_id, now_unix_ms, |draft| {
            ensure_owning_cluster(draft, owning_cluster_id)?;
            draft.mutations.manifests.push(manifest);
            Ok(())
        })
    }

    /// Atomically add a set of point observations and their corresponding
    /// writes to one open transaction.
    ///
    /// A logical key is observed at most once (the first observation wins) and
    /// has at most one staged write (the latest value wins). This makes retries
    /// safe and guarantees that a crash cannot leave half of a product
    /// operation in the durable draft.
    pub fn stage_logical_mutations(
        &self,
        transaction_id: &str,
        principal: &str,
        owning_cluster_id: &str,
        mutations: Vec<StagedLogicalMutation>,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.mutate_for_principal(transaction_id, principal, now_unix_ms, |draft| {
            ensure_owning_cluster(draft, owning_cluster_id)?;
            for mutation in mutations {
                if !draft
                    .mutations
                    .points
                    .iter()
                    .any(|point| point.key == mutation.key)
                {
                    draft.mutations.points.push(PointObservation {
                        key: mutation.key.clone(),
                        observed_version: mutation.observed_version,
                    });
                }
                draft
                    .mutations
                    .writes
                    .retain(|write| write.key() != &mutation.key);
                draft.mutations.writes.push(match mutation.value {
                    Some(value) => WriteOperation::Put {
                        key: mutation.key,
                        value,
                    },
                    None => WriteOperation::Delete { key: mutation.key },
                });
            }
            Ok(())
        })
    }

    pub fn add_event(&self, transaction_id: &str, event: Vec<u8>, now_unix_ms: u64) -> Result<()> {
        crate::mvcc_outbox::StreamOutboxEvent::decode(&event)
            .context("stage versioned stream outbox event")?;
        self.mutate(transaction_id, now_unix_ms, |draft| {
            if !draft.mutations.events.contains(&event) {
                draft.mutations.events.push(event);
            }
            Ok(())
        })
    }

    pub fn add_stream_event(
        &self,
        transaction_id: &str,
        event: crate::mvcc_outbox::StreamOutboxEvent,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.add_event(transaction_id, event.encode()?, now_unix_ms)
    }

    pub fn add_job(&self, transaction_id: &str, job: Vec<u8>, now_unix_ms: u64) -> Result<()> {
        self.mutate(transaction_id, now_unix_ms, |draft| {
            if !draft.mutations.jobs.contains(&job) {
                draft.mutations.jobs.push(job);
            }
            Ok(())
        })
    }

    pub fn add_idempotency_result(
        &self,
        transaction_id: &str,
        principal: &str,
        result: crate::mvcc_transaction::IdempotencyResult,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.mutate_for_principal(transaction_id, principal, now_unix_ms, |draft| {
            if let Some(existing) = draft.mutations.idempotency_results.iter().find(|existing| {
                existing.namespace == result.namespace && existing.key == result.key
            }) {
                if existing != &result {
                    bail!("idempotency result identity was reused with a different payload");
                }
                return Ok(());
            }
            draft.mutations.idempotency_results.push(result);
            Ok(())
        })
    }

    pub fn resolved_idempotency_result(
        &self,
        transaction_id: &str,
        principal: &str,
        namespace: &str,
        key: &str,
    ) -> Result<Option<crate::mvcc_transaction::IdempotencyResult>> {
        let draft = self.load_for_principal(transaction_id, principal)?;
        if !matches!(draft.state, DraftState::Resolved { .. }) {
            return Ok(None);
        }
        Ok(draft
            .mutations
            .idempotency_results
            .into_iter()
            .find(|result| result.namespace == namespace && result.key == key))
    }

    pub async fn commit(
        &self,
        runtime: &impl TransactionRuntime,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<CommitOutcome> {
        let (bundle, resolved, durability) = {
            let _guard = self.transition.lock().unwrap();
            let mut draft = self.load_for_principal(transaction_id, principal)?;
            match &draft.state {
                DraftState::Open => {
                    if now_unix_ms >= draft.expires_at_unix_ms {
                        draft.state = DraftState::Expired;
                        self.save(&draft)?;
                        bail!("transaction has expired");
                    }
                    let bundle = build_bundle(&draft)?;
                    if is_read_only_bundle(&bundle) {
                        let result = CertificationResult::Committed {
                            commit_version: draft.snapshot_version,
                        };
                        draft.state = DraftState::Resolved {
                            bundle: bundle.clone(),
                            result: result.clone(),
                        };
                        self.save(&draft)?;
                        (bundle, Some(result), draft.durability)
                    } else {
                        draft.state = DraftState::Committing {
                            bundle: bundle.clone(),
                        };
                        self.save(&draft)?;
                        (bundle, None, draft.durability)
                    }
                }
                DraftState::Committing { bundle } => (bundle.clone(), None, draft.durability),
                DraftState::Resolved { bundle, result } => {
                    (bundle.clone(), Some(result.clone()), draft.durability)
                }
                DraftState::RolledBack => bail!("transaction was rolled back"),
                DraftState::Expired => bail!("transaction has expired"),
            }
        };

        let outcome = match resolved {
            Some(result) if is_read_only_bundle(&bundle) => Ok(CommitOutcome {
                certification: result,
                local_apply: None,
            }),
            Some(result) => runtime.apply_transaction_decision(bundle.clone(), result),
            None => {
                runtime
                    .commit_transaction_bundle(bundle.clone(), durability)
                    .await
            }
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                if crate::mvcc_transaction::is_pre_certification_failure(&error) {
                    let _guard = self.transition.lock().unwrap();
                    let mut draft = self.load_for_principal(transaction_id, principal)?;
                    if matches!(draft.state, DraftState::Committing { .. }) {
                        draft.state = DraftState::Open;
                        self.save(&draft)?;
                    }
                }
                return Err(error);
            }
        };
        let _guard = self.transition.lock().unwrap();
        let mut draft = self.load_for_principal(transaction_id, principal)?;
        draft.state = DraftState::Resolved {
            bundle,
            result: outcome.certification.clone(),
        };
        self.save(&draft)?;
        Ok(outcome)
    }

    pub fn handle(&self, transaction_id: &str) -> Result<TransactionHandle> {
        Ok(handle(&self.load(transaction_id)?))
    }

    pub(crate) fn logical_key_for_conflict_hash(
        &self,
        transaction_id: &str,
        principal: &str,
        key_hash: [u8; 32],
    ) -> Result<Option<LogicalKey>> {
        let draft = self.load_for_principal(transaction_id, principal)?;
        let mut keys = BTreeSet::new();
        keys.extend(draft.mutations.points.iter().map(|point| point.key.clone()));
        keys.extend(
            draft
                .mutations
                .predicates
                .iter()
                .map(|predicate| predicate.key.clone()),
        );
        keys.extend(
            draft
                .mutations
                .writes
                .iter()
                .map(|write| write.key().clone()),
        );
        let resolved_bundle = match &draft.state {
            DraftState::Committing { bundle } | DraftState::Resolved { bundle, .. } => Some(bundle),
            DraftState::Open | DraftState::RolledBack | DraftState::Expired => None,
        };
        if let Some(bundle) = resolved_bundle {
            keys.extend(
                bundle
                    .point_observations
                    .iter()
                    .map(|point| point.key.clone()),
            );
            keys.extend(
                bundle
                    .predicates
                    .iter()
                    .map(|predicate| predicate.key.clone()),
            );
            keys.extend(bundle.writes.iter().map(|write| write.key().clone()));
        }
        Ok(keys.into_iter().find(|key| {
            crate::mvcc_consensus_adapter::logical_key_hash(&draft.cluster_id, key).0 == key_hash
        }))
    }

    pub fn binding(&self, transaction_id: &str, principal: &str) -> Result<TransactionBinding> {
        let draft = self.load_for_principal(transaction_id, principal)?;
        if !matches!(
            draft.state,
            DraftState::Open | DraftState::Committing { .. }
        ) {
            bail!("transaction can no longer accept staged data");
        }
        Ok(TransactionBinding {
            cluster_id: draft.cluster_id,
            durability: draft.durability,
        })
    }

    /// Return this transaction's latest staged value for `key`.
    ///
    /// `None` means the transaction has not written the key. `Some(None)` is a
    /// staged tombstone and `Some(Some(value))` is a staged put.
    pub fn staged_value(
        &self,
        transaction_id: &str,
        principal: &str,
        key: &LogicalKey,
    ) -> Result<Option<Option<Vec<u8>>>> {
        let draft = self.load_for_principal(transaction_id, principal)?;
        if !matches!(
            draft.state,
            DraftState::Open | DraftState::Committing { .. }
        ) {
            bail!("transaction can no longer be read");
        }
        Ok(draft
            .mutations
            .writes
            .iter()
            .rev()
            .find(|write| write.key() == key)
            .map(|write| match write {
                WriteOperation::Put { value, .. } => Some(value.clone()),
                WriteOperation::Delete { .. } => None,
            }))
    }

    pub fn staged_writes(
        &self,
        transaction_id: &str,
        principal: &str,
    ) -> Result<Vec<WriteOperation>> {
        let draft = self.load_for_principal(transaction_id, principal)?;
        if !matches!(
            draft.state,
            DraftState::Open | DraftState::Committing { .. }
        ) {
            bail!("transaction can no longer be read");
        }
        Ok(draft.mutations.writes.clone())
    }

    /// Rebuild canonical bundles for durable transactions which may still
    /// enter certification after this process restarts.
    ///
    /// A `Committing` draft stores both its staged mutations and the frozen
    /// bundle. Requiring those representations to agree prevents recovery from
    /// binding physical durability evidence to a different transaction.
    pub(crate) fn recoverable_transaction_bundles(
        &self,
    ) -> Result<Vec<RecoverableTransactionBundle>> {
        let _guard = self.transition.lock().unwrap();
        let cf = self.cf(CF_TRANSACTIONS)?;
        let mut bundles = Vec::new();
        for row in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = row?;
            let draft: Draft =
                serde_json::from_slice(&value).context("decode recoverable transaction draft")?;
            if key.as_ref() != draft.transaction_id.as_bytes() {
                bail!("persisted transaction draft key does not match its transaction ID");
            }
            match &draft.state {
                DraftState::Open => match build_bundle(&draft) {
                    Ok(bundle) => bundles.push(RecoverableTransactionBundle {
                        bundle,
                        require_complete_evidence: false,
                    }),
                    Err(error) => {
                        tracing::warn!(
                            transaction_id = %draft.transaction_id,
                            %error,
                            "skipping invalid open transaction during evidence recovery"
                        );
                    }
                },
                DraftState::Committing { bundle } => {
                    let staged = build_bundle(&draft)?;
                    if bundle != &staged {
                        bail!(
                            "persisted committing bundle does not match its transaction's staged mutations"
                        );
                    }
                    bundles.push(RecoverableTransactionBundle {
                        bundle: bundle.clone(),
                        require_complete_evidence: true,
                    });
                }
                DraftState::Resolved { .. } | DraftState::RolledBack | DraftState::Expired => {}
            }
        }
        Ok(bundles)
    }

    pub fn status(
        &self,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<TransactionRegistryStatus> {
        let _guard = self.transition.lock().unwrap();
        let mut draft = self.load_for_principal(transaction_id, principal)?;
        if matches!(draft.state, DraftState::Open) && now_unix_ms >= draft.expires_at_unix_ms {
            draft.state = DraftState::Expired;
            self.save(&draft)?;
        }
        let (state, result) = match &draft.state {
            DraftState::Open => ("open", None),
            DraftState::Committing { .. } => ("committing", None),
            DraftState::Resolved { result, .. } => match result {
                CertificationResult::Committed { .. } => ("committed", Some(result.clone())),
                CertificationResult::Aborted { .. } => ("aborted", Some(result.clone())),
            },
            DraftState::RolledBack => ("rolled_back", None),
            DraftState::Expired => ("expired", None),
        };
        Ok(TransactionRegistryStatus {
            cluster_id: draft.cluster_id,
            transaction_id: draft.transaction_id,
            snapshot_version: draft.snapshot_version,
            expires_at_unix_ms: draft.expires_at_unix_ms,
            state,
            result,
            durability: draft.durability,
        })
    }

    pub fn status_by_idempotency(
        &self,
        cluster_id: &str,
        idempotency_key: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<Option<TransactionRegistryStatus>> {
        let Some(draft) = self.find_by_idempotency(cluster_id, idempotency_key)? else {
            return Ok(None);
        };
        if draft.principal != principal {
            bail!("idempotency key belongs to another principal");
        }
        self.status(&draft.transaction_id, principal, now_unix_ms)
            .map(Some)
    }

    pub fn rollback(
        &self,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<TransactionRegistryStatus> {
        let _guard = self.transition.lock().unwrap();
        let mut draft = self.load_for_principal(transaction_id, principal)?;
        match draft.state {
            DraftState::Open if now_unix_ms < draft.expires_at_unix_ms => {
                // Rolled-back intent must not remain as predicate-visible or
                // recovery-visible staged state. Physical payloads are
                // content-addressed and reclaimed independently once their
                // manifest references are no longer pinned here.
                draft.mutations = DraftMutations::default();
                draft.state = DraftState::RolledBack;
                self.save(&draft)?;
            }
            DraftState::Open => {
                draft.state = DraftState::Expired;
                self.save(&draft)?;
                bail!("transaction has expired");
            }
            DraftState::RolledBack => {}
            _ => bail!("transaction can no longer be rolled back"),
        }
        Ok(TransactionRegistryStatus {
            cluster_id: draft.cluster_id,
            transaction_id: draft.transaction_id,
            snapshot_version: draft.snapshot_version,
            expires_at_unix_ms: draft.expires_at_unix_ms,
            state: "rolled_back",
            result: None,
            durability: draft.durability,
        })
    }

    fn mutate(
        &self,
        transaction_id: &str,
        now_unix_ms: u64,
        change: impl FnOnce(&mut Draft) -> Result<()>,
    ) -> Result<()> {
        let _guard = self.transition.lock().unwrap();
        let mut draft = self.load(transaction_id)?;
        if !matches!(draft.state, DraftState::Open) {
            bail!("transaction is no longer open");
        }
        if now_unix_ms >= draft.expires_at_unix_ms {
            draft.state = DraftState::Expired;
            self.save(&draft)?;
            bail!("transaction has expired");
        }
        change(&mut draft)?;
        self.save(&draft)
    }

    fn mutate_for_principal(
        &self,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
        change: impl FnOnce(&mut Draft) -> Result<()>,
    ) -> Result<()> {
        let _guard = self.transition.lock().unwrap();
        let mut draft = self.load_for_principal(transaction_id, principal)?;
        if !matches!(draft.state, DraftState::Open) {
            bail!("transaction is no longer open");
        }
        if now_unix_ms >= draft.expires_at_unix_ms {
            draft.state = DraftState::Expired;
            self.save(&draft)?;
            bail!("transaction has expired");
        }
        change(&mut draft)?;
        self.save(&draft)
    }

    fn retry_handle(&self, draft: Draft, principal: &str) -> Result<TransactionHandle> {
        if draft.principal != principal {
            bail!("idempotency key belongs to another principal");
        }
        Ok(handle(&draft))
    }

    fn find_by_idempotency(&self, cluster_id: &str, key: &str) -> Result<Option<Draft>> {
        let Some(transaction_id) = self.db.get_cf(
            self.cf(CF_IDEMPOTENCY)?,
            idempotency_index_key(cluster_id, key),
        )?
        else {
            return Ok(None);
        };
        let transaction_id =
            std::str::from_utf8(&transaction_id).context("invalid transaction ID index")?;
        self.load(transaction_id).map(Some)
    }

    fn load(&self, transaction_id: &str) -> Result<Draft> {
        let bytes = self
            .db
            .get_cf(self.cf(CF_TRANSACTIONS)?, transaction_id.as_bytes())?
            .with_context(|| format!("unknown transaction {transaction_id}"))?;
        serde_json::from_slice(&bytes).context("decode persisted transaction draft")
    }

    fn load_for_principal(&self, transaction_id: &str, principal: &str) -> Result<Draft> {
        let draft = self.load(transaction_id)?;
        if draft.principal != principal {
            bail!("transaction belongs to another principal");
        }
        Ok(draft)
    }

    fn save(&self, draft: &Draft) -> Result<()> {
        self.db.put_cf_opt(
            self.cf(CF_TRANSACTIONS)?,
            draft.transaction_id.as_bytes(),
            serde_json::to_vec(draft)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn cf(&self, name: &str) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| anyhow!("missing transaction registry column family {name}"))
    }
}

fn is_read_only_bundle(bundle: &TransactionBundle) -> bool {
    bundle.writes.is_empty()
        && bundle.shard_manifests.is_empty()
        && bundle.outbox_events.is_empty()
        && bundle.materialisation_jobs.is_empty()
        && bundle.idempotency_results.is_empty()
}

fn build_bundle(draft: &Draft) -> Result<TransactionBundle> {
    let mut builder = TransactionBundleBuilder::new(
        &draft.cluster_id,
        &draft.transaction_id,
        draft.snapshot_version,
        &draft.principal,
        Default::default(),
    );
    for point in &draft.mutations.points {
        builder.observe_point(point.key.clone(), point.observed_version);
    }
    for range in &draft.mutations.ranges {
        builder.observe_scan(
            range.table_id,
            range.start_application_key.clone(),
            range.end_application_key.clone(),
            range.observed_range_stamp,
        )?;
    }
    for predicate in &draft.mutations.predicates {
        builder.predicate(
            predicate.key.clone(),
            predicate.kind.clone(),
            predicate.observed_version,
        );
    }
    for predicate in &draft.mutations.assignment_predicates {
        builder.require_assignment(predicate.clone());
    }
    for write in &draft.mutations.writes {
        match write {
            WriteOperation::Put { key, value } => {
                builder.put(key.clone(), value.clone());
            }
            WriteOperation::Delete { key } => {
                builder.delete(key.clone());
            }
        }
    }
    for manifest in &draft.mutations.manifests {
        builder.add_shard_manifest(manifest.clone());
    }
    for event in &draft.mutations.events {
        builder.add_outbox_event(event.clone());
    }
    for job in &draft.mutations.jobs {
        builder.add_materialisation_job(job.clone());
    }
    for result in &draft.mutations.idempotency_results {
        builder.add_idempotency_result(result.clone());
    }
    builder.build()
}

fn ensure_owning_cluster(draft: &Draft, owning_cluster_id: &str) -> Result<()> {
    if owning_cluster_id != draft.cluster_id {
        bail!("staged resource belongs to another cluster");
    }
    Ok(())
}

fn transaction_id(cluster_id: &str, principal: &str, idempotency_key: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"anvil.mvcc.open-transaction.v1");
    hash.update((cluster_id.len() as u64).to_be_bytes());
    hash.update(cluster_id.as_bytes());
    hash.update((principal.len() as u64).to_be_bytes());
    hash.update(principal.as_bytes());
    hash.update((idempotency_key.len() as u64).to_be_bytes());
    hash.update(idempotency_key.as_bytes());
    format!("tx-{:x}", hash.finalize())
}

fn idempotency_index_key(cluster_id: &str, idempotency_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(cluster_id.len() + idempotency_key.len() + 8);
    key.extend_from_slice(&(cluster_id.len() as u32).to_be_bytes());
    key.extend_from_slice(cluster_id.as_bytes());
    key.extend_from_slice(&(idempotency_key.len() as u32).to_be_bytes());
    key.extend_from_slice(idempotency_key.as_bytes());
    key
}

fn handle(draft: &Draft) -> TransactionHandle {
    TransactionHandle {
        cluster_id: draft.cluster_id.clone(),
        transaction_id: draft.transaction_id.clone(),
        snapshot_version: draft.snapshot_version,
        expires_at_unix_ms: draft.expires_at_unix_ms,
        durability: draft.durability,
    }
}

fn durable_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

#[cfg(test)]
#[path = "mvcc_open_transactions/tests.rs"]
mod tests;
