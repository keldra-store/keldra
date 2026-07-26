use std::collections::{BTreeMap, BTreeSet};

use stateright::{Model, Property};

pub const METADATA_REPLICA_COUNT: u8 = 3;
pub const METADATA_QUORUM: usize = 2;
pub const STORAGE_SHARD_COUNT: u8 = 6;
pub const STORAGE_SHARD_COMMIT_QUORUM: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootKey(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchId(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxId(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteWriteId(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerFence(pub u8);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CoreStoreModel {
    pub max_steps: u8,
    pub roots: u8,
    pub batches: u8,
}

impl CoreStoreModel {
    pub fn small() -> Self {
        Self {
            max_steps: if cfg!(feature = "exhaustive-small") {
                12
            } else {
                8
            },
            roots: 2,
            batches: 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CoreStoreState {
    pub steps: u8,
    pub clock: u8,
    pub roots: BTreeMap<RootKey, RootState>,
    pub replicas: BTreeMap<NodeId, CoreMetaReplica>,
    pub batches: BTreeMap<BatchId, CoreMetaBatch>,
    pub transactions: BTreeMap<TxId, TransactionState>,
    pub byte_writes: BTreeMap<ByteWriteId, ByteWriteState>,
    pub published: BTreeMap<(RootKey, u8), BatchId>,
    pub visible_rows: BTreeMap<(RootKey, u8), BTreeSet<RowId>>,
    pub rejected_stale_owner_publishes: u8,
    pub rejected_scope_mismatches: u8,
    pub rejected_missing_byte_cert_commits: u8,
}

impl CoreStoreState {
    pub fn new(root_count: u8) -> Self {
        let roots = (0..root_count)
            .map(|root| (RootKey(root), RootState::new()))
            .collect();
        let replicas = (0..METADATA_REPLICA_COUNT)
            .map(|node| (NodeId(node), CoreMetaReplica::default()))
            .collect();

        Self {
            steps: 0,
            clock: 0,
            roots,
            replicas,
            batches: BTreeMap::new(),
            transactions: BTreeMap::new(),
            byte_writes: BTreeMap::new(),
            published: BTreeMap::new(),
            visible_rows: BTreeMap::new(),
            rejected_stale_owner_publishes: 0,
            rejected_scope_mismatches: 0,
            rejected_missing_byte_cert_commits: 0,
        }
    }

    pub fn apply(&self, action: Action) -> Option<Self> {
        let mut state = self.clone();
        state.steps = state.steps.saturating_add(1);
        match action {
            Action::StartImplicitBatch(batch_id, root_key) => {
                start_batch(&mut state, batch_id, root_key, None)?;
            }
            Action::StartByteBackedBatch(batch_id, root_key, byte_write_id) => {
                if !state.byte_writes.contains_key(&byte_write_id) {
                    return None;
                }
                start_batch(&mut state, batch_id, root_key, Some(byte_write_id))?;
            }
            Action::StartByteWrite(byte_write_id) => {
                if state.byte_writes.contains_key(&byte_write_id) {
                    return None;
                }
                state
                    .byte_writes
                    .insert(byte_write_id, ByteWriteState::default());
            }
            Action::ShardFsync(byte_write_id, shard_id) => {
                if shard_id.0 >= STORAGE_SHARD_COUNT {
                    return None;
                }
                let byte_write = state.byte_writes.get_mut(&byte_write_id)?;
                byte_write.shard_receipts.insert(shard_id);
            }
            Action::BuildShardCertificate(byte_write_id) => {
                let byte_write = state.byte_writes.get_mut(&byte_write_id)?;
                if byte_write.shard_receipts.len() < STORAGE_SHARD_COMMIT_QUORUM {
                    return None;
                }
                byte_write.commit_certificate_built = true;
            }
            Action::Prepare(batch_id, node_id) => {
                let batch = state.batches.get_mut(&batch_id)?;
                batch.prepare_receipts.insert(node_id);
                state
                    .replicas
                    .get_mut(&node_id)?
                    .pending_batches
                    .insert(batch_id);
            }
            Action::BuildCommitCertificate(batch_id) => {
                let batch = state.batches.get_mut(&batch_id)?;
                if batch.prepare_receipts.len() < METADATA_QUORUM {
                    return None;
                }
                batch.commit_certificate_built = true;
            }
            Action::PersistCommitCertificate(batch_id, node_id) => {
                let batch = state.batches.get_mut(&batch_id)?;
                if !batch.commit_certificate_built {
                    return None;
                }
                batch.certificate_persist_receipts.insert(node_id);
                state
                    .replicas
                    .get_mut(&node_id)?
                    .persisted_commit_certificates
                    .insert(batch_id);
            }
            Action::AttemptPublish(batch_id, fence) => {
                publish_batch(&mut state, batch_id, fence);
            }
            Action::BumpOwnerFence(root_key) => {
                let root = state.roots.get_mut(&root_key)?;
                root.owner_epoch = root.owner_epoch.saturating_add(1);
                root.owner_fence = OwnerFence(root.owner_fence.0.saturating_add(1));
            }
            Action::BeginTransaction(tx_id, root_key) => {
                if state.transactions.contains_key(&tx_id) {
                    return None;
                }
                state.transactions.insert(
                    tx_id,
                    TransactionState {
                        root_key,
                        status: TransactionStatus::Open,
                        staged_rows: BTreeSet::new(),
                        expiry_tick: state.clock.saturating_add(2),
                    },
                );
            }
            Action::StageMutation(tx_id, root_key) => {
                let tx = state.transactions.get_mut(&tx_id)?;
                if tx.status != TransactionStatus::Open {
                    return None;
                }
                if tx.root_key != root_key {
                    tx.status = TransactionStatus::ScopeMismatch;
                    state.rejected_scope_mismatches =
                        state.rejected_scope_mismatches.saturating_add(1);
                } else {
                    tx.staged_rows
                        .insert(transaction_row(tx_id, tx.staged_rows.len()));
                }
            }
            Action::CommitTransaction(tx_id) => {
                commit_transaction(&mut state, tx_id)?;
            }
            Action::RollbackTransaction(tx_id) => {
                let tx = state.transactions.get_mut(&tx_id)?;
                if tx.status != TransactionStatus::Open {
                    return None;
                }
                tx.status = TransactionStatus::RolledBack;
            }
            Action::ExpireTransaction(tx_id) => {
                let tx = state.transactions.get_mut(&tx_id)?;
                if tx.status != TransactionStatus::Open || state.clock < tx.expiry_tick {
                    return None;
                }
                tx.status = TransactionStatus::Expired;
            }
            Action::AdvanceClock => {
                state.clock = state.clock.saturating_add(1);
            }
        }
        Some(state)
    }

    pub fn latest_generation(&self, root_key: RootKey) -> Option<u8> {
        self.roots
            .get(&root_key)
            .map(|root| root.visible_generation)
    }

    pub fn read_latest_committed(&self, root_key: RootKey) -> Option<BTreeSet<RowId>> {
        self.latest_generation(root_key)
            .map(|generation| self.read_at_generation(root_key, generation))
    }

    pub fn read_at_generation(&self, root_key: RootKey, generation: u8) -> BTreeSet<RowId> {
        let mut rows = BTreeSet::new();
        for current in 1..=generation {
            if let Some(generation_rows) = self.visible_rows.get(&(root_key, current)) {
                rows.extend(generation_rows.iter().copied());
            }
        }
        rows
    }

    pub fn root_generations_never_regress(&self) -> bool {
        self.roots
            .values()
            .all(|root| root.visible_generation >= root.lowest_observed_generation)
    }

    pub fn one_anchor_per_root_generation(&self) -> bool {
        let mut seen = BTreeSet::new();
        self.batches
            .values()
            .all(|batch| match batch.published_generation {
                Some(generation) => seen.insert((batch.root_key, generation)),
                None => true,
            })
    }

    pub fn visible_generations_have_persisted_certificates(&self) -> bool {
        self.published.values().all(|batch_id| {
            self.batches
                .get(batch_id)
                .map(|batch| batch.certificate_persist_receipts.len() >= METADATA_QUORUM)
                .unwrap_or(false)
        })
    }

    pub fn certificates_require_quorum_prepare(&self) -> bool {
        self.batches.values().all(|batch| {
            !batch.commit_certificate_built || batch.prepare_receipts.len() >= METADATA_QUORUM
        })
    }

    pub fn roots_reference_persisted_commit_evidence(&self) -> bool {
        self.published
            .iter()
            .all(|((root_key, generation), batch_id)| {
                self.batches.get(batch_id).is_some_and(|batch| {
                    batch.root_key == *root_key
                        && batch.target_generation == *generation
                        && batch.published_generation == Some(*generation)
                        && batch.certificate_persist_receipts.len() >= METADATA_QUORUM
                })
            })
    }

    pub fn staged_rows_are_invisible(&self) -> bool {
        self.transactions.values().all(|tx| {
            if tx.status == TransactionStatus::Committed {
                return true;
            }
            tx.staged_rows
                .iter()
                .all(|row| !self.visible_rows.values().any(|rows| rows.contains(row)))
        })
    }

    pub fn stale_owner_never_published(&self) -> bool {
        self.batches.values().all(|batch| {
            batch.published_generation.is_none()
                || batch.published_with_fence == Some(batch.owner_fence)
        })
    }

    pub fn published_batches_reference_durable_bytes(&self) -> bool {
        self.published.values().all(|batch_id| {
            let Some(batch) = self.batches.get(batch_id) else {
                return false;
            };
            let Some(byte_write_id) = batch.required_byte_write else {
                return true;
            };
            self.byte_writes
                .get(&byte_write_id)
                .is_some_and(|write| write.commit_certificate_built)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RootState {
    pub visible_generation: u8,
    pub lowest_observed_generation: u8,
    pub root_anchor_hash: u64,
    pub owner_node: NodeId,
    pub owner_epoch: u8,
    pub owner_fence: OwnerFence,
}

impl RootState {
    fn new() -> Self {
        Self {
            visible_generation: 0,
            lowest_observed_generation: 0,
            root_anchor_hash: 0,
            owner_node: NodeId(0),
            owner_epoch: 0,
            owner_fence: OwnerFence(1),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct CoreMetaReplica {
    pub pending_batches: BTreeSet<BatchId>,
    pub committed_rows: BTreeMap<(RootKey, u8), BTreeSet<RowId>>,
    pub persisted_commit_certificates: BTreeSet<BatchId>,
    pub highest_generation_servable: BTreeMap<RootKey, u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CoreMetaBatch {
    pub root_key: RootKey,
    pub target_generation: u8,
    pub owner_node: NodeId,
    pub owner_epoch: u8,
    pub owner_fence: OwnerFence,
    pub rows: BTreeSet<RowId>,
    pub required_byte_write: Option<ByteWriteId>,
    pub prepare_receipts: BTreeSet<NodeId>,
    pub commit_certificate_built: bool,
    pub certificate_persist_receipts: BTreeSet<NodeId>,
    pub published_generation: Option<u8>,
    pub published_with_fence: Option<OwnerFence>,
}

impl CoreMetaBatch {
    fn new(
        root_key: RootKey,
        batch_id: BatchId,
        target_generation: u8,
        owner_node: NodeId,
        owner_epoch: u8,
        owner_fence: OwnerFence,
        rows: BTreeSet<RowId>,
        required_byte_write: Option<ByteWriteId>,
    ) -> Self {
        let _ = batch_id;
        Self {
            root_key,
            target_generation,
            owner_node,
            owner_epoch,
            owner_fence,
            rows,
            required_byte_write,
            prepare_receipts: BTreeSet::new(),
            commit_certificate_built: false,
            certificate_persist_receipts: BTreeSet::new(),
            published_generation: None,
            published_with_fence: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ByteWriteState {
    pub shard_receipts: BTreeSet<ShardId>,
    pub commit_certificate_built: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionState {
    pub root_key: RootKey,
    pub status: TransactionStatus,
    pub staged_rows: BTreeSet<RowId>,
    pub expiry_tick: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionStatus {
    Open,
    Committed,
    RolledBack,
    Expired,
    ScopeMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    StartImplicitBatch(BatchId, RootKey),
    StartByteWrite(ByteWriteId),
    ShardFsync(ByteWriteId, ShardId),
    BuildShardCertificate(ByteWriteId),
    StartByteBackedBatch(BatchId, RootKey, ByteWriteId),
    Prepare(BatchId, NodeId),
    BuildCommitCertificate(BatchId),
    PersistCommitCertificate(BatchId, NodeId),
    AttemptPublish(BatchId, OwnerFence),
    BumpOwnerFence(RootKey),
    BeginTransaction(TxId, RootKey),
    StageMutation(TxId, RootKey),
    CommitTransaction(TxId),
    RollbackTransaction(TxId),
    ExpireTransaction(TxId),
    AdvanceClock,
}

impl Model for CoreStoreModel {
    type State = CoreStoreState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![CoreStoreState::new(self.roots)]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.steps >= self.max_steps {
            return;
        }

        for batch in 0..self.batches {
            let batch_id = BatchId(batch);
            if !state.batches.contains_key(&batch_id) {
                actions.push(Action::StartImplicitBatch(batch_id, RootKey(0)));
                if state.byte_writes.contains_key(&ByteWriteId(0)) {
                    actions.push(Action::StartByteBackedBatch(
                        batch_id,
                        RootKey(0),
                        ByteWriteId(0),
                    ));
                }
            } else {
                for node in 0..METADATA_REPLICA_COUNT {
                    let node_id = NodeId(node);
                    let batch_state = &state.batches[&batch_id];
                    if !batch_state.prepare_receipts.contains(&node_id) {
                        actions.push(Action::Prepare(batch_id, node_id));
                    }
                    if batch_state.commit_certificate_built
                        && !batch_state.certificate_persist_receipts.contains(&node_id)
                    {
                        actions.push(Action::PersistCommitCertificate(batch_id, node_id));
                    }
                }
                if !state.batches[&batch_id].commit_certificate_built {
                    actions.push(Action::BuildCommitCertificate(batch_id));
                }
                actions.push(Action::AttemptPublish(batch_id, OwnerFence(0)));
                actions.push(Action::AttemptPublish(batch_id, OwnerFence(1)));
            }
        }

        if !state.byte_writes.contains_key(&ByteWriteId(0)) {
            actions.push(Action::StartByteWrite(ByteWriteId(0)));
        } else {
            for shard in 0..STORAGE_SHARD_COUNT {
                let shard_id = ShardId(shard);
                if !state.byte_writes[&ByteWriteId(0)]
                    .shard_receipts
                    .contains(&shard_id)
                {
                    actions.push(Action::ShardFsync(ByteWriteId(0), shard_id));
                }
            }
            if !state.byte_writes[&ByteWriteId(0)].commit_certificate_built {
                actions.push(Action::BuildShardCertificate(ByteWriteId(0)));
            }
        }

        if state
            .roots
            .get(&RootKey(0))
            .is_some_and(|root| root.owner_fence.0 < 2)
        {
            actions.push(Action::BumpOwnerFence(RootKey(0)));
        }

        let tx_id = TxId(0);
        if !state.transactions.contains_key(&tx_id) {
            actions.push(Action::BeginTransaction(tx_id, RootKey(0)));
        } else {
            actions.push(Action::StageMutation(tx_id, RootKey(0)));
            actions.push(Action::StageMutation(tx_id, RootKey(1)));
            actions.push(Action::CommitTransaction(tx_id));
            actions.push(Action::RollbackTransaction(tx_id));
            actions.push(Action::ExpireTransaction(tx_id));
            actions.push(Action::AdvanceClock);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        state.apply(action)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always(
                "root generation never regresses",
                |_, state: &CoreStoreState| state.root_generations_never_regress(),
            ),
            Property::<Self>::always(
                "one anchor per root generation",
                |_, state: &CoreStoreState| state.one_anchor_per_root_generation(),
            ),
            Property::<Self>::always(
                "visible roots require persisted certificates",
                |_, state: &CoreStoreState| state.visible_generations_have_persisted_certificates(),
            ),
            Property::<Self>::always(
                "certificates require prepare quorum",
                |_, state: &CoreStoreState| state.certificates_require_quorum_prepare(),
            ),
            Property::<Self>::always(
                "roots reference commit evidence",
                |_, state: &CoreStoreState| state.roots_reference_persisted_commit_evidence(),
            ),
            Property::<Self>::always("staged rows are invisible", |_, state: &CoreStoreState| {
                state.staged_rows_are_invisible()
            }),
            Property::<Self>::always("stale owners never publish", |_, state: &CoreStoreState| {
                state.stale_owner_never_published()
            }),
            Property::<Self>::always(
                "published batches reference durable bytes",
                |_, state: &CoreStoreState| state.published_batches_reference_durable_bytes(),
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.steps <= self.max_steps
    }
}

fn start_batch(
    state: &mut CoreStoreState,
    batch_id: BatchId,
    root_key: RootKey,
    required_byte_write: Option<ByteWriteId>,
) -> Option<()> {
    if state.batches.contains_key(&batch_id) {
        return None;
    }
    let root = state.roots.get(&root_key)?;
    state.batches.insert(
        batch_id,
        CoreMetaBatch::new(
            root_key,
            batch_id,
            root.visible_generation + 1,
            root.owner_node,
            root.owner_epoch,
            root.owner_fence,
            BTreeSet::from([implicit_row(batch_id)]),
            required_byte_write,
        ),
    );
    Some(())
}

fn publish_batch(state: &mut CoreStoreState, batch_id: BatchId, supplied_fence: OwnerFence) {
    let Some(batch) = state.batches.get(&batch_id).cloned() else {
        return;
    };
    let Some(root) = state.roots.get_mut(&batch.root_key) else {
        return;
    };

    let byte_write_is_durable = batch.required_byte_write.map_or(true, |byte_write_id| {
        state
            .byte_writes
            .get(&byte_write_id)
            .is_some_and(|write| write.commit_certificate_built)
    });
    let can_publish = batch.commit_certificate_built
        && byte_write_is_durable
        && batch.certificate_persist_receipts.len() >= METADATA_QUORUM
        && root.visible_generation + 1 == batch.target_generation
        && root.owner_node == batch.owner_node
        && root.owner_epoch == batch.owner_epoch
        && root.owner_fence == batch.owner_fence
        && supplied_fence == batch.owner_fence;

    if !can_publish {
        if !byte_write_is_durable {
            state.rejected_missing_byte_cert_commits =
                state.rejected_missing_byte_cert_commits.saturating_add(1);
        }
        if supplied_fence != root.owner_fence || batch.owner_fence != root.owner_fence {
            state.rejected_stale_owner_publishes =
                state.rejected_stale_owner_publishes.saturating_add(1);
        }
        return;
    }

    root.visible_generation = batch.target_generation;
    root.root_anchor_hash = anchor_hash(batch_id, batch.target_generation);
    state
        .published
        .insert((batch.root_key, batch.target_generation), batch_id);
    state.visible_rows.insert(
        (batch.root_key, batch.target_generation),
        batch.rows.clone(),
    );

    for replica_id in &batch.certificate_persist_receipts {
        if let Some(replica) = state.replicas.get_mut(replica_id) {
            replica.committed_rows.insert(
                (batch.root_key, batch.target_generation),
                batch.rows.clone(),
            );
            replica
                .highest_generation_servable
                .insert(batch.root_key, batch.target_generation);
        }
    }

    if let Some(batch) = state.batches.get_mut(&batch_id) {
        batch.published_generation = Some(batch.target_generation);
        batch.published_with_fence = Some(supplied_fence);
    }
}

fn commit_transaction(state: &mut CoreStoreState, tx_id: TxId) -> Option<()> {
    let tx = state.transactions.get(&tx_id)?.clone();
    if tx.status != TransactionStatus::Open || state.clock >= tx.expiry_tick {
        return None;
    }
    let root = state.roots.get(&tx.root_key)?.clone();
    let batch_id = BatchId(100 + tx_id.0);
    if state.batches.contains_key(&batch_id) {
        return None;
    }

    let mut batch = CoreMetaBatch::new(
        tx.root_key,
        batch_id,
        root.visible_generation + 1,
        root.owner_node,
        root.owner_epoch,
        root.owner_fence,
        tx.staged_rows.clone(),
        None,
    );
    batch.prepare_receipts = BTreeSet::from([NodeId(0), NodeId(1)]);
    batch.commit_certificate_built = true;
    batch.certificate_persist_receipts = BTreeSet::from([NodeId(0), NodeId(1)]);
    state.batches.insert(batch_id, batch);
    publish_batch(state, batch_id, root.owner_fence);
    state.transactions.get_mut(&tx_id)?.status = TransactionStatus::Committed;
    Some(())
}

fn anchor_hash(batch_id: BatchId, generation: u8) -> u64 {
    ((batch_id.0 as u64) << 32) | generation as u64
}

fn implicit_row(batch_id: BatchId) -> RowId {
    RowId(batch_id.0 as u16 + 1)
}

fn transaction_row(tx_id: TxId, ordinal: usize) -> RowId {
    RowId(1_000 + tx_id.0 as u16 * 16 + ordinal as u16)
}
