use std::collections::{BTreeMap, BTreeSet};

use stateright::{Model, Property};

pub const NODE_COUNT: u8 = 3;
pub const QUORUM_HOLDERS: usize = 2;
pub const ERASURE_HOLDERS: usize = 3;
pub const ERASURE_DATA_SHARDS: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionId(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalKey {
    pub table: u8,
    pub partition: u8,
    pub key: u8,
}

impl LogicalKey {
    pub const fn new(table: u8, partition: u8, key: u8) -> Self {
        Self {
            table,
            partition,
            key,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RangeId(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Durability {
    Local,
    Quorum,
    Erasure,
}

impl Durability {
    fn required_holders(self) -> usize {
        match self {
            Self::Local => 1,
            Self::Quorum => QUORUM_HOLDERS,
            Self::Erasure => ERASURE_HOLDERS,
        }
    }

    fn readable_holders(self) -> usize {
        match self {
            Self::Local | Self::Quorum => 1,
            Self::Erasure => ERASURE_DATA_SHARDS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionStatus {
    Open,
    Proposed { proposal: u8 },
    Committed { version: u8 },
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Transaction {
    pub snapshot: u8,
    pub point_observations: BTreeMap<LogicalKey, u8>,
    pub range_observations: BTreeMap<RangeId, u8>,
    pub writes: BTreeSet<LogicalKey>,
    pub range_changes: BTreeSet<RangeId>,
    /// Holders cover the immutable bundle and its required durable data form.
    pub bundle_holders: BTreeMap<NodeId, u8>,
    pub durability: Durability,
    /// Exact incarnation-qualified evidence accepted with the proposal.
    pub certified_holders: BTreeMap<NodeId, u8>,
    pub status: TransactionStatus,
    pub coordinator_alive: bool,
    pub data_lost_reported: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Node {
    pub incarnation: u8,
    pub alive: bool,
    pub applied: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MvccRaftState {
    pub steps: u8,
    pub commit_version: u8,
    pub next_proposal: u8,
    pub leader: Option<NodeId>,
    pub nodes: BTreeMap<NodeId, Node>,
    pub transactions: BTreeMap<TransactionId, Transaction>,
    pub key_versions: BTreeMap<LogicalKey, u8>,
    pub key_history: BTreeSet<(LogicalKey, u8)>,
    pub range_stamps: BTreeMap<RangeId, u8>,
    pub range_history: BTreeSet<(RangeId, u8)>,
    pub committed_bundles: BTreeMap<u8, TransactionId>,
    pub visible_rows: BTreeMap<(LogicalKey, u8), TransactionId>,
    pub repair_runs: BTreeSet<(TransactionId, NodeId)>,
    pub active_snapshots: BTreeMap<TransactionId, u8>,
    pub gc_watermark: u8,
}

impl Default for MvccRaftState {
    fn default() -> Self {
        Self {
            steps: 0,
            commit_version: 0,
            next_proposal: 1,
            leader: Some(NodeId(0)),
            nodes: (0..NODE_COUNT)
                .map(|id| {
                    (
                        NodeId(id),
                        Node {
                            incarnation: 1,
                            alive: true,
                            applied: 0,
                        },
                    )
                })
                .collect(),
            transactions: BTreeMap::new(),
            key_versions: BTreeMap::new(),
            key_history: BTreeSet::new(),
            range_stamps: BTreeMap::new(),
            range_history: BTreeSet::new(),
            committed_bundles: BTreeMap::new(),
            visible_rows: BTreeMap::new(),
            repair_runs: BTreeSet::new(),
            active_snapshots: BTreeMap::new(),
            gc_watermark: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    Begin(TransactionId),
    ObservePoint(TransactionId, LogicalKey),
    ObserveRange(TransactionId, RangeId),
    Write(TransactionId, LogicalKey, RangeId),
    PersistBundle(TransactionId, NodeId, u8),
    Propose(TransactionId, Durability),
    CommitNext,
    CrashCoordinator(TransactionId),
    FailNode(NodeId),
    ReplaceNode(NodeId),
    ElectLeader(NodeId),
    ApplyCommitted(NodeId),
    Repair(TransactionId, NodeId),
    ReportDataLoss(TransactionId),
    ExternalInsert(LogicalKey, RangeId),
    ExternalDelete(LogicalKey, RangeId),
    GarbageCollect(u8),
}

impl MvccRaftState {
    pub fn apply(&self, action: Action) -> Option<Self> {
        let mut next = self.clone();
        next.steps = next.steps.saturating_add(1);
        match action {
            Action::Begin(id) => {
                if next.transactions.contains_key(&id) {
                    return None;
                }
                next.active_snapshots.insert(id, next.commit_version);
                next.transactions.insert(
                    id,
                    Transaction {
                        snapshot: next.commit_version,
                        point_observations: BTreeMap::new(),
                        range_observations: BTreeMap::new(),
                        writes: BTreeSet::new(),
                        range_changes: BTreeSet::new(),
                        bundle_holders: BTreeMap::new(),
                        durability: Durability::Local,
                        certified_holders: BTreeMap::new(),
                        status: TransactionStatus::Open,
                        coordinator_alive: true,
                        data_lost_reported: false,
                    },
                );
            }
            Action::ObservePoint(id, key) => {
                let snapshot = next.transactions.get(&id)?.snapshot;
                let version = next
                    .key_history
                    .range((key, 0)..=(key, snapshot))
                    .next_back()
                    .map_or(0, |(_, version)| *version);
                let tx = next.open_transaction_mut(id)?;
                tx.point_observations.entry(key).or_insert(version);
            }
            Action::ObserveRange(id, range) => {
                let snapshot = next.transactions.get(&id)?.snapshot;
                let stamp = next
                    .range_history
                    .range((range, 0)..=(range, snapshot))
                    .next_back()
                    .map_or(0, |(_, version)| *version);
                let tx = next.open_transaction_mut(id)?;
                tx.range_observations.entry(range).or_insert(stamp);
            }
            Action::Write(id, key, range) => {
                let tx = next.open_transaction_mut(id)?;
                tx.writes.insert(key);
                tx.range_changes.insert(range);
            }
            Action::PersistBundle(id, node, incarnation) => {
                let current = next.nodes.get(&node)?;
                if !current.alive || current.incarnation != incarnation {
                    return None;
                }
                let tx = next.open_transaction_mut(id)?;
                tx.bundle_holders.insert(node, incarnation);
            }
            Action::Propose(id, durability) => {
                let leader = next.leader.and_then(|leader| next.nodes.get(&leader))?;
                if !leader.alive {
                    return None;
                }
                let valid = next.valid_holder_count(id)?;
                let certified_holders = next
                    .transactions
                    .get(&id)?
                    .bundle_holders
                    .iter()
                    .filter(|(node, incarnation)| {
                        next.nodes
                            .get(node)
                            .is_some_and(|current| current.incarnation == **incarnation)
                    })
                    .map(|(node, incarnation)| (*node, *incarnation))
                    .collect();
                let proposal = next.next_proposal;
                let tx = next.transactions.get_mut(&id)?;
                match tx.status {
                    TransactionStatus::Open => {
                        if !tx.coordinator_alive || valid < durability.required_holders() {
                            return None;
                        }
                        tx.durability = durability;
                        tx.certified_holders = certified_holders;
                        tx.status = TransactionStatus::Proposed { proposal };
                        next.next_proposal = next.next_proposal.saturating_add(1);
                    }
                    // A duplicate proposal is an idempotent no-op and can never
                    // replace the first outcome or durability claim.
                    TransactionStatus::Proposed { .. }
                    | TransactionStatus::Committed { .. }
                    | TransactionStatus::Aborted => {}
                }
            }
            Action::CommitNext => {
                let leader = next.leader.and_then(|leader| next.nodes.get(&leader))?;
                if !leader.alive {
                    return None;
                }
                let id = next
                    .transactions
                    .iter()
                    .filter_map(|(id, tx)| match tx.status {
                        TransactionStatus::Proposed { proposal } => Some((proposal, *id)),
                        _ => None,
                    })
                    .min()?
                    .1;
                next.certify(id)?;
            }
            Action::CrashCoordinator(id) => {
                let tx = next.transactions.get_mut(&id)?;
                tx.coordinator_alive = false;
            }
            Action::FailNode(node) => {
                let state = next.nodes.get_mut(&node)?;
                state.alive = false;
                if next.leader == Some(node) {
                    next.leader = None;
                }
            }
            Action::ReplaceNode(node) => {
                let gc_watermark = next.gc_watermark;
                let state = next.nodes.get_mut(&node)?;
                state.incarnation = state.incarnation.saturating_add(1);
                state.alive = true;
                // A replacement incarnation must bootstrap at or above the GC
                // floor before it participates as a current replica.
                state.applied = gc_watermark;
                if next.leader == Some(node) {
                    next.leader = None;
                }
            }
            Action::ElectLeader(node) => {
                if !next.nodes.get(&node)?.alive {
                    return None;
                }
                next.leader = Some(node);
            }
            Action::ApplyCommitted(node) => {
                if !next.nodes.get(&node)?.alive {
                    return None;
                }
                let committed = next.committed_bundles.values().copied().collect::<Vec<_>>();
                for id in committed {
                    if id.0 == u8::MAX {
                        continue;
                    }
                    if !next.bundle_available(id) {
                        return None;
                    }
                    let incarnation = next.nodes[&node].incarnation;
                    next.transactions
                        .get_mut(&id)?
                        .bundle_holders
                        .insert(node, incarnation);
                }
                let replica = next.nodes.get_mut(&node)?;
                replica.applied = next.commit_version;
            }
            Action::Repair(id, target) => {
                if !next.nodes.get(&target)?.alive
                    || !matches!(
                        next.transactions.get(&id)?.status,
                        TransactionStatus::Committed { .. }
                    )
                    || !next.bundle_available(id)
                {
                    return None;
                }
                next.repair_runs.insert((id, target));
                let incarnation = next.nodes[&target].incarnation;
                next.transactions
                    .get_mut(&id)?
                    .bundle_holders
                    .insert(target, incarnation);
            }
            Action::ReportDataLoss(id) => {
                if next.bundle_available(id) {
                    return None;
                }
                let tx = next.transactions.get_mut(&id)?;
                if tx.durability != Durability::Local
                    || !matches!(tx.status, TransactionStatus::Committed { .. })
                {
                    return None;
                }
                tx.data_lost_reported = true;
            }
            Action::ExternalInsert(key, range) | Action::ExternalDelete(key, range) => {
                next.commit_version = next.commit_version.saturating_add(1);
                next.key_versions.insert(key, next.commit_version);
                next.key_history.insert((key, next.commit_version));
                next.range_stamps.insert(range, next.commit_version);
                next.range_history.insert((range, next.commit_version));
                next.committed_bundles
                    .insert(next.commit_version, TransactionId(u8::MAX));
            }
            Action::GarbageCollect(requested) => {
                let safe = next.gc_safe_version();
                next.gc_watermark = next.gc_watermark.max(requested.min(safe));
                next.visible_rows
                    .retain(|(_, version), _| *version >= next.gc_watermark);
            }
        }
        Some(next)
    }

    fn open_transaction_mut(&mut self, id: TransactionId) -> Option<&mut Transaction> {
        let tx = self.transactions.get_mut(&id)?;
        matches!(tx.status, TransactionStatus::Open).then_some(tx)
    }

    fn certify(&mut self, id: TransactionId) -> Option<()> {
        let conflicts = {
            let tx = self.transactions.get(&id)?;
            tx.point_observations.iter().any(|(key, observed)| {
                self.key_versions.get(key).copied().unwrap_or(0) != *observed
            }) || tx.range_observations.iter().any(|(range, observed)| {
                self.range_stamps.get(range).copied().unwrap_or(0) != *observed
            })
        };
        self.active_snapshots.remove(&id);
        if conflicts {
            self.transactions.get_mut(&id)?.status = TransactionStatus::Aborted;
            return Some(());
        }
        self.commit_version = self.commit_version.saturating_add(1);
        let version = self.commit_version;
        let tx = self.transactions.get_mut(&id)?;
        for key in &tx.writes {
            self.key_versions.insert(*key, version);
            self.key_history.insert((*key, version));
            self.visible_rows.insert((*key, version), id);
        }
        for range in &tx.range_changes {
            self.range_stamps.insert(*range, version);
            self.range_history.insert((*range, version));
        }
        tx.status = TransactionStatus::Committed { version };
        self.committed_bundles.insert(version, id);
        Some(())
    }

    pub fn valid_holder_count(&self, id: TransactionId) -> Option<usize> {
        let tx = self.transactions.get(&id)?;
        Some(
            tx.bundle_holders
                .iter()
                .filter(|(node, incarnation)| {
                    self.nodes
                        .get(node)
                        .is_some_and(|current| current.incarnation == **incarnation)
                })
                .count(),
        )
    }

    pub fn bundle_available(&self, id: TransactionId) -> bool {
        let Some(tx) = self.transactions.get(&id) else {
            return false;
        };
        let alive = tx
            .bundle_holders
            .iter()
            .filter(|(node, incarnation)| {
                self.nodes
                    .get(node)
                    .is_some_and(|current| current.alive && current.incarnation == **incarnation)
            })
            .count();
        alive >= tx.durability.readable_holders()
    }

    pub fn gc_safe_version(&self) -> u8 {
        self.active_snapshots
            .values()
            .copied()
            .chain(
                self.nodes
                    .values()
                    .filter(|node| node.alive)
                    .map(|node| node.applied),
            )
            .min()
            .unwrap_or(self.commit_version)
    }

    pub fn outcomes_are_unique(&self) -> bool {
        self.committed_bundles
            .values()
            .filter(|id| id.0 != u8::MAX)
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            == self
                .transactions
                .values()
                .filter(|tx| matches!(tx.status, TransactionStatus::Committed { .. }))
                .count()
    }

    pub fn committed_writes_are_atomic(&self) -> bool {
        self.transactions.iter().all(|(id, tx)| {
            let TransactionStatus::Committed { version } = tx.status else {
                return true;
            };
            version < self.gc_watermark
                || tx
                    .writes
                    .iter()
                    .all(|key| self.visible_rows.get(&(*key, version)) == Some(id))
        })
    }

    pub fn prepared_bundles_are_invisible(&self) -> bool {
        self.transactions.iter().all(|(id, tx)| {
            !matches!(
                tx.status,
                TransactionStatus::Open | TransactionStatus::Proposed { .. }
            ) || !self.visible_rows.values().any(|visible| visible == id)
        })
    }

    pub fn applied_watermarks_are_bounded(&self) -> bool {
        self.nodes
            .values()
            .all(|node| node.applied <= self.commit_version)
    }

    pub fn durability_claims_are_honest(&self) -> bool {
        self.transactions.values().all(|tx| {
            !matches!(
                tx.status,
                TransactionStatus::Proposed { .. } | TransactionStatus::Committed { .. }
            ) || tx.certified_holders.len() >= tx.durability.required_holders()
        })
    }

    pub fn minority_failure_is_reconstructable(&self) -> bool {
        self.transactions.values().all(|tx| {
            if !matches!(tx.status, TransactionStatus::Committed { .. })
                || tx.durability == Durability::Local
            {
                return true;
            }
            let live_certified = tx
                .certified_holders
                .iter()
                .filter(|(node, incarnation)| {
                    self.nodes.get(node).is_some_and(|current| {
                        current.alive && current.incarnation == **incarnation
                    })
                })
                .count();
            let unavailable = tx.certified_holders.len() - live_certified;
            unavailable > 1 || live_certified >= tx.durability.readable_holders()
        })
    }

    pub fn gc_respects_readers(&self) -> bool {
        self.gc_watermark <= self.gc_safe_version()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MvccRaftModel {
    pub max_steps: u8,
}

impl MvccRaftModel {
    pub fn small() -> Self {
        Self {
            max_steps: if cfg!(feature = "exhaustive-small") {
                6
            } else {
                4
            },
        }
    }
}

impl Model for MvccRaftModel {
    type State = MvccRaftState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![MvccRaftState::default()]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.steps >= self.max_steps {
            return;
        }
        for tx in 0..2 {
            let id = TransactionId(tx);
            let Some(transaction) = state.transactions.get(&id) else {
                actions.push(Action::Begin(id));
                continue;
            };
            if matches!(transaction.status, TransactionStatus::Open) {
                actions.push(Action::ObservePoint(id, LogicalKey::new(0, 0, 0)));
                actions.push(Action::ObserveRange(id, RangeId(0)));
                actions.push(Action::Write(id, LogicalKey::new(tx, tx, 0), RangeId(tx)));
                for node in 0..NODE_COUNT {
                    let node = NodeId(node);
                    if state.nodes[&node].alive
                        && transaction.bundle_holders.get(&node)
                            != Some(&state.nodes[&node].incarnation)
                    {
                        actions.push(Action::PersistBundle(
                            id,
                            node,
                            state.nodes[&node].incarnation,
                        ));
                    }
                }
                if transaction.coordinator_alive {
                    for durability in [Durability::Local, Durability::Quorum, Durability::Erasure] {
                        if state.valid_holder_count(id).unwrap_or(0)
                            >= durability.required_holders()
                        {
                            actions.push(Action::Propose(id, durability));
                        }
                    }
                    actions.push(Action::CrashCoordinator(id));
                }
            } else if matches!(transaction.status, TransactionStatus::Proposed { .. })
                && transaction.coordinator_alive
            {
                actions.push(Action::CrashCoordinator(id));
            } else if matches!(transaction.status, TransactionStatus::Committed { .. })
                && state.bundle_available(id)
            {
                for node in 0..NODE_COUNT {
                    let node = NodeId(node);
                    if state.nodes[&node].alive {
                        actions.push(Action::Repair(id, node));
                    }
                }
            }
        }
        if state
            .leader
            .is_some_and(|leader| state.nodes[&leader].alive)
            && state
                .transactions
                .values()
                .any(|tx| matches!(tx.status, TransactionStatus::Proposed { .. }))
        {
            actions.push(Action::CommitNext);
        }
        actions.push(Action::ExternalInsert(LogicalKey::new(0, 0, 0), RangeId(0)));
        if state.gc_watermark < state.gc_safe_version() {
            actions.push(Action::GarbageCollect(state.commit_version));
        }
        for node in 0..NODE_COUNT {
            let node = NodeId(node);
            if state.nodes[&node].alive {
                actions.push(Action::FailNode(node));
                if state.leader.is_none() {
                    actions.push(Action::ElectLeader(node));
                }
                if state.nodes[&node].applied < state.commit_version {
                    actions.push(Action::ApplyCommitted(node));
                }
            } else {
                actions.push(Action::ReplaceNode(node));
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        state.apply(action)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "one outcome per transaction",
                |_: &Self, state: &MvccRaftState| state.outcomes_are_unique(),
            ),
            Property::always(
                "committed bundles are atomically visible",
                |_: &Self, state: &MvccRaftState| state.committed_writes_are_atomic(),
            ),
            Property::always(
                "prepared bundles remain invisible",
                |_: &Self, state: &MvccRaftState| state.prepared_bundles_are_invisible(),
            ),
            Property::always(
                "replica watermarks do not exceed commit order",
                |_: &Self, state: &MvccRaftState| state.applied_watermarks_are_bounded(),
            ),
            Property::always(
                "durability claims are backed by holders",
                |_: &Self, state: &MvccRaftState| state.durability_claims_are_honest(),
            ),
            Property::always(
                "quorum and erasure survive one holder failure",
                |_: &Self, state: &MvccRaftState| state.minority_failure_is_reconstructable(),
            ),
            Property::always(
                "GC preserves active and lagging readers",
                |_: &Self, state: &MvccRaftState| state.gc_respects_readers(),
            ),
        ]
    }
}
