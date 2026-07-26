//! The only module permitted to name concrete OpenRaft application types.

use std::{
    fmt::Debug,
    io::Cursor,
    ops::{Bound, RangeBounds},
};

use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState,
    OptionalSend, RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
};
use serde::{Deserialize, Serialize};

use crate::{
    CertificationResult, CertificationState, CertifyTransaction, CommitVersion, RaftStorageError,
    RocksRaftStore,
    storage::{KEY_LAST_PURGED_LOG_ID, KEY_OPENRAFT_STATE},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RaftApplyResult {
    Certification(CertificationResult),
    Noop,
    Rejected(String),
}

openraft::declare_raft_types!(
    pub(crate) AnvilRaftConfig:
        D = CertifyTransaction,
        R = RaftApplyResult,
        NodeId = u64,
        Node = openraft::BasicNode,
);

pub(crate) type RaftEntry = Entry<AnvilRaftConfig>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MachineState {
    certification: CertificationState,
    last_applied_log_id: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    snapshot_generation: u64,
}

impl Default for MachineState {
    fn default() -> Self {
        Self {
            certification: CertificationState::default(),
            last_applied_log_id: None,
            membership: StoredMembership::default(),
            snapshot_generation: 0,
        }
    }
}

/// Concrete OpenRaft V2 log store backed by the existing RocksDB.
#[derive(Clone)]
pub(crate) struct OpenRaftLogStore {
    store: RocksRaftStore,
}

/// Concrete OpenRaft V2 state machine backed by the existing RocksDB.
#[derive(Clone)]
pub(crate) struct OpenRaftStateMachine {
    store: RocksRaftStore,
}

pub(crate) fn stores(store: RocksRaftStore) -> (OpenRaftLogStore, OpenRaftStateMachine) {
    (
        OpenRaftLogStore {
            store: store.clone(),
        },
        OpenRaftStateMachine { store },
    )
}

fn storage_error(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    error: impl ToString,
) -> StorageError<u64> {
    StorageIOError::new(subject, verb, AnyError::error(error)).into()
}

fn read_error(error: RaftStorageError) -> StorageError<u64> {
    storage_error(ErrorSubject::Store, ErrorVerb::Read, error)
}

fn write_error(error: RaftStorageError) -> StorageError<u64> {
    storage_error(ErrorSubject::Store, ErrorVerb::Write, error)
}

fn range_start<R: RangeBounds<u64>>(range: &R) -> u64 {
    match range.start_bound() {
        Bound::Included(value) => *value,
        Bound::Excluded(value) => value.saturating_add(1),
        Bound::Unbounded => 0,
    }
}

fn range_end<R: RangeBounds<u64>>(range: &R) -> u64 {
    match range.end_bound() {
        Bound::Included(value) => value.saturating_add(1),
        Bound::Excluded(value) => *value,
        Bound::Unbounded => u64::MAX,
    }
}

impl RaftLogReader<AnvilRaftConfig> for OpenRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftEntry>, StorageError<u64>> {
        self.store
            .scan_logs(range_start(&range), range_end(&range))
            .map_err(read_error)?
            .into_iter()
            .map(|(_, bytes)| {
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                    .map(|(entry, _)| entry)
                    .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))
            })
            .collect()
    }
}

impl RaftLogStorage<AnvilRaftConfig> for OpenRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<AnvilRaftConfig>, StorageError<u64>> {
        let last_purged_log_id = self
            .store
            .read_meta(KEY_LAST_PURGED_LOG_ID)
            .map_err(read_error)?;
        let last_log_id = match self.store.last_log_index().map_err(read_error)? {
            Some(index) => {
                let bytes = self
                    .store
                    .get_log(index)
                    .map_err(read_error)?
                    .ok_or_else(|| {
                        storage_error(
                            ErrorSubject::LogIndex(index),
                            ErrorVerb::Read,
                            "last log metadata points to a missing entry",
                        )
                    })?;
                let (entry, _): (RaftEntry, _) =
                    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                        .map_err(|error| {
                            storage_error(ErrorSubject::LogIndex(index), ErrorVerb::Read, error)
                        })?;
                Some(entry.log_id)
            }
            None => last_purged_log_id,
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.store.save_vote(vote).map_err(write_error)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.store.read_vote().map_err(read_error)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<AnvilRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = RaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let encoded = entries
            .into_iter()
            .map(|entry| {
                let index = entry.log_id.index;
                bincode::serde::encode_to_vec(&entry, bincode::config::standard())
                    .map(|bytes| (index, bytes))
                    .map_err(|error| {
                        storage_error(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, error)
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        match self.store.append_logs(&encoded) {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                callback.log_io_completed(Err(std::io::Error::other(message.clone())));
                Err(storage_error(ErrorSubject::Logs, ErrorVerb::Write, message))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.store.truncate_logs(log_id.index).map_err(write_error)
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.store
            .purge_logs_with_id(log_id.index, &log_id)
            .map_err(write_error)
    }
}

#[derive(Clone)]
pub(crate) struct OpenRaftSnapshotBuilder {
    store: RocksRaftStore,
}

impl RaftSnapshotBuilder<AnvilRaftConfig> for OpenRaftSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<AnvilRaftConfig>, StorageError<u64>> {
        let mut state: MachineState = self
            .store
            .read_state_value(KEY_OPENRAFT_STATE)
            .map_err(read_error)?
            .unwrap_or_default();
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        self.store
            .sync_state_value(KEY_OPENRAFT_STATE, &state)
            .map_err(write_error)?;
        let bytes = bincode::serde::encode_to_vec(&state, bincode::config::standard())
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Read, error))?;
        let meta = SnapshotMeta {
            last_log_id: state.last_applied_log_id,
            last_membership: state.membership,
            snapshot_id: format!(
                "{}-{}",
                state.last_applied_log_id.map_or(0, |log_id| log_id.index),
                state.snapshot_generation
            ),
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<AnvilRaftConfig> for OpenRaftStateMachine {
    type SnapshotBuilder = OpenRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let state: MachineState = self
            .store
            .read_state_value(KEY_OPENRAFT_STATE)
            .map_err(read_error)?
            .unwrap_or_default();
        Ok((state.last_applied_log_id, state.membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftApplyResult>, StorageError<u64>>
    where
        I: IntoIterator<Item = RaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut state: MachineState = self
            .store
            .read_state_value(KEY_OPENRAFT_STATE)
            .map_err(read_error)?
            .unwrap_or_default();
        let mut responses = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            let response = match entry.payload {
                EntryPayload::Blank => RaftApplyResult::Noop,
                EntryPayload::Membership(membership) => {
                    state.membership = StoredMembership::new(Some(log_id), membership);
                    RaftApplyResult::Noop
                }
                EntryPayload::Normal(command) => match state
                    .certification
                    .apply(CommitVersion(log_id.index), &command)
                {
                    Ok(result) => RaftApplyResult::Certification(result),
                    Err(error) => RaftApplyResult::Rejected(error.to_string()),
                },
            };
            state.last_applied_log_id = Some(log_id);
            responses.push(response);
        }
        self.store
            .sync_state_value(KEY_OPENRAFT_STATE, &state)
            .map_err(write_error)?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        OpenRaftSnapshotBuilder {
            store: self.store.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let bytes = snapshot.into_inner();
        let (state, consumed): (MachineState, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).map_err(
                |error| {
                    storage_error(
                        ErrorSubject::Snapshot(Some(meta.signature())),
                        ErrorVerb::Read,
                        error,
                    )
                },
            )?;
        if consumed != bytes.len()
            || state.last_applied_log_id != meta.last_log_id
            || state.membership != meta.last_membership
        {
            return Err(storage_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                "snapshot body does not match its metadata",
            ));
        }
        self.store
            .sync_state_value(KEY_OPENRAFT_STATE, &state)
            .map_err(write_error)
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<AnvilRaftConfig>>, StorageError<u64>> {
        if self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(read_error)?
            .is_none()
        {
            return Ok(None);
        }
        let mut builder = self.get_snapshot_builder().await;
        builder.build_snapshot().await.map(Some)
    }
}

#[cfg(test)]
mod tests {
    use openraft::{CommittedLeaderId, RaftTypeConfig, storage::RaftLogStorage};
    use tempfile::TempDir;

    use super::*;
    use crate::{BundleHash, DurabilityLevel, NodeId, NodeIncarnation, TransactionId};

    #[test]
    fn application_payload_is_the_compact_certification_command() {
        fn assert_data_type<C: RaftTypeConfig<D = CertifyTransaction>>() {}
        fn assert_response_type<C: RaftTypeConfig<R = RaftApplyResult>>() {}
        assert_data_type::<AnvilRaftConfig>();
        assert_response_type::<AnvilRaftConfig>();
    }

    #[test]
    fn concrete_types_implement_openraft_v2_storage_contracts() {
        fn assert_log<T: RaftLogStorage<AnvilRaftConfig>>() {}
        fn assert_state<T: RaftStateMachine<AnvilRaftConfig>>() {}
        assert_log::<OpenRaftLogStore>();
        assert_state::<OpenRaftStateMachine>();

        let directory = TempDir::new().unwrap();
        let (log, state) = stores(RocksRaftStore::open(directory.path(), 0).unwrap());
        drop((log, state));
    }

    #[tokio::test]
    async fn duplicate_transaction_advances_raft_applied_id_but_keeps_commit_version() {
        let directory = TempDir::new().unwrap();
        let (_, mut machine) = stores(RocksRaftStore::open(directory.path(), 0).unwrap());
        let command = CertifyTransaction {
            transaction_id: TransactionId([1; 16]),
            snapshot_version: CommitVersion(0),
            point_observations: vec![],
            range_observations: vec![],
            written_point_keys: vec![],
            advanced_range_stamps: vec![],
            bundle_hash: BundleHash([2; 32]),
            bundle_length: 1,
            durability: DurabilityLevel::Local,
            durable_holders: vec![NodeIncarnation {
                node_id: NodeId(1),
                incarnation: 1,
            }],
        };
        let log_id_1 = LogId::new(CommittedLeaderId::new(1, 1), 1);
        let log_id_9 = LogId::new(CommittedLeaderId::new(1, 1), 9);
        let first = machine
            .apply([Entry {
                log_id: log_id_1,
                payload: EntryPayload::Normal(command.clone()),
            }])
            .await
            .unwrap();
        let retry = machine
            .apply([Entry {
                log_id: log_id_9,
                payload: EntryPayload::Normal(command),
            }])
            .await
            .unwrap();
        assert_eq!(first, retry);
        assert!(matches!(
            retry.as_slice(),
            [RaftApplyResult::Certification(
                CertificationResult::Committed {
                    commit_version: CommitVersion(1),
                    ..
                }
            )]
        ));
        assert_eq!(machine.applied_state().await.unwrap().0, Some(log_id_9));
    }
}
