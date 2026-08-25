//! Process-local registry for readers pinned to immutable committed views.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{IndexDiskLease, ManifestArtifactDirectory, SelectedCommittedIndexView};
use crate::index_runtime::cache::CACHE_DISK_LEASE_MINIMUM_BYTES;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct OpenedCommittedViewKey {
    pub(super) storage_tenant: String,
    pub(super) bucket: String,
    pub(super) tenant_id: u64,
    pub(super) bucket_id: u64,
    pub(super) index_id: u64,
    pub(super) definition_version: u64,
}

#[derive(Clone)]
pub(super) struct OpenedCommittedViewRegistry {
    maximum_entries: usize,
    maximum_bytes: u64,
    state: Arc<Mutex<OpenedCommittedViewRegistryState>>,
    changed: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct OpenedCommittedViewRegistryState {
    clock: u64,
    leased_bytes: u64,
    entries: BTreeMap<OpenedCommittedViewKey, OpenedCommittedViewEntry>,
}

#[derive(Clone)]
pub(super) struct OpenedCommittedIndexView {
    pub(super) selected: SelectedCommittedIndexView,
    pub(super) directory: ManifestArtifactDirectory,
    pub(super) disk_leases: Vec<IndexDiskLease>,
}

struct OpenedCommittedViewEntry {
    opened: Option<OpenedCommittedIndexView>,
    refreshing: bool,
    last_refresh_started: Option<std::time::Instant>,
    last_used: u64,
}

#[derive(Clone, Copy)]
pub(super) enum CommittedViewOpenReason {
    ExactRevision,
    Initial,
    Freshness,
    Background,
}

impl CommittedViewOpenReason {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::ExactRevision => "exact_revision",
            Self::Initial => "initial",
            Self::Freshness => "freshness",
            Self::Background => "background",
        }
    }
}

impl OpenedCommittedViewRegistry {
    pub(super) fn new(maximum_entries: usize, maximum_bytes: u64) -> Self {
        Self {
            maximum_entries: maximum_entries.max(1),
            maximum_bytes: maximum_bytes.max(1),
            state: Arc::new(Mutex::new(OpenedCommittedViewRegistryState::default())),
            changed: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// `None` means this index has not been opened. `Some(None)` is a verified
    /// empty current pointer and prevents repeated object-plane reads while its
    /// asynchronous reopen is in flight.
    pub(super) fn get(
        &self,
        key: &OpenedCommittedViewKey,
    ) -> Option<Option<OpenedCommittedIndexView>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.clock = state.clock.wrapping_add(1).max(1);
        let clock = state.clock;
        let entry = state.entries.get_mut(key)?;
        entry.last_used = clock;
        Some(entry.opened.clone())
    }

    pub(super) fn install(
        &self,
        key: OpenedCommittedViewKey,
        opened: Option<OpenedCommittedIndexView>,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.clock = state.clock.wrapping_add(1).max(1);
        let clock = state.clock;
        let leased_bytes = opened.as_ref().map_or(0, opened_view_bytes);
        if let Some(replaced) = state.entries.remove(&key) {
            state.leased_bytes = state
                .leased_bytes
                .saturating_sub(replaced.opened.as_ref().map_or(0, opened_view_bytes));
        }
        state.leased_bytes = state.leased_bytes.saturating_add(leased_bytes);
        state.entries.insert(
            key,
            OpenedCommittedViewEntry {
                opened,
                refreshing: false,
                last_refresh_started: None,
                last_used: clock,
            },
        );
        evict_to_budget(&mut state, self.maximum_entries, self.maximum_bytes);
    }

    pub(super) fn begin_refresh(
        &self,
        key: &OpenedCommittedViewKey,
        minimum_interval: Duration,
    ) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = state.entries.get_mut(key) else {
            return false;
        };
        if entry.refreshing {
            return false;
        }
        let now = std::time::Instant::now();
        if entry
            .last_refresh_started
            .is_some_and(|started| now.duration_since(started) < minimum_interval)
        {
            return false;
        }
        entry.refreshing = true;
        entry.last_refresh_started = Some(now);
        true
    }

    pub(super) fn finish_refresh(
        &self,
        key: OpenedCommittedViewKey,
        result: Option<Option<OpenedCommittedIndexView>>,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let replacement_bytes = {
            let Some(entry) = state.entries.get_mut(&key) else {
                return;
            };
            entry.refreshing = false;
            let Some(opened) = result else {
                return;
            };
            let does_not_regress = match (&entry.opened, &opened) {
                (Some(current), Some(next)) => {
                    next.selected.manifest.revision >= current.selected.manifest.revision
                }
                (Some(_), None) => false,
                (None, _) => true,
            };
            does_not_regress.then(|| {
                let previous = entry.opened.as_ref().map_or(0, opened_view_bytes);
                let next = opened.as_ref().map_or(0, opened_view_bytes);
                entry.opened = opened;
                (previous, next)
            })
        };
        if let Some((previous, next)) = replacement_bytes {
            state.leased_bytes = state
                .leased_bytes
                .saturating_sub(previous)
                .saturating_add(next);
        }
        evict_to_budget(&mut state, self.maximum_entries, self.maximum_bytes);
        self.changed.notify_waiters();
    }

    pub(super) fn changed(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.changed.notified()
    }
}

fn opened_view_bytes(view: &OpenedCommittedIndexView) -> u64 {
    view.disk_leases
        .iter()
        .map(IndexDiskLease::bytes)
        .fold(0_u64, u64::saturating_add)
}

pub(super) fn opened_pack_charge(lengths: impl IntoIterator<Item = u64>) -> u64 {
    lengths.into_iter().fold(0_u64, |total, length| {
        total.saturating_add(length.max(CACHE_DISK_LEASE_MINIMUM_BYTES))
    })
}

fn evict_to_budget(
    state: &mut OpenedCommittedViewRegistryState,
    maximum_entries: usize,
    maximum_bytes: u64,
) {
    while state.entries.len() > maximum_entries || state.leased_bytes > maximum_bytes {
        let Some(oldest) = state
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        if let Some(evicted) = state.entries.remove(&oldest) {
            state.leased_bytes = state
                .leased_bytes
                .saturating_sub(evicted.opened.as_ref().map_or(0, opened_view_bytes));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opened_pack_charge_bounds_tiny_pack_metadata() {
        assert_eq!(
            opened_pack_charge([1, 1]),
            2 * CACHE_DISK_LEASE_MINIMUM_BYTES
        );
        assert_eq!(opened_pack_charge([8 * 1024]), 8 * 1024);
    }
}
