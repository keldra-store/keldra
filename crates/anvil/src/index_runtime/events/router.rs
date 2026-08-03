//! One disposable node-local fan-out for complete all-source journal barriers.
//!
//! The collector is the only reader of the cluster source journals on this
//! node. Local builders receive shared immutable batches and never trigger a
//! source fetch. Nothing here is authoritative or persisted.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{RwLock, watch};

use super::{IndexBarrier, IndexEventError, IndexEventJournal, IndexJournalBatch};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexEventRouterRetention {
    max_batches: usize,
    max_changes: usize,
}

impl IndexEventRouterRetention {
    pub(crate) fn new(
        max_batches: usize,
        max_changes: usize,
    ) -> Result<Self, IndexEventRouterError> {
        if max_batches == 0 || max_changes == 0 {
            return Err(IndexEventRouterError::InvalidRetention);
        }
        Ok(Self {
            max_batches,
            max_changes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexRescanReason {
    HistoryUnavailable,
    SourceEpochUnavailable,
    PlacementFenceUnavailable,
}

#[derive(Clone, Debug)]
pub(crate) enum IndexEventCatchUp {
    Available {
        /// Consecutive complete vector-barrier batches. No ordering between
        /// different source journals is implied by their internal vectors.
        batches: Vec<Arc<IndexJournalBatch>>,
        through: IndexBarrier,
    },
    RescanRequired {
        reason: IndexRescanReason,
        current: IndexBarrier,
    },
}

#[derive(Clone)]
pub(crate) struct IndexEventRouter {
    state: Arc<RwLock<RouterState>>,
}

impl IndexEventRouter {
    pub(crate) async fn start(
        journal: Arc<IndexEventJournal>,
        retention: IndexEventRouterRetention,
        poll_interval: Duration,
    ) -> Result<(Self, IndexEventRouterTask), IndexEventRouterError> {
        if poll_interval.is_zero() {
            return Err(IndexEventRouterError::InvalidPollInterval);
        }
        let initial = journal.capture_barrier().await?;
        let state = Arc::new(RwLock::new(RouterState::new(initial)));
        let router = Self {
            state: state.clone(),
        };
        let (stop, mut stop_signal) = watch::channel(false);
        let task = tokio::spawn(async move {
            let first = tokio::time::Instant::now() + poll_interval;
            let mut interval = tokio::time::interval_at(first, poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = stop_signal.changed() => {
                        if changed.is_err() || *stop_signal.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if let Err(error) = collect_once(&journal, &state, retention).await {
                            tracing::warn!(%error, "index event router requires a later retry or builder rescan");
                        }
                    }
                }
            }
        });
        Ok((
            router,
            IndexEventRouterTask {
                stop,
                task: Some(task),
            },
        ))
    }

    pub(crate) async fn current_barrier(&self) -> IndexBarrier {
        self.state.read().await.current().clone()
    }

    /// Return every retained complete batch after exactly `after`.
    ///
    /// A merely similar cursor is never accepted. Losing the relevant fence,
    /// source epoch, or bounded history explicitly requires a source rescan.
    pub(crate) async fn changes_after(&self, after: &IndexBarrier) -> IndexEventCatchUp {
        self.state.read().await.changes_after(after)
    }
}

pub(crate) struct IndexEventRouterTask {
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl IndexEventRouterTask {
    pub(crate) async fn shutdown(mut self) -> Result<(), IndexEventRouterError> {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| IndexEventRouterError::Task(error.to_string()))?;
        }
        Ok(())
    }
}

impl Drop for IndexEventRouterTask {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct RouterState {
    /// Exact barrier immediately preceding the first retained batch.
    anchor: IndexBarrier,
    batches: VecDeque<Arc<IndexJournalBatch>>,
    retained_changes: usize,
}

impl RouterState {
    fn new(anchor: IndexBarrier) -> Self {
        Self {
            anchor,
            batches: VecDeque::new(),
            retained_changes: 0,
        }
    }

    fn current(&self) -> &IndexBarrier {
        self.batches
            .back()
            .map_or(&self.anchor, |batch| &batch.through)
    }

    fn append(&mut self, batch: IndexJournalBatch, retention: IndexEventRouterRetention) {
        self.retained_changes = self.retained_changes.saturating_add(batch.changes.len());
        self.batches.push_back(Arc::new(batch));
        while self.batches.len() > retention.max_batches
            || self.retained_changes > retention.max_changes
        {
            let Some(expired) = self.batches.pop_front() else {
                break;
            };
            self.retained_changes = self.retained_changes.saturating_sub(expired.changes.len());
            self.anchor = expired.through.clone();
        }
    }

    fn rebase(&mut self, current: IndexBarrier) {
        self.anchor = current;
        self.batches.clear();
        self.retained_changes = 0;
    }

    fn changes_after(&self, after: &IndexBarrier) -> IndexEventCatchUp {
        let current = self.current().clone();
        let start = if after == &self.anchor {
            Some(0)
        } else {
            self.batches
                .iter()
                .position(|batch| &batch.through == after)
                .map(|index| index + 1)
        };
        let Some(start) = start else {
            return IndexEventCatchUp::RescanRequired {
                reason: rescan_reason(after, &current),
                current,
            };
        };
        IndexEventCatchUp::Available {
            batches: self.batches.iter().skip(start).cloned().collect(),
            through: current,
        }
    }
}

async fn collect_once(
    journal: &IndexEventJournal,
    state: &RwLock<RouterState>,
    retention: IndexEventRouterRetention,
) -> Result<(), IndexEventError> {
    let from = state.read().await.current().clone();
    let target = journal.capture_barrier().await?;
    if target == from {
        return Ok(());
    }
    match journal.drain(&from, target.clone()).await {
        Ok(batch) => state.write().await.append(batch, retention),
        Err(error) => {
            // The complete target vector is safe as a new rescan anchor even
            // when the intervening history could not be drained.
            state.write().await.rebase(target);
            return Err(error);
        }
    }
    Ok(())
}

fn rescan_reason(after: &IndexBarrier, current: &IndexBarrier) -> IndexRescanReason {
    if after.fence != current.fence || after.sources.keys().ne(current.sources.keys()) {
        return IndexRescanReason::PlacementFenceUnavailable;
    }
    if after
        .sources
        .iter()
        .any(|(node, cursor)| cursor.source != current.sources[node].source)
    {
        return IndexRescanReason::SourceEpochUnavailable;
    }
    IndexRescanReason::HistoryUnavailable
}

#[derive(Debug, Error)]
pub(crate) enum IndexEventRouterError {
    #[error("index event router retention bounds must be positive")]
    InvalidRetention,
    #[error("index event router poll interval must be positive")]
    InvalidPollInterval,
    #[error(transparent)]
    Journal(#[from] IndexEventError),
    #[error("index event router task failed: {0}")]
    Task(String),
}

#[cfg(test)]
#[path = "router/tests.rs"]
mod tests;
