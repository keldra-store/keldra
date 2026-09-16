//! Atomic all-or-none admission for mutation conflict resources.
//!
//! A waiter never owns a proper subset of its requested resources.  This
//! avoids the convoy created by acquiring per-resource mutexes one at a time
//! while retaining FIFO order independently for every resource.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

#[derive(Clone, Default)]
pub(super) struct MutationConflictScheduler {
    inner: Arc<Mutex<SchedulerState>>,
}

#[derive(Default)]
struct SchedulerState {
    next_ticket: u64,
    active: BTreeSet<Vec<u8>>,
    waiting: VecDeque<Waiter>,
    /// A successful notification transfers a grant to the receiving future.
    /// Keeping it here until claimed makes cancellation after notification,
    /// but before the future is next polled, release the resources safely.
    granted: BTreeMap<u64, BTreeSet<Vec<u8>>>,
}

struct Waiter {
    ticket: u64,
    resources: BTreeSet<Vec<u8>>,
    ready: oneshot::Sender<()>,
}

pub(super) struct MutationConflictGuard {
    scheduler: MutationConflictScheduler,
    resources: BTreeSet<Vec<u8>>,
}

pub(super) struct MutationConflictAcquisition {
    pending: PendingAcquisition,
    ready: oneshot::Receiver<()>,
}

struct PendingAcquisition {
    scheduler: MutationConflictScheduler,
    ticket: u64,
    armed: bool,
}

impl MutationConflictScheduler {
    pub(super) async fn acquire(
        &self,
        resources: impl IntoIterator<Item = Vec<u8>>,
    ) -> MutationConflictGuard {
        self.register(resources).acquire().await
    }

    /// Atomically registers the complete set before waiting for its grant.
    /// Callers may use this split phase to publish ordered registration to a
    /// successor without serializing the actual conflict wait.
    pub(super) fn register(
        &self,
        resources: impl IntoIterator<Item = Vec<u8>>,
    ) -> MutationConflictAcquisition {
        let resources = resources.into_iter().collect::<BTreeSet<_>>();
        let (ticket, ready) = self.enqueue(resources);
        MutationConflictAcquisition {
            pending: PendingAcquisition {
                scheduler: self.clone(),
                ticket,
                armed: true,
            },
            ready,
        }
    }

    fn enqueue(&self, resources: BTreeSet<Vec<u8>>) -> (u64, oneshot::Receiver<()>) {
        let (ready, receiver) = oneshot::channel();
        let mut state = self
            .inner
            .lock()
            .expect("mutation conflict scheduler lock is not poisoned");
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .expect("mutation conflict scheduler ticket space is exhausted");
        let ticket = state.next_ticket;
        state.waiting.push_back(Waiter {
            ticket,
            resources,
            ready,
        });
        schedule_waiters(&mut state);
        (ticket, receiver)
    }

    fn claim(&self, ticket: u64) -> Option<BTreeSet<Vec<u8>>> {
        self.inner
            .lock()
            .expect("mutation conflict scheduler lock is not poisoned")
            .granted
            .remove(&ticket)
    }

    fn cancel(&self, ticket: u64) {
        let mut state = self
            .inner
            .lock()
            .expect("mutation conflict scheduler lock is not poisoned");
        if let Some(position) = state
            .waiting
            .iter()
            .position(|waiter| waiter.ticket == ticket)
        {
            state.waiting.remove(position);
        } else if let Some(resources) = state.granted.remove(&ticket) {
            for resource in resources {
                state.active.remove(&resource);
            }
        } else {
            return;
        }
        schedule_waiters(&mut state);
    }

    fn release(&self, resources: &BTreeSet<Vec<u8>>) {
        let mut state = self
            .inner
            .lock()
            .expect("mutation conflict scheduler lock is not poisoned");
        for resource in resources {
            let removed = state.active.remove(resource);
            debug_assert!(
                removed,
                "a mutation conflict guard owns every resource it releases"
            );
        }
        schedule_waiters(&mut state);
    }

    #[cfg(test)]
    pub(super) fn waiting(&self) -> usize {
        self.inner
            .lock()
            .expect("mutation conflict scheduler lock is not poisoned")
            .waiting
            .len()
    }
}

impl MutationConflictAcquisition {
    pub(super) async fn acquire(self) -> MutationConflictGuard {
        let Self { mut pending, ready } = self;
        let ticket = pending.ticket;
        ready
            .await
            .expect("mutation conflict scheduler retains every queued notification");
        let resources = pending
            .scheduler
            .claim(ticket)
            .expect("notified mutation conflict grant remains claimable");
        pending.armed = false;
        MutationConflictGuard {
            scheduler: pending.scheduler.clone(),
            resources,
        }
    }
}

impl Drop for MutationConflictGuard {
    fn drop(&mut self) {
        self.scheduler.release(&self.resources);
    }
}

impl Drop for PendingAcquisition {
    fn drop(&mut self) {
        if self.armed {
            self.scheduler.cancel(self.ticket);
        }
    }
}

fn intersects(left: &BTreeSet<Vec<u8>>, right: &BTreeSet<Vec<u8>>) -> bool {
    left.iter().any(|resource| right.contains(resource))
}

fn schedule_waiters(state: &mut SchedulerState) {
    let mut retained = VecDeque::with_capacity(state.waiting.len());
    let mut earlier_blocked_resources = BTreeSet::new();
    while let Some(waiter) = state.waiting.pop_front() {
        if intersects(&waiter.resources, &state.active)
            || intersects(&waiter.resources, &earlier_blocked_resources)
        {
            earlier_blocked_resources.extend(waiter.resources.iter().cloned());
            retained.push_back(waiter);
            continue;
        }

        state.active.extend(waiter.resources.iter().cloned());
        state
            .granted
            .insert(waiter.ticket, waiter.resources.clone());
        if waiter.ready.send(()).is_err() {
            let resources = state
                .granted
                .remove(&waiter.ticket)
                .expect("failed notification still owns its conflict grant");
            for resource in resources {
                state.active.remove(&resource);
            }
        }
    }
    state.waiting = retained;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resource(name: &str) -> Vec<u8> {
        name.as_bytes().to_vec()
    }

    #[tokio::test]
    async fn a_waiter_never_holds_a_partial_resource_set() {
        let scheduler = MutationConflictScheduler::default();
        let held_b = scheduler.acquire([resource("b")]).await;
        let waiting_ab = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.acquire([resource("a"), resource("b")]).await }
        });
        while scheduler.waiting() != 1 {
            tokio::task::yield_now().await;
        }

        let independent_a = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.acquire([resource("a")]).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting_ab.is_finished());
        assert!(!independent_a.is_finished());

        drop(held_b);
        let held_ab = tokio::time::timeout(std::time::Duration::from_secs(1), waiting_ab)
            .await
            .expect("the oldest waiter acquires its complete resource set")
            .unwrap();
        assert!(!independent_a.is_finished());
        drop(held_ab);
        tokio::time::timeout(std::time::Duration::from_secs(1), independent_a)
            .await
            .expect("the younger waiter proceeds after the older owner")
            .unwrap();
    }

    #[tokio::test]
    async fn disjoint_waiter_bypasses_an_older_blocked_waiter() {
        let scheduler = MutationConflictScheduler::default();
        let held_a = scheduler.acquire([resource("a")]).await;
        let waiting_a = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.acquire([resource("a")]).await }
        });
        while scheduler.waiting() != 1 {
            tokio::task::yield_now().await;
        }

        let independent_b = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            scheduler.acquire([resource("b")]),
        )
        .await
        .expect("an independent resource does not join the conflict convoy");
        assert!(!waiting_a.is_finished());
        drop(independent_b);
        drop(held_a);
        waiting_a.await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_a_queued_waiter_releases_its_fifo_position() {
        let scheduler = MutationConflictScheduler::default();
        let held = scheduler.acquire([resource("a")]).await;
        let cancelled = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.acquire([resource("a"), resource("b")]).await }
        });
        while scheduler.waiting() != 1 {
            tokio::task::yield_now().await;
        }
        let waiting_b = tokio::spawn({
            let scheduler = scheduler.clone();
            async move { scheduler.acquire([resource("b")]).await }
        });
        while scheduler.waiting() != 2 {
            tokio::task::yield_now().await;
        }

        cancelled.abort();
        let _ = cancelled.await;
        let acquired_b = tokio::time::timeout(std::time::Duration::from_secs(1), waiting_b)
            .await
            .expect("cancelling the older waiter unblocks its resource successors")
            .unwrap();
        drop(acquired_b);
        drop(held);
        assert_eq!(scheduler.waiting(), 0);
    }

    #[tokio::test]
    async fn cancelling_after_notification_cannot_leak_a_grant() {
        let scheduler = MutationConflictScheduler::default();
        let (ticket, ready) = scheduler.enqueue([resource("a")].into_iter().collect());
        let pending = PendingAcquisition {
            scheduler: scheduler.clone(),
            ticket,
            armed: true,
        };
        ready.await.unwrap();
        drop(pending);

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            scheduler.acquire([resource("a")]),
        )
        .await
        .expect("a notified but unclaimed grant is released on cancellation");
    }
}
