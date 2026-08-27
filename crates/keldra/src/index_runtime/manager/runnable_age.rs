//! Runnable-time clock for age-bounded active mutation buffers.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
pub(super) struct BuilderRunnableClock {
    state: Arc<Mutex<RunnableClockState>>,
}

#[derive(Default)]
struct RunnableClockState {
    elapsed: Duration,
    active: usize,
    active_since: Option<Instant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BufferAge {
    wall_started: Instant,
    runnable_started: Duration,
}

impl BuilderRunnableClock {
    pub(super) fn stamp(&self) -> BufferAge {
        BufferAge {
            wall_started: Instant::now(),
            runnable_started: self.elapsed(),
        }
    }

    pub(super) fn elapsed(&self) -> Duration {
        let state = self.lock();
        state.elapsed.saturating_add(
            state
                .active_since
                .map_or(Duration::ZERO, |started| started.elapsed()),
        )
    }

    pub(super) fn add(&self, elapsed: Duration) {
        let mut state = self.lock();
        state.elapsed = state.elapsed.saturating_add(elapsed);
    }

    pub(super) fn measure<T>(&self, work: impl FnOnce() -> T) -> T {
        let _guard = self.enter();
        work()
    }

    fn enter(&self) -> RunnableGuard {
        let mut state = self.lock();
        if state.active == 0 {
            state.active_since = Some(Instant::now());
        }
        state.active = state.active.saturating_add(1);
        RunnableGuard {
            clock: self.clone(),
        }
    }

    fn leave(&self) {
        let mut state = self.lock();
        debug_assert!(state.active > 0);
        state.active = state.active.saturating_sub(1);
        if state.active == 0
            && let Some(started) = state.active_since.take()
        {
            state.elapsed = state.elapsed.saturating_add(started.elapsed());
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RunnableClockState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct RunnableGuard {
    clock: BuilderRunnableClock,
}

impl Drop for RunnableGuard {
    fn drop(&mut self) {
        self.clock.leave();
    }
}

impl BufferAge {
    pub(super) fn earliest(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        match (left, right) {
            (Some(left), Some(right)) => Some(if left.runnable_started <= right.runnable_started {
                left
            } else {
                right
            }),
            (Some(age), None) | (None, Some(age)) => Some(age),
            (None, None) => None,
        }
    }

    pub(super) fn elapsed(self, clock: &BuilderRunnableClock) -> Duration {
        clock.elapsed().saturating_sub(self.runnable_started)
    }

    pub(super) fn wall_elapsed(self) -> Duration {
        self.wall_started.elapsed()
    }

    pub(super) fn reached(self, clock: &BuilderRunnableClock, maximum: Duration) -> bool {
        self.elapsed(clock) >= maximum
    }

    pub(super) fn remaining(
        self,
        clock: &BuilderRunnableClock,
        maximum: Duration,
    ) -> Option<Duration> {
        maximum
            .checked_sub(self.elapsed(clock))
            .filter(|age| !age.is_zero())
    }
}

pub(super) async fn measure_runnable<F: Future>(
    clock: BuilderRunnableClock,
    future: F,
) -> F::Output {
    tokio::pin!(future);
    std::future::poll_fn(|context| clock.measure(|| future.as_mut().poll(context))).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_or_resource_wait_does_not_manufacture_an_age_flush() {
        let clock = BuilderRunnableClock::default();
        let age = BufferAge {
            wall_started: Instant::now().checked_sub(Duration::from_secs(30)).unwrap(),
            runnable_started: clock.elapsed(),
        };

        // Wall time is deliberately irrelevant while the definition is not
        // runnable. Advancing no runnable time cannot cross the age boundary.
        assert!(!age.reached(&clock, Duration::from_secs(1)));
        assert_eq!(age.elapsed(&clock), Duration::ZERO);
        assert!(age.wall_elapsed() >= Duration::from_secs(30));
    }

    #[test]
    fn one_second_of_runnable_work_triggers_the_age_flush_boundary() {
        let clock = BuilderRunnableClock::default();
        let age = clock.stamp();
        clock.add(Duration::from_millis(999));
        assert!(!age.reached(&clock, Duration::from_secs(1)));
        clock.add(Duration::from_millis(1));
        assert!(age.reached(&clock, Duration::from_secs(1)));
    }
}
