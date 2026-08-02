use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};

use crate::VersionId;

const NODE_BITS: u32 = 10;
const SEQUENCE_BITS: u32 = 12;
const MAX_NODE_ID: u16 = (1 << NODE_BITS) - 1;
const MAX_SEQUENCE: u16 = (1 << SEQUENCE_BITS) - 1;
const CUSTOM_EPOCH_MILLIS: u64 = 1_767_225_600_000; // 2026-01-01T00:00:00Z

#[derive(Debug, Default)]
struct ClockState {
    millisecond: u64,
    sequence: u16,
    high_watermark: u64,
}

/// Generates compact, time-sortable IDs without a distributed allocator.
#[derive(Debug)]
pub struct VersionClock {
    node_id: u16,
    state: Mutex<ClockState>,
}

impl VersionClock {
    pub fn new(node_id: u16) -> Result<Self> {
        Self::with_high_watermark(node_id, None)
    }

    pub fn with_high_watermark(node_id: u16, high_watermark: Option<VersionId>) -> Result<Self> {
        if node_id > MAX_NODE_ID {
            bail!("node id must fit in {NODE_BITS} bits");
        }
        Ok(Self {
            node_id,
            state: Mutex::new(ClockState {
                high_watermark: high_watermark.map_or(0, |version| version.0),
                ..Default::default()
            }),
        })
    }

    pub fn next(&self) -> Result<VersionId> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        self.next_at(now)
    }

    /// Fences future local allocations above a version learned from another
    /// executor. Persisting the high watermark is not enough while this
    /// process remains alive: the in-memory clock must learn it as well.
    pub(crate) fn observe(&self, version: VersionId) {
        let mut state = self.state.lock().expect("version clock poisoned");
        state.high_watermark = state.high_watermark.max(version.0);
    }

    fn next_at(&self, unix_millis: u64) -> Result<VersionId> {
        if unix_millis < CUSTOM_EPOCH_MILLIS {
            bail!("system clock predates the Anvil version epoch");
        }
        let mut state = self.state.lock().expect("version clock poisoned");
        let mut logical = unix_millis.saturating_sub(CUSTOM_EPOCH_MILLIS);
        logical = logical.max(state.millisecond);
        if logical == state.millisecond {
            if state.sequence == MAX_SEQUENCE {
                logical = logical
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("version timestamp overflow"))?;
                state.sequence = 0;
            } else {
                state.sequence += 1;
            }
        } else {
            state.sequence = 0;
        }
        state.millisecond = logical;
        let mut value = (logical << (NODE_BITS + SEQUENCE_BITS))
            | (u64::from(self.node_id) << SEQUENCE_BITS)
            | u64::from(state.sequence);
        if value <= state.high_watermark {
            logical = (state.high_watermark >> (NODE_BITS + SEQUENCE_BITS))
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("version timestamp overflow"))?;
            state.millisecond = logical;
            state.sequence = 0;
            value = (logical << (NODE_BITS + SEQUENCE_BITS))
                | (u64::from(self.node_id) << SEQUENCE_BITS);
        }
        state.high_watermark = value;
        Ok(VersionId(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_monotonic_when_the_clock_stalls_or_moves_backwards() {
        let clock = VersionClock::new(7).unwrap();
        let first = clock.next_at(CUSTOM_EPOCH_MILLIS + 100).unwrap();
        let second = clock.next_at(CUSTOM_EPOCH_MILLIS + 100).unwrap();
        let third = clock.next_at(CUSTOM_EPOCH_MILLIS + 99).unwrap();
        assert!(first < second && second < third);
    }

    #[test]
    fn persisted_high_watermark_fences_a_restarted_clock() {
        let high = VersionId(((CUSTOM_EPOCH_MILLIS + 10_000) << (NODE_BITS + SEQUENCE_BITS)) | 9);
        let clock = VersionClock::with_high_watermark(1, Some(high)).unwrap();
        assert!(clock.next_at(CUSTOM_EPOCH_MILLIS + 1).unwrap() > high);
    }

    #[test]
    fn observed_remote_version_fences_the_live_clock() {
        let clock = VersionClock::new(1).unwrap();
        let remote = VersionId(((CUSTOM_EPOCH_MILLIS + 10_000) << (NODE_BITS + SEQUENCE_BITS)) | 9);
        clock.observe(remote);
        assert!(clock.next_at(CUSTOM_EPOCH_MILLIS + 1).unwrap() > remote);
    }
}
