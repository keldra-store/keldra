use std::fs::ReadDir;
use std::path::PathBuf;
use std::time::Duration;

/// Hard work limits for one ordinary blob garbage-collection tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobGcBudget {
    pub max_records: u32,
    pub max_bytes: u64,
    pub max_duration: Duration,
}

impl BlobGcBudget {
    pub fn new(max_records: u32, max_bytes: u64, max_duration: Duration) -> Option<Self> {
        (max_records != 0 && max_bytes != 0 && !max_duration.is_zero()).then_some(Self {
            max_records,
            max_bytes,
            max_duration,
        })
    }
}

/// Caller-owned progress for incremental local lifecycle maintenance.
///
/// The authoritative due order is durable. This cursor is deliberately
/// process-local: losing it merely restarts a prefix seek and bounded scans of
/// `.staging` and `.gc`; canonical content directories are never inventoried.
#[derive(Default)]
pub struct BlobGcCursor {
    pub(crate) phase: BlobGcPhase,
}

impl std::fmt::Debug for BlobGcCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlobGcCursor")
            .field("phase", &self.phase.name())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum BlobGcPhaseName {
    #[default]
    Due,
    Filesystem,
}

#[derive(Default)]
pub(crate) enum BlobGcPhase {
    #[default]
    Due,
    DueAfter(Vec<u8>),
    Filesystem(FilesystemGcCursor),
}

impl BlobGcPhase {
    fn name(&self) -> BlobGcPhaseName {
        match self {
            Self::Due | Self::DueAfter(_) => BlobGcPhaseName::Due,
            Self::Filesystem(_) => BlobGcPhaseName::Filesystem,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum FilesystemGcDirectory {
    #[default]
    Staging,
    Quarantine,
    Complete,
}

#[derive(Default)]
pub(crate) struct FilesystemGcCursor {
    pub(crate) directory: FilesystemGcDirectory,
    pub(crate) entries: Option<ReadDir>,
    pub(crate) replay: Option<FilesystemGcRecord>,
}

pub(crate) enum FilesystemGcRecord {
    Staged {
        path: PathBuf,
        modified_at: u64,
        encoded_bytes: u64,
    },
    Quarantined {
        path: PathBuf,
        encoded_bytes: u64,
    },
}

/// Work completed by one bounded collector tick.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobGcTick {
    pub removed: u64,
    pub inspected_records: u32,
    pub inspected_bytes: u64,
    pub cycle_complete: bool,
}
