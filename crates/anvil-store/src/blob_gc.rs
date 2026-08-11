use std::fs::ReadDir;
use std::path::PathBuf;
use std::time::Duration;

use crate::{BlobRef, ShardIdentity};

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

/// Caller-owned progress for the incremental local blob collector.
///
/// This cursor is deliberately process-local. Losing it restarts a lazy scan
/// after the node is already serving; it never makes startup inventory the
/// filesystem or adds another durable source of truth.
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
    References,
    Filesystem,
}

#[derive(Default)]
pub(crate) enum BlobGcPhase {
    #[default]
    References,
    ReferencesAfter(Vec<u8>),
    Filesystem(FilesystemGcCursor),
}

impl BlobGcPhase {
    fn name(&self) -> BlobGcPhaseName {
        match self {
            Self::References | Self::ReferencesAfter(_) => BlobGcPhaseName::References,
            Self::Filesystem(_) => BlobGcPhaseName::Filesystem,
        }
    }
}

#[derive(Default)]
pub(crate) struct FilesystemGcCursor {
    pub(crate) root: Option<ReadDir>,
    pub(crate) child: Option<FilesystemGcChild>,
    pub(crate) replay: Option<FilesystemGcRecord>,
}

pub(crate) struct FilesystemGcChild {
    pub(crate) kind: FilesystemGcChildKind,
    pub(crate) entries: ReadDir,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FilesystemGcChildKind {
    Staging,
    Quarantine,
    HashPrefix(String),
}

pub(crate) enum FilesystemGcRecord {
    Directory {
        path: PathBuf,
        kind: FilesystemGcChildKind,
        encoded_bytes: u64,
    },
    Staged {
        path: PathBuf,
        modified_at: u64,
        encoded_bytes: u64,
    },
    Quarantined {
        path: PathBuf,
        encoded_bytes: u64,
    },
    Blob {
        path: PathBuf,
        reference: BlobRef,
        modified_at: u64,
        encoded_bytes: u64,
    },
    Shard {
        path: PathBuf,
        identity: ShardIdentity,
        modified_at: u64,
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
