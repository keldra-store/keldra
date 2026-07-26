use async_trait::async_trait;
use thiserror::Error;

use crate::{CertificationResult, CertifyTransaction, CommitVersion};

/// Anvil-owned boundary hiding OpenRaft from product code.
#[async_trait]
pub trait Consensus: Send + Sync {
    async fn certify(
        &self,
        command: CertifyTransaction,
    ) -> Result<CertificationResult, ConsensusError>;

    async fn linearized_read_barrier(&self) -> Result<CommitVersion, ConsensusError>;

    fn observed_commit_version(&self) -> CommitVersion;
}

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("this node is not the current consensus leader")]
    ForwardToLeader,
    #[error("consensus is temporarily unavailable: {0}")]
    Unavailable(String),
    #[error("consensus storage failed: {0}")]
    Storage(String),
    #[error("certification was rejected: {0}")]
    Rejected(String),
}

// Compile-time pin and ownership boundary. Concrete OpenRaft types must remain
// confined to this module and the storage implementation.
pub(crate) const OPENRAFT_SERIES: &str = "0.9";

#[allow(dead_code)]
fn openraft_version_boundary() {
    let _: Option<openraft::Config> = None;
    debug_assert_eq!(OPENRAFT_SERIES, "0.9");
}
