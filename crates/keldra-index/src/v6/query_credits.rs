//! Opaque memory admission retained for the complete query-block operation.

use crate::IndexError;

use super::IndexingMemoryPermit;

pub struct QueryBlockCredits {
    admitted: usize,
    remaining: usize,
    loaded_blocks: usize,
    _permit: QueryCreditPermit,
}

pub trait QueryMemoryPermit: Send {
    fn admitted_bytes(&self) -> usize;
}

enum QueryCreditPermit {
    Pipeline { _permit: IndexingMemoryPermit },
    Query { _permit: Box<dyn QueryMemoryPermit> },
}

impl std::fmt::Debug for QueryBlockCredits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryBlockCredits")
            .field("admitted", &self.admitted)
            .field("remaining", &self.remaining)
            .field("loaded_blocks", &self.loaded_blocks)
            .finish_non_exhaustive()
    }
}

impl QueryBlockCredits {
    pub fn from_pipeline_permit(permit: IndexingMemoryPermit) -> Self {
        Self {
            admitted: permit.bytes(),
            remaining: permit.bytes(),
            loaded_blocks: 0,
            _permit: QueryCreditPermit::Pipeline { _permit: permit },
        }
    }

    pub fn from_query_permit(permit: Box<dyn QueryMemoryPermit>) -> Result<Self, IndexError> {
        let remaining = permit.admitted_bytes();
        if remaining == 0 {
            return Err(IndexError::InvalidDefinition(
                "v6 query memory admission is empty".into(),
            ));
        }
        Ok(Self {
            admitted: remaining,
            remaining,
            loaded_blocks: 0,
            _permit: QueryCreditPermit::Query { _permit: permit },
        })
    }

    pub const fn remaining(&self) -> usize {
        self.remaining
    }

    pub fn reserve(&mut self, bytes: usize) -> Result<(), IndexError> {
        if bytes > self.remaining {
            return Err(IndexError::ResourceLimit {
                needed: bytes,
                limit: self.remaining,
            });
        }
        self.remaining -= bytes;
        Ok(())
    }

    /// Return a general reservation after the associated bytes are no longer
    /// resident. Loaded-block lane accounting is handled separately.
    pub fn release(&mut self, bytes: usize) -> Result<(), IndexError> {
        if self
            .remaining
            .checked_add(bytes)
            .is_none_or(|remaining| remaining > self.admitted)
        {
            return Err(IndexError::Integrity);
        }
        self.remaining += bytes;
        Ok(())
    }

    pub fn reserve_loaded_block(
        &mut self,
        bytes: usize,
        maximum_loaded_blocks: usize,
    ) -> Result<(), IndexError> {
        if self.loaded_blocks >= maximum_loaded_blocks {
            return Err(IndexError::ResourceLimit {
                needed: self.loaded_blocks.saturating_add(1),
                limit: maximum_loaded_blocks,
            });
        }
        self.reserve(bytes)
            .map(|()| self.loaded_blocks = self.loaded_blocks.saturating_add(1))
    }

    pub fn release_loaded_block(&mut self, bytes: usize) -> Result<(), IndexError> {
        if self.loaded_blocks == 0
            || self
                .remaining
                .checked_add(bytes)
                .is_none_or(|remaining| remaining > self.admitted)
        {
            return Err(IndexError::Integrity);
        }
        self.remaining += bytes;
        self.loaded_blocks -= 1;
        Ok(())
    }
}
