//! Opaque memory admission retained for the complete query-block operation.

use crate::IndexError;

use super::IndexingMemoryPermit;

pub struct QueryBlockCredits {
    admitted: usize,
    remaining: usize,
    loaded_blocks: usize,
    required_query_lease_bytes: Option<usize>,
    _permit: QueryCreditPermit,
}

pub trait QueryMemoryPermit: Send {
    fn admitted_bytes(&self) -> usize;
}

enum QueryCreditPermit {
    Pipeline {
        permit: IndexingMemoryPermit,
        maximum: usize,
        growable: bool,
    },
    Query {
        _permit: Box<dyn QueryMemoryPermit>,
    },
}

impl std::fmt::Debug for QueryBlockCredits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryBlockCredits")
            .field("admitted", &self.admitted)
            .field("remaining", &self.remaining)
            .field("loaded_blocks", &self.loaded_blocks)
            .field(
                "required_query_lease_bytes",
                &self.required_query_lease_bytes,
            )
            .finish_non_exhaustive()
    }
}

impl QueryBlockCredits {
    pub fn from_pipeline_permit(permit: IndexingMemoryPermit) -> Self {
        let admitted = permit.bytes();
        Self {
            admitted,
            remaining: admitted,
            loaded_blocks: 0,
            required_query_lease_bytes: None,
            _permit: QueryCreditPermit::Pipeline {
                permit,
                maximum: admitted,
                growable: false,
            },
        }
    }

    /// Start with a minimal pipeline reservation and grow it only as query
    /// records become resident. The shared pipeline and stage limits remain
    /// authoritative for every increment.
    #[doc(hidden)]
    pub fn from_growable_pipeline_permit(
        permit: IndexingMemoryPermit,
        maximum: usize,
    ) -> Result<Self, IndexError> {
        let admitted = permit.bytes();
        if maximum < admitted {
            return Err(IndexError::InvalidDefinition(
                "v1 query memory maximum is below its initial admission".into(),
            ));
        }
        Ok(Self {
            admitted,
            remaining: admitted,
            loaded_blocks: 0,
            required_query_lease_bytes: None,
            _permit: QueryCreditPermit::Pipeline {
                permit,
                maximum,
                growable: true,
            },
        })
    }

    pub fn from_query_permit(permit: Box<dyn QueryMemoryPermit>) -> Result<Self, IndexError> {
        let remaining = permit.admitted_bytes();
        if remaining == 0 {
            return Err(IndexError::InvalidDefinition(
                "v1 query memory admission is empty".into(),
            ));
        }
        Ok(Self {
            admitted: remaining,
            remaining,
            loaded_blocks: 0,
            required_query_lease_bytes: None,
            _permit: QueryCreditPermit::Query { _permit: permit },
        })
    }

    pub const fn remaining(&self) -> usize {
        self.remaining
    }

    /// Total query lease needed by the reservation which exhausted these
    /// credits. Logical execution limits do not populate this retry signal.
    #[doc(hidden)]
    pub const fn required_query_lease_bytes(&self) -> Option<usize> {
        self.required_query_lease_bytes
    }

    pub fn reserve(&mut self, bytes: usize) -> Result<(), IndexError> {
        if bytes > self.remaining {
            let additional = bytes - self.remaining;
            let required = self
                .admitted
                .checked_sub(self.remaining)
                .ok_or(IndexError::Integrity)?
                .checked_add(bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            match &mut self._permit {
                QueryCreditPermit::Pipeline {
                    permit,
                    maximum,
                    growable: true,
                } => {
                    let next = self
                        .admitted
                        .checked_add(additional)
                        .ok_or(IndexError::OffsetOverflow)?;
                    if next > *maximum {
                        return Err(IndexError::ResourceLimit {
                            needed: next,
                            limit: *maximum,
                        });
                    }
                    permit.grow_to(next).map_err(|admission| match admission {
                        super::MemoryAdmission::ReplayRequired {
                            available_bytes, ..
                        } => IndexError::ResourceLimit {
                            needed: next,
                            limit: self.admitted.saturating_add(available_bytes),
                        },
                        super::MemoryAdmission::Admitted => unreachable!(),
                    })?;
                    self.admitted = next;
                    self.remaining = self
                        .remaining
                        .checked_add(additional)
                        .ok_or(IndexError::OffsetOverflow)?;
                }
                QueryCreditPermit::Query { .. } => {
                    self.required_query_lease_bytes = Some(
                        self.required_query_lease_bytes
                            .map_or(required, |recorded| recorded.max(required)),
                    );
                    return Err(IndexError::ResourceLimit {
                        needed: required,
                        limit: self.admitted,
                    });
                }
                QueryCreditPermit::Pipeline { .. } => {
                    return Err(IndexError::ResourceLimit {
                        needed: bytes,
                        limit: self.remaining,
                    });
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::{IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage};

    struct QueryPermit(usize);

    impl QueryMemoryPermit for QueryPermit {
        fn admitted_bytes(&self) -> usize {
            self.0
        }
    }

    fn memory(bytes: usize) -> IndexingMemoryCredits {
        IndexingMemoryCredits::new(
            bytes,
            IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap()
    }

    #[test]
    fn growable_pipeline_credits_charge_only_resident_query_bytes() {
        let memory = memory(256);
        let permit = memory
            .acquire(IndexingMemoryStage::OrderingCatalog, 1)
            .unwrap();
        let mut credits = QueryBlockCredits::from_growable_pipeline_permit(permit, 128).unwrap();

        assert_eq!(memory.used_bytes(), 1);
        credits.reserve(96).unwrap();
        assert_eq!(memory.used_bytes(), 96);
        assert_eq!(credits.remaining(), 0);
        credits.release(32).unwrap();
        assert_eq!(memory.used_bytes(), 96);
        assert_eq!(credits.remaining(), 32);
        drop(credits);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn growable_pipeline_credits_fail_without_losing_existing_admission() {
        let memory = memory(128);
        let _other = memory
            .acquire(IndexingMemoryStage::OrderingCatalog, 64)
            .unwrap();
        let permit = memory
            .acquire(IndexingMemoryStage::OrderingCatalog, 1)
            .unwrap();
        let mut credits = QueryBlockCredits::from_growable_pipeline_permit(permit, 128).unwrap();

        assert!(matches!(
            credits.reserve(65),
            Err(IndexError::ResourceLimit { .. })
        ));
        assert_eq!(memory.used_bytes(), 65);
        assert_eq!(credits.remaining(), 1);
    }

    #[test]
    fn query_credit_exhaustion_records_the_total_required_lease() {
        let mut credits = QueryBlockCredits::from_query_permit(Box::new(QueryPermit(16))).unwrap();
        credits.reserve(10).unwrap();

        assert_eq!(
            credits.reserve(8),
            Err(IndexError::ResourceLimit {
                needed: 18,
                limit: 16,
            })
        );
        assert_eq!(credits.required_query_lease_bytes(), Some(18));
        assert_eq!(credits.remaining(), 6);
    }

    #[test]
    fn loaded_block_limit_does_not_report_query_credit_exhaustion() {
        let mut credits = QueryBlockCredits::from_query_permit(Box::new(QueryPermit(16))).unwrap();

        assert!(matches!(
            credits.reserve_loaded_block(1, 0),
            Err(IndexError::ResourceLimit { .. })
        ));
        assert_eq!(credits.required_query_lease_bytes(), None);
        assert_eq!(credits.remaining(), 16);
    }
}
