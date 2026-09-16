//! Opaque memory admission retained for the complete query-block operation.

use std::sync::{Arc, Mutex, MutexGuard};

use crate::IndexError;

use super::IndexingMemoryPermit;

pub struct QueryBlockCredits {
    ledger: Arc<Mutex<QueryCreditLedger>>,
}

struct QueryCreditLedger {
    admitted: usize,
    remaining: usize,
    loaded_blocks: usize,
    required_query_lease_bytes: Option<usize>,
    _permit: QueryCreditPermit,
}

/// An exact byte reservation which is returned to its originating ledger when
/// it leaves scope. The reservation retains the root query permit even if the
/// originating credit handle is dropped first.
#[must_use = "dropping the reservation immediately returns its admitted bytes"]
pub(crate) struct QueryCreditReservation {
    ledger: Arc<Mutex<QueryCreditLedger>>,
    bytes: usize,
    loaded_block: bool,
    active: bool,
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
        let ledger = lock_ledger(&self.ledger);
        formatter
            .debug_struct("QueryBlockCredits")
            .field("admitted", &ledger.admitted)
            .field("remaining", &ledger.remaining)
            .field("loaded_blocks", &ledger.loaded_blocks)
            .field(
                "required_query_lease_bytes",
                &ledger.required_query_lease_bytes,
            )
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for QueryCreditReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryCreditReservation")
            .field("bytes", &self.bytes)
            .field("loaded_block", &self.loaded_block)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

fn lock_ledger(ledger: &Mutex<QueryCreditLedger>) -> MutexGuard<'_, QueryCreditLedger> {
    // No user code, allocation, or await runs while this lock is held. Recover
    // the state after a panic so a poisoned mutex cannot strand the root permit.
    ledger
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl QueryBlockCredits {
    /// Publish-phase growth may use the writer's explicitly promised sealing
    /// headroom, while already-retained prepared bytes keep exact ownership.
    pub fn enter_sealing(&mut self) -> Result<(), IndexError> {
        let mut ledger = lock_ledger(&self.ledger);
        match &mut ledger._permit {
            QueryCreditPermit::Pipeline { permit, .. } => match permit.enter_sealing()? {
                super::MemoryAdmission::Admitted => Ok(()),
                super::MemoryAdmission::ReplayRequired {
                    needed_bytes,
                    available_bytes,
                } => Err(IndexError::ResourceLimit {
                    needed: needed_bytes,
                    limit: available_bytes,
                }),
            },
            QueryCreditPermit::Query { .. } => Err(IndexError::InvalidDefinition(
                "query reader lease cannot enter producer sealing".into(),
            )),
        }
    }

    pub fn from_pipeline_permit(permit: IndexingMemoryPermit) -> Self {
        let admitted = permit.bytes();
        Self {
            ledger: Arc::new(Mutex::new(QueryCreditLedger {
                admitted,
                remaining: admitted,
                loaded_blocks: 0,
                required_query_lease_bytes: None,
                _permit: QueryCreditPermit::Pipeline {
                    permit,
                    maximum: admitted,
                    growable: false,
                },
            })),
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
            ledger: Arc::new(Mutex::new(QueryCreditLedger {
                admitted,
                remaining: admitted,
                loaded_blocks: 0,
                required_query_lease_bytes: None,
                _permit: QueryCreditPermit::Pipeline {
                    permit,
                    maximum,
                    growable: true,
                },
            })),
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
            ledger: Arc::new(Mutex::new(QueryCreditLedger {
                admitted: remaining,
                remaining,
                loaded_blocks: 0,
                required_query_lease_bytes: None,
                _permit: QueryCreditPermit::Query { _permit: permit },
            })),
        })
    }

    /// Fork an independently mutable handle to the same exact query lease.
    /// Pipeline credits remain exclusive because their growable permit is tied
    /// to one ordered producer operation.
    pub fn try_fork_query(&self) -> Result<Self, IndexError> {
        if !matches!(
            &lock_ledger(&self.ledger)._permit,
            QueryCreditPermit::Query { .. }
        ) {
            return Err(IndexError::InvalidDefinition(
                "only query-bound credits can be forked".into(),
            ));
        }
        Ok(Self {
            ledger: Arc::clone(&self.ledger),
        })
    }

    pub fn remaining(&self) -> usize {
        lock_ledger(&self.ledger).remaining
    }

    /// Exact retained permit charge, including already-consumed builder input.
    pub fn admitted_bytes(&self) -> usize {
        lock_ledger(&self.ledger).admitted
    }

    /// Total query lease needed by the reservation which exhausted these
    /// credits. Logical execution limits do not populate this retry signal.
    #[doc(hidden)]
    pub fn required_query_lease_bytes(&self) -> Option<usize> {
        lock_ledger(&self.ledger).required_query_lease_bytes
    }

    pub fn reserve(&mut self, bytes: usize) -> Result<(), IndexError> {
        reserve(&mut lock_ledger(&self.ledger), bytes)
    }

    pub(crate) fn reserve_scoped(
        &mut self,
        bytes: usize,
    ) -> Result<QueryCreditReservation, IndexError> {
        self.reserve(bytes)?;
        Ok(QueryCreditReservation {
            ledger: Arc::clone(&self.ledger),
            bytes,
            loaded_block: false,
            active: true,
        })
    }

    /// Return a general reservation after the associated bytes are no longer
    /// resident. Loaded-block lane accounting is handled separately.
    pub fn release(&mut self, bytes: usize) -> Result<(), IndexError> {
        release(&mut lock_ledger(&self.ledger), bytes, false)
    }

    pub fn reserve_loaded_block(
        &mut self,
        bytes: usize,
        maximum_loaded_blocks: usize,
    ) -> Result<(), IndexError> {
        let mut ledger = lock_ledger(&self.ledger);
        if ledger.loaded_blocks >= maximum_loaded_blocks {
            return Err(IndexError::ResourceLimit {
                needed: ledger.loaded_blocks.saturating_add(1),
                limit: maximum_loaded_blocks,
            });
        }
        reserve(&mut ledger, bytes)?;
        ledger.loaded_blocks = ledger.loaded_blocks.saturating_add(1);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn reserve_loaded_block_scoped(
        &mut self,
        bytes: usize,
        maximum_loaded_blocks: usize,
    ) -> Result<QueryCreditReservation, IndexError> {
        self.reserve_loaded_block(bytes, maximum_loaded_blocks)?;
        Ok(QueryCreditReservation {
            ledger: Arc::clone(&self.ledger),
            bytes,
            loaded_block: true,
            active: true,
        })
    }

    pub fn release_loaded_block(&mut self, bytes: usize) -> Result<(), IndexError> {
        release(&mut lock_ledger(&self.ledger), bytes, true)
    }
}

fn reserve(ledger: &mut QueryCreditLedger, bytes: usize) -> Result<(), IndexError> {
    if bytes > ledger.remaining {
        let additional = bytes - ledger.remaining;
        let admitted = ledger.admitted;
        let remaining = ledger.remaining;
        let required = admitted
            .checked_sub(remaining)
            .ok_or(IndexError::Integrity)?
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if matches!(&ledger._permit, QueryCreditPermit::Query { .. }) {
            ledger.required_query_lease_bytes = Some(
                ledger
                    .required_query_lease_bytes
                    .map_or(required, |recorded| recorded.max(required)),
            );
            return Err(IndexError::ResourceLimit {
                needed: required,
                limit: admitted,
            });
        }
        let QueryCreditPermit::Pipeline {
            permit,
            maximum,
            growable,
        } = &mut ledger._permit
        else {
            unreachable!("query permits returned above")
        };
        if !*growable {
            return Err(IndexError::ResourceLimit {
                needed: bytes,
                limit: remaining,
            });
        }
        let next = admitted
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
                limit: admitted.saturating_add(available_bytes),
            },
            super::MemoryAdmission::Admitted => unreachable!(),
        })?;
        ledger.admitted = next;
        ledger.remaining = remaining
            .checked_add(additional)
            .ok_or(IndexError::OffsetOverflow)?;
    }
    ledger.remaining -= bytes;
    Ok(())
}

fn release(
    ledger: &mut QueryCreditLedger,
    bytes: usize,
    loaded_block: bool,
) -> Result<(), IndexError> {
    if loaded_block && ledger.loaded_blocks == 0 {
        return Err(IndexError::Integrity);
    }
    if ledger
        .remaining
        .checked_add(bytes)
        .is_none_or(|remaining| remaining > ledger.admitted)
    {
        return Err(IndexError::Integrity);
    }
    ledger.remaining += bytes;
    if loaded_block {
        ledger.loaded_blocks -= 1;
    }
    Ok(())
}

impl QueryCreditReservation {
    pub(crate) fn release_in_place(&mut self) -> Result<(), IndexError> {
        let outcome = release(
            &mut lock_ledger(&self.ledger),
            self.bytes,
            self.loaded_block,
        );
        if outcome.is_ok() {
            self.active = false;
        }
        outcome
    }
}

impl Drop for QueryCreditReservation {
    fn drop(&mut self) {
        if self.active {
            let outcome = release(
                &mut lock_ledger(&self.ledger),
                self.bytes,
                self.loaded_block,
            );
            debug_assert!(outcome.is_ok(), "scoped query-credit release must balance");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier, mpsc};

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

    #[test]
    fn forked_query_handles_share_bytes_and_retry_high_water() {
        let mut credits = QueryBlockCredits::from_query_permit(Box::new(QueryPermit(16))).unwrap();
        let mut fork = credits.try_fork_query().unwrap();

        credits.reserve(10).unwrap();
        assert_eq!(fork.remaining(), 6);
        assert_eq!(
            fork.reserve(8),
            Err(IndexError::ResourceLimit {
                needed: 18,
                limit: 16,
            })
        );
        assert_eq!(credits.required_query_lease_bytes(), Some(18));
        credits.release(10).unwrap();
        fork.reserve(12).unwrap();
        assert_eq!(
            credits.reserve(8),
            Err(IndexError::ResourceLimit {
                needed: 20,
                limit: 16,
            })
        );
        assert_eq!(fork.required_query_lease_bytes(), Some(20));
    }

    #[test]
    fn loaded_block_limit_is_global_across_query_forks() {
        let mut credits = QueryBlockCredits::from_query_permit(Box::new(QueryPermit(16))).unwrap();
        let mut fork = credits.try_fork_query().unwrap();

        credits.reserve_loaded_block(4, 1).unwrap();
        assert_eq!(
            fork.reserve_loaded_block(4, 1),
            Err(IndexError::ResourceLimit {
                needed: 2,
                limit: 1,
            })
        );
        assert_eq!(credits.remaining(), 12);
        fork.release_loaded_block(4).unwrap();
        assert_eq!(credits.remaining(), 16);
    }

    #[test]
    fn scoped_reservations_release_exact_bytes_and_block_lane() {
        let mut credits = QueryBlockCredits::from_query_permit(Box::new(QueryPermit(16))).unwrap();
        {
            let _bytes = credits.reserve_scoped(6).unwrap();
            assert_eq!(credits.remaining(), 10);
        }
        assert_eq!(credits.remaining(), 16);
        {
            let _block = credits.reserve_loaded_block_scoped(7, 1).unwrap();
            assert_eq!(credits.remaining(), 9);
        }
        assert_eq!(credits.remaining(), 16);
        credits.reserve_loaded_block(1, 1).unwrap();
    }

    #[test]
    fn pipeline_credits_cannot_be_forked() {
        let memory = memory(16);
        let permit = memory
            .acquire(IndexingMemoryStage::OrderingCatalog, 16)
            .unwrap();
        let credits = QueryBlockCredits::from_pipeline_permit(permit);

        assert!(matches!(
            credits.try_fork_query(),
            Err(IndexError::InvalidDefinition(_))
        ));
    }

    #[test]
    fn concurrent_forks_cannot_over_admit_the_query_lease() {
        let credits = QueryBlockCredits::from_query_permit(Box::new(QueryPermit(16))).unwrap();
        let start = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let (sender, receiver) = mpsc::channel();
        let mut workers = Vec::new();
        for _ in 0..2 {
            let mut fork = credits.try_fork_query().unwrap();
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            let sender = sender.clone();
            workers.push(std::thread::spawn(move || {
                start.wait();
                let reservation = fork.reserve_scoped(10).ok();
                sender.send(reservation.is_some()).unwrap();
                release.wait();
                drop(reservation);
            }));
        }
        drop(sender);

        start.wait();
        let outcomes = [receiver.recv().unwrap(), receiver.recv().unwrap()];
        assert_eq!(outcomes.into_iter().filter(|admitted| *admitted).count(), 1);
        assert_eq!(credits.remaining(), 6);
        assert_eq!(credits.required_query_lease_bytes(), Some(20));
        release.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(credits.remaining(), 16);
    }
}
