//! Accounted lazy access to the existing immutable stream trees.
use super::*;
use keldra_index::v1::{IndexingMemoryCredits, IndexingMemoryStage};

#[derive(Clone)]
pub(super) struct CompactionTreeAccess {
    pub(super) publisher: V1ProjectionPublisher,
    pub(super) storage_tenant: String,
    pub(super) bucket: String,
    pub(super) tenant_id: u64,
    pub(super) bucket_id: u64,
    pub(super) partition: keldra_index::v1::ProjectionPartitionIdentity,
    pub(super) credits: IndexingMemoryCredits,
    pub(super) runtime: tokio::runtime::Handle,
    pub(super) output: Option<Arc<Mutex<IndexingMemoryPermit>>>,
}

struct AccountedTreePage {
    bytes: Vec<u8>,
    _permit: IndexingMemoryPermit,
}

struct AccountedSplicePage {
    bytes: Bytes,
    _owner: Arc<Mutex<IndexingMemoryPermit>>,
}
impl AsRef<[u8]> for AccountedSplicePage {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl AsRef<[u8]> for AccountedTreePage {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl CompactionTreeAccess {
    pub(super) fn with_output_reservation(mut self) -> Result<Self, Status> {
        let permit = self
            .credits
            .acquire(IndexingMemoryStage::WorkerScratch, 0)
            .map_err(|error| index_status(admission_error(error)))?;
        self.output = Some(Arc::new(Mutex::new(permit)));
        Ok(self)
    }

    pub(super) fn output_owner(&self) -> Arc<Mutex<IndexingMemoryPermit>> {
        self.output
            .as_ref()
            .expect("splice output reservation installed")
            .clone()
    }

    pub(super) fn retain_component_pages(&self, pages: &mut [EncodedComponentStreamPage]) {
        let owner = self.output_owner();
        for page in pages {
            page.bytes = Bytes::from_owner(AccountedSplicePage {
                bytes: std::mem::take(&mut page.bytes),
                _owner: owner.clone(),
            });
        }
    }

    pub(super) fn seal_output(
        &self,
        mut lengths: impl Iterator<Item = usize>,
    ) -> Result<(), Status> {
        let retained = lengths
            .try_fold(0usize, |total, length| {
                total.checked_add(length).and_then(|value| {
                    value.checked_add(std::mem::size_of::<EncodedComponentStreamPage>() + 128)
                })
            })
            .ok_or_else(|| Status::resource_exhausted("compaction page output size overflow"))?;
        let owner = self.output_owner();
        let mut owner = owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if retained > owner.bytes() {
            return Err(Status::data_loss(
                "compaction splice exceeded admitted page output",
            ));
        }
        owner.shrink_to(retained).map_err(index_status)
    }

    pub(super) fn page(
        &self,
        hash: [u8; 32],
        query: bool,
    ) -> Result<Bytes, keldra_index::IndexError> {
        // Admit the bounded wire read before allocating it. Then admit its
        // decoded representation before the selector/splicer decodes it.
        let mut permit = self
            .credits
            .acquire(IndexingMemoryStage::WorkerScratch, MAX_PAGE_BYTES)
            .map_err(admission_error)?;
        let path = if query {
            projection_query_run_stream_page_path(self.partition, hash)
        } else {
            projection_stream_page_path(self.partition, hash)
        };
        let bytes = self
            .runtime
            .block_on(read_artifact(
                &self.publisher,
                &self.storage_tenant,
                &self.bucket,
                self.tenant_id,
                self.bucket_id,
                path,
                hash,
                MAX_PAGE_BYTES,
            ))
            .map_err(tree_read_error)?;
        // Merge substitutes slots and never splits a tree page. Bound the
        // rewritten leaf's exact current-format reference width, or a branch's
        // one newly introduced target-level summary per child. Reserve before
        // decoding/encoding; Vec growth and right-sizing may coexist briefly.
        if let Some(output) = &self.output {
            let path_len = keldra_index::v1::projection_pack_path(self.partition, [0; 32]).len();
            let bound = page_output_bound(&bytes, query, path_len)?;
            let additional = bound
                .checked_mul(3)
                .and_then(|value| {
                    value.checked_add(std::mem::size_of::<EncodedComponentStreamPage>() + 128)
                })
                .ok_or(keldra_index::IndexError::OffsetOverflow)?;
            let mut owner = output
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let needed = owner
                .bytes()
                .checked_add(additional)
                .ok_or(keldra_index::IndexError::OffsetOverflow)?;
            owner.grow_to(needed).map_err(admission_error)?;
        }
        let decoded = page_decode_bound(&bytes, query)?;
        let charge = bytes
            .capacity()
            .checked_add(decoded)
            .ok_or(keldra_index::IndexError::OffsetOverflow)?;
        permit.grow_to(charge).map_err(admission_error)?;
        permit.shrink_to(charge)?;
        tracing::debug!(
            counter.keldra_index_v1_compaction_pages_read_total = 1_u64,
            histogram.keldra_index_v1_compaction_page_scratch_bytes = charge as f64,
            "keldra_index_v1_compaction_lazy_page"
        );
        Ok(Bytes::from_owner(AccountedTreePage {
            bytes,
            _permit: permit,
        }))
    }
}

fn tree_read_error(error: Status) -> keldra_index::IndexError {
    match error.code() {
        tonic::Code::ResourceExhausted => {
            keldra_index::IndexError::AdmissionDenied(error.to_string())
        }
        tonic::Code::DeadlineExceeded => keldra_index::IndexError::DeadlineExceeded,
        tonic::Code::DataLoss => keldra_index::IndexError::IntegrityViolation(error.to_string()),
        _ => keldra_index::IndexError::Io(error.to_string()),
    }
}

pub(super) fn admission_error(
    admission: keldra_index::v1::MemoryAdmission,
) -> keldra_index::IndexError {
    match admission {
        keldra_index::v1::MemoryAdmission::ReplayRequired {
            needed_bytes,
            available_bytes,
        } => keldra_index::IndexError::ResourceLimit {
            needed: needed_bytes,
            limit: available_bytes,
        },
        keldra_index::v1::MemoryAdmission::Admitted => keldra_index::IndexError::Integrity,
    }
}

/// Bounds decoded vectors from the actual encoded page header, not stream
/// history or a fraction of the configured node budget. Component references
/// carry at least 50 wire bytes; level entries carry nine. Paths are bounded
/// by the encoded page itself. The existing decoder remains the validator.
fn page_decode_bound(bytes: &[u8], query: bool) -> Result<usize, keldra_index::IndexError> {
    let count_offset = if query {
        11
    } else {
        match bytes.get(10).copied() {
            Some(1 | 6) => 12,
            Some(2 | 3 | 4) => 44,
            _ => return Err(keldra_index::IndexError::Integrity),
        }
    };
    let encoded_count = bytes
        .get(count_offset..count_offset + 2)
        .ok_or(keldra_index::IndexError::Integrity)?;
    let count = usize::from(u16::from_le_bytes(encoded_count.try_into().unwrap()));
    let item_bytes = if query {
        std::mem::size_of::<keldra_index::v1::QueryRunReference>()
            .max(std::mem::size_of::<keldra_index::v1::QueryRunChild>())
    } else {
        std::mem::size_of::<keldra_index::v1::ComponentSegmentDescriptor>()
    };
    let fixed = count
        .checked_mul(item_bytes)
        .ok_or(keldra_index::IndexError::OffsetOverflow)?;
    let references = if query {
        0
    } else {
        (bytes.len() / 50)
            .checked_mul(std::mem::size_of::<keldra_index::v1::ArtifactPackReference>())
            .and_then(|value| value.checked_add(bytes.len()))
            .and_then(|value| {
                value.checked_add((bytes.len() / 9) * std::mem::size_of::<(u8, u64)>())
            })
            .ok_or(keldra_index::IndexError::OffsetOverflow)?
    };
    // Splicing coexists with a replacement vector while rewriting one page.
    fixed
        .checked_mul(2)
        .and_then(|value| value.checked_add(references))
        .ok_or(keldra_index::IndexError::OffsetOverflow)
}

fn page_output_bound(
    bytes: &[u8],
    query: bool,
    path_len: usize,
) -> Result<usize, keldra_index::IndexError> {
    if query {
        return Ok(bytes.len());
    }
    let count_offset = match bytes.get(10).copied() {
        Some(1 | 6) => 12,
        Some(2 | 3 | 4) => 44,
        _ => return Err(keldra_index::IndexError::Integrity),
    };
    let count = bytes
        .get(count_offset..count_offset + 2)
        .ok_or(keldra_index::IndexError::Integrity)?;
    let count = usize::from(u16::from_le_bytes(count.try_into().unwrap()));
    match bytes.get(count_offset - 1).copied() {
        Some(1) => count
            .checked_mul(
                225_usize
                    .checked_add(path_len)
                    .ok_or(keldra_index::IndexError::OffsetOverflow)?,
            )
            .and_then(|value| value.checked_add(count_offset + 2))
            .map(|value| value.max(bytes.len()))
            .ok_or(keldra_index::IndexError::OffsetOverflow),
        Some(2) => count
            .checked_mul(9)
            .and_then(|value| value.checked_add(bytes.len()))
            .ok_or(keldra_index::IndexError::OffsetOverflow),
        _ => Err(keldra_index::IndexError::Integrity),
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::working_memory::{IndexWorkingMemory, SharedIndexingMemoryBackend};
    use super::*;
    use keldra_index::v1::{IndexingMemoryLimits, IndexingMemoryStage};

    #[test]
    fn growable_merge_owner_uses_spare_aggregate_capacity_and_survives_cloning() {
        let memory = IndexWorkingMemory::new(1_000, [200, 800]).unwrap();
        let limits = IndexingMemoryLimits {
            hot_payload_bytes: 800,
            worker_scratch_bytes: 800,
            prepared_rows_bytes: 800,
            replay_input_bytes: 800,
            projection_accumulator_bytes: 800,
            seal_scratch_bytes: 800,
            ordering_catalog_bytes: 800,
        };
        let credits = IndexingMemoryCredits::new_with_backend(
            800,
            limits,
            Arc::new(SharedIndexingMemoryBackend(memory.clone())),
        )
        .unwrap();
        let mut permit = credits
            .acquire(IndexingMemoryStage::WorkerScratch, 0)
            .unwrap();
        permit.grow_to(650).unwrap(); // Far beyond the deleted 800/8 share.
        assert_eq!(memory.free_bytes(), 350);
        assert!(
            credits
                .acquire(IndexingMemoryStage::WorkerScratch, 151)
                .is_err()
        );
        permit.shrink_to(24).unwrap();
        let bytes = Bytes::from_owner(AccountedTreePage {
            bytes: vec![7; 24],
            _permit: permit,
        });
        let reader = bytes.clone();
        drop(bytes);
        assert_eq!(memory.free_bytes(), 976);
        drop(reader);
        assert_eq!(memory.free_bytes(), 1_000);
        assert_eq!(credits.used_bytes(), 0);
    }

    #[test]
    fn read_refusals_and_corruption_keep_their_correct_category() {
        assert!(matches!(
            tree_read_error(Status::resource_exhausted("scratch")),
            keldra_index::IndexError::AdmissionDenied(_)
        ));
        assert!(matches!(
            tree_read_error(Status::data_loss("page")),
            keldra_index::IndexError::IntegrityViolation(_)
        ));
        assert_eq!(
            tree_read_error(Status::deadline_exceeded("caller")),
            keldra_index::IndexError::DeadlineExceeded
        );
    }

    #[test]
    fn page_decode_admission_tracks_actual_fanout_not_stream_history() {
        let mut small = vec![0; 13];
        small[11..13].copy_from_slice(&1_u16.to_le_bytes());
        let mut larger = small.clone();
        larger[11..13].copy_from_slice(&100_u16.to_le_bytes());
        assert!(
            page_decode_bound(&larger, true).unwrap() > page_decode_bound(&small, true).unwrap()
        );
        assert!(page_decode_bound(&[], true).is_err());
    }

    #[test]
    fn page_output_bound_includes_target_level_growth_without_a_configured_share() {
        let mut branch = vec![0; 1_024];
        branch[10] = 1;
        branch[11] = 2;
        branch[12..14].copy_from_slice(&128_u16.to_le_bytes());
        assert_eq!(
            page_output_bound(&branch, false, 200).unwrap(),
            1_024 + 128 * 9
        );
        let mut leaf = vec![0; 14];
        leaf[10] = 1;
        leaf[11] = 1;
        leaf[12..14].copy_from_slice(&64_u16.to_le_bytes());
        assert_eq!(
            page_output_bound(&leaf, false, 200).unwrap(),
            14 + 64 * (225 + 200)
        );
    }

    #[test]
    fn generated_page_allocation_keeps_guard_after_base_owner_is_discarded() {
        let limits = IndexingMemoryLimits {
            hot_payload_bytes: 100,
            worker_scratch_bytes: 100,
            prepared_rows_bytes: 100,
            replay_input_bytes: 100,
            projection_accumulator_bytes: 100,
            seal_scratch_bytes: 100,
            ordering_catalog_bytes: 100,
        };
        let credits = IndexingMemoryCredits::new(100, limits).unwrap();
        let owner = Arc::new(Mutex::new(
            credits
                .acquire(IndexingMemoryStage::WorkerScratch, 24)
                .unwrap(),
        ));
        let page = Bytes::from_owner(AccountedSplicePage {
            bytes: Bytes::from(vec![7; 24]),
            _owner: owner.clone(),
        });
        let pending_cas_page = page.clone();
        drop(page);
        drop(owner);
        assert_eq!(credits.used_bytes(), 24);
        drop(pending_cas_page);
        assert_eq!(credits.used_bytes(), 0);
    }
}
