//! One speculative source page per writer; Current remains sole publication authority.
use super::super::events::IndexJournalPage;
use super::*;

#[derive(Clone)]
pub(super) struct LookAheadContext {
    pub(super) journal: Arc<IndexEventJournal>,
    pub(super) target: IndexBarrier,
}

pub(super) struct ReadyPage {
    pub(super) page: IndexJournalPage,
    pub(super) dispatches: Vec<V1SourceDispatch>,
    pub(super) memory: IndexingMemoryPermit,
    prepared: BTreeMap<
        (u64, u32),
        (
            (Mutation, u64, Arc<IndexingMemoryPermit>),
            V1PreparationSlot,
        ),
    >,
    progress: Option<IndexingProgressReservation>,
}

pub(super) struct LookAheadTask {
    task: Option<
        tokio::task::JoinHandle<(V1OrderedSourceDispatcher, Result<Option<ReadyPage>, Status>)>,
    >,
    predecessor_next: u64,
    partition: ProjectionPartitionIdentity,
    generation: [u8; 32],
    finished_before_publication: bool,
}
impl LookAheadTask {
    pub(super) fn note_publication_finished(&mut self) {
        self.finished_before_publication =
            self.task.as_ref().is_some_and(|task| task.is_finished());
    }
}
impl Drop for LookAheadTask {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn start_look_ahead(
    writer: &mut Writer,
    next: u64,
    physical_catalog_identity: [u8; 32],
    reader: &ClusterObjectReader,
    extractor: &V1ProjectionExtractor,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    if !writer.look_ahead_permitted
        || writer.look_ahead.is_some()
        || !writer.look_ahead_prepared.is_empty()
    {
        return Ok(());
    }
    writer.look_ahead_permitted = false;
    let Some(context) = writer.look_ahead_context.clone() else {
        return Ok(());
    };
    let max_page = u64::try_from(limits.flush_bytes.saturating_div(4).max(1))
        .unwrap_or(u64::MAX)
        .min(MAX_INDEX_EVENT_PAGE_BYTES)
        .max(1);
    let bytes = usize::try_from(max_page)
        .unwrap_or(usize::MAX)
        .saturating_mul(JOURNAL_PAGE_RESIDENT_MULTIPLIER)
        .max(1);
    // Optional read-ahead never takes promised foreground sealing room.
    let Ok(memory) = credits.acquire(IndexingMemoryStage::ReplayInput, bytes) else {
        tracing::debug!(
            counter.keldra_index_v1_look_ahead_admission_fallbacks = 1,
            "optional look-ahead page admission unavailable"
        );
        return Ok(());
    };
    let metadata_peak = match super::sealing::publication_metadata_bound(writer, true) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(()),
    };
    let Some(mut dispatcher) = writer.dispatcher.take() else {
        return Ok(());
    };
    let scanned = writer.scanned.clone();
    let recipe = writer.recipe.clone();
    let source = writer.source;
    let partition = writer.partition;
    let generation = recipe.physical_generation;
    let reader = reader.clone();
    let extractor = extractor.clone();
    let credits = credits.clone();
    tracing::debug!(
        counter.keldra_index_v1_look_ahead_started = 1,
        publication_next = next,
        source_node = source.node_id,
        "started bounded source preparation before immutable staging"
    );
    let task = tokio::spawn(async move {
        let started = Instant::now();
        let result = async {
            let Some(page) = context.journal.next_page(recipe.family.tenant_id, recipe.family.bucket_id, &scanned, &context.target, max_page).await.map_err(event_status)? else { return Ok(None); };
            let mut dispatches = Vec::new();
            for change in &page.changes {
                let event_source = page.through.sources[&change.node].source;
                dispatches.extend(dispatcher.observe(event_source, &change.change)?);
            }
            let node = NodeId(u64::from(source.node_id));
            let proposed = page.through.sources.get(&node).filter(|cursor| cursor.source == source).map_or(next, |cursor| cursor.next_offset);
            let safe_next = dispatcher.checkpoint_limit(source, proposed);
            let (_, mutations) = prepare_dispatches(next, dispatches.clone(), safe_next)?;
            let mut ready = ReadyPage { page, dispatches, memory, prepared: BTreeMap::new(), progress: None };
            // A source page can release a larger retained atomic group. Keep
            // that group intact and fall back to normal bounded preparation;
            // optional speculation must not split it to fit its own budget.
            if mutation_window_needed(&BTreeMap::new(), 0, &mutations)? > limits.flush_bytes { return Ok(Some(ready)); }
            let window = match prepare::prepare_window(recipe.clone(), source, physical_catalog_identity, mutations, &reader, &extractor, &credits, limits).await {
                Ok(window) => window,
                Err(error) if halts_partition(&error) => return Err(error),
                Err(_) => return Ok(Some(ready)),
            };
            let query = match keldra_index::v1::query_batches_seal_peak_bytes(window.chunks.values().flat_map(|(_, slots)| slots.iter()).filter_map(|slot| slot.prepared.as_ref().map(|prepared| &prepared.query)), keldra_index::v1::QueryBlockLimits::default_for_memory()).map_err(index_status) {
                Ok(query) => query,
                Err(error) if error.code() == tonic::Code::ResourceExhausted => return Ok(Some(ready)),
                Err(error) => return Err(error),
            };
            let components = match window.chunks.values().flat_map(|(metadata, slots)| metadata.iter().zip(slots)).try_fold(0usize, |bytes, ((mutation, _, _), slot)| {
                let Some(prepared) = &slot.prepared else { return Ok(bytes); };
                bytes.checked_add(keldra_index::v1::ProjectionMutationBuffer::source_replacement_admission_bytes(&mutation.path, &prepared.current).map_err(index_status)?).ok_or_else(|| Status::resource_exhausted("look-ahead sealing bound overflow"))
            }) {
                Ok(bytes) => bytes,
                Err(error) if error.code() == tonic::Code::ResourceExhausted => return Ok(Some(ready)),
                Err(error) => return Err(error),
            };
            let Some(peak) = components.checked_mul(3).and_then(|bytes| bytes.checked_add(query)).and_then(|bytes| bytes.checked_add(metadata_peak)) else { return Ok(Some(ready)); };
            let Ok(progress) = credits.reserve_progress(peak) else { tracing::debug!(counter.keldra_index_v1_look_ahead_admission_fallbacks = 1, "optional look-ahead output progress admission unavailable"); return Ok(Some(ready)); };
            ready.progress = Some(progress);
            for (metadata, slots) in window.chunks.into_values() {
                for (metadata, slot) in metadata.into_iter().zip(slots) { ready.prepared.insert((metadata.0.offset, metadata.0.ordinal), (metadata, slot)); }
            }
            tracing::debug!(counter.keldra_index_v1_look_ahead_prepared_mutations = ready.prepared.len(), histogram.keldra_index_v1_look_ahead_prepare_duration_seconds = started.elapsed().as_secs_f64(), publication_next = next, "prepared bounded look-ahead source window");
            Ok(Some(ready))
        }.await;
        (dispatcher, result)
    });
    writer.look_ahead = Some(LookAheadTask {
        task: Some(task),
        predecessor_next: next,
        partition,
        generation,
        finished_before_publication: false,
    });
    Ok(())
}

pub(super) async fn finish_look_ahead(
    writer: &mut Writer,
    target: &IndexBarrier,
) -> Result<Option<ReadyPage>, Status> {
    let Some(mut pending) = writer.look_ahead.take() else {
        return Ok(None);
    };
    if writer.pending_publication.is_some() {
        writer.look_ahead = Some(pending);
        return Ok(None);
    }
    let published = writer
        .current
        .as_ref()
        .map_or(0, |current| current.current.next_offset);
    if !cut_matches(
        published,
        writer.partition,
        writer.recipe.physical_generation,
        target.fence,
        &pending,
    ) {
        return Err(Status::failed_precondition(
            "look-ahead predecessor or placement fence changed",
        ));
    }
    let waited = Instant::now();
    let (dispatcher, result) = pending
        .task
        .as_mut()
        .expect("look-ahead task owned once")
        .await
        .map_err(|error| Status::internal(format!("look-ahead source task failed: {error}")))?;
    pending.task.take();
    writer.dispatcher = Some(dispatcher);
    let Some(mut ready) = result? else {
        return Ok(None);
    };
    tracing::debug!(
        histogram.keldra_index_v1_look_ahead_adoption_wait_seconds = waited.elapsed().as_secs_f64(),
        counter.keldra_index_v1_look_ahead_adopted_mutations = ready.prepared.len(),
        prepared_before_publication_finished = pending.finished_before_publication,
        publication_next = pending.predecessor_next,
        "adopted speculative source window after authoritative predecessor"
    );
    writer.look_ahead_prepared.append(&mut ready.prepared);
    if writer.sealing_progress.is_none() {
        writer.sealing_progress = ready.progress.take();
    } else {
        writer.look_ahead_progress = ready.progress.take();
    }
    Ok(Some(ready))
}

fn cut_matches(
    published: u64,
    partition: ProjectionPartitionIdentity,
    generation: [u8; 32],
    fence: keldra_store::PlacementLogId,
    pending: &LookAheadTask,
) -> bool {
    published >= pending.predecessor_next
        && partition == pending.partition
        && generation == pending.generation
        && fence.term == pending.partition.placement_term
        && fence.index == pending.partition.placement_index
}

#[cfg(test)]
mod tests {
    use super::*;
    use keldra_index::v1::IndexingMemoryLimits;
    fn credits(bytes: usize) -> IndexingMemoryCredits {
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
    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap()
    }
    fn dispatcher(memory: &IndexingMemoryCredits) -> V1OrderedSourceDispatcher {
        V1OrderedSourceDispatcher::new(
            keldra_store::PlacementLogId { term: 4, index: 5 },
            BTreeSet::from([SourceId {
                node_id: 2,
                source_epoch: [5; 32],
            }]),
            memory.acquire(IndexingMemoryStage::ReplayInput, 1).unwrap(),
            512,
        )
    }
    #[tokio::test]
    async fn independent_task_ownership_cannot_adopt_an_unpublished_predecessor_cut() {
        let memory = credits(1024);
        let control = dispatcher(&memory);
        let (prepared_tx, prepared_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            prepared_tx.send(()).unwrap();
            (control, Ok(None))
        });
        let mut pending = LookAheadTask {
            task: Some(task),
            predecessor_next: 11,
            partition: partition(),
            generation: [9; 32],
            finished_before_publication: false,
        };
        // Unit proof of independent scheduling and cut ownership only. Actual
        // flush/staging overlap is established separately by API qualification
        // and prepared_before_publication_finished production telemetry.
        tokio::time::timeout(Duration::from_secs(1), prepared_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(!cut_matches(
            10,
            partition(),
            [9; 32],
            keldra_store::PlacementLogId { term: 4, index: 5 },
            &pending
        ));
        assert!(cut_matches(
            11,
            partition(),
            [9; 32],
            keldra_store::PlacementLogId { term: 4, index: 5 },
            &pending
        ));
        assert!(!cut_matches(
            11,
            partition(),
            [8; 32],
            keldra_store::PlacementLogId { term: 4, index: 5 },
            &pending
        ));
        assert!(!cut_matches(
            11,
            partition(),
            [9; 32],
            keldra_store::PlacementLogId { term: 4, index: 6 },
            &pending
        ));
        let (control, result) = pending.task.as_mut().unwrap().await.unwrap();
        pending.task.take();
        assert!(result.unwrap().is_none());
        drop(control);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[tokio::test]
    async fn cancellation_during_owned_task_await_aborts_and_releases_credits() {
        let memory = credits(1024);
        let control = dispatcher(&memory);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
            (control, Ok(None))
        });
        let mut pending = LookAheadTask {
            task: Some(task),
            predecessor_next: 11,
            partition: partition(),
            generation: [9; 32],
            finished_before_publication: false,
        };
        started_rx.await.unwrap();
        let waiter = tokio::spawn(async move {
            let _ = pending.task.as_mut().unwrap().await;
        });
        waiter.abort();
        let _ = waiter.await;
        for _ in 0..10 {
            if memory.used_bytes() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn speculative_dispatcher_retains_atomic_unit_and_blocks_later_ordinary_cut() {
        let memory = credits(4096);
        let source = SourceId {
            node_id: 2,
            source_epoch: [5; 32],
        };
        let mut control = V1OrderedSourceDispatcher::new(
            keldra_store::PlacementLogId { term: 4, index: 5 },
            BTreeSet::from([source]),
            memory.acquire(IndexingMemoryStage::ReplayInput, 1).unwrap(),
            2048,
        );
        let mut head = keldra_store::ObjectHeadChange {
            offset: 20,
            tenant_id: 1,
            bucket_id: 2,
            exact_path: "items/held".into(),
            canonical_path: None,
            path_version: keldra_store::VersionId(20),
            kind: keldra_store::ObjectHeadChangeKind::Put,
            program_commit_cursor: Some(11),
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
        };
        assert!(
            control
                .observe(source, &keldra_store::LocalChange::ObjectHead(head.clone()))
                .unwrap()
                .is_empty()
        );
        head.offset = 21;
        head.exact_path = "items/later".into();
        head.path_version = keldra_store::VersionId(21);
        head.program_commit_cursor = None;
        assert!(
            control
                .observe(source, &keldra_store::LocalChange::ObjectHead(head))
                .unwrap()
                .is_empty()
        );
        assert_eq!(control.checkpoint_limit(source, 22), 20);
        // Neither speculation nor a publication stalled at an earlier cut can
        // authorize splitting the held atomic unit or publish the later row.
        drop(control);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn speculative_admission_cannot_consume_foreground_sealing_promise() {
        let memory = credits(1024);
        let _promise = memory.reserve_progress(900).unwrap();
        assert!(
            memory
                .acquire(IndexingMemoryStage::ReplayInput, 125)
                .is_err()
        );
        assert!(
            memory
                .acquire(IndexingMemoryStage::SealScratch, 900)
                .is_ok()
        );
    }
}
