//! Metadata-only acknowledgement of a completely replayed atomic barrier.
use super::*;
use keldra_index::v1::{
    IndexingMemoryCredits, IndexingMemoryStage, QueryRunReference,
    prepare_atomic_cut_acknowledgement,
};

impl V1ProjectionPublisher {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn prepare_atomic_cut_publication(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        current: &LoadedV1ProjectionGeneration,
        through_atomic: u64,
        credits: &IndexingMemoryCredits,
    ) -> Result<PendingV1Publication, Status> {
        let metadata = credits
            .acquire(
                IndexingMemoryStage::WorkerScratch,
                compaction_publication::metadata_admission_bytes(&current.generation)?,
            )
            .map_err(|_| {
                Status::resource_exhausted("v1 atomic acknowledgement metadata unavailable")
            })?;
        let permit = credits
            .acquire(IndexingMemoryStage::WorkerScratch, 1)
            .map_err(|_| {
                Status::resource_exhausted("v1 atomic acknowledgement memory unavailable")
            })?;
        let mut query_credits =
            QueryBlockCredits::from_growable_pipeline_permit(permit, credits.total_limit_bytes())
                .map_err(index_status)?;
        let mut pages = BTreeMap::new();
        let root = current.generation.query_stream_root;
        let mut latest: Option<(QueryRunReference, Bytes)> = None;
        if root.run_count != 0 {
            let mut hash = root.stream_root_hash;
            loop {
                // Read/decode/native entries and retained path-copy inputs
                // coexist. Admit them before cache lookup and decoding.
                query_credits
                    .reserve(3 * MAX_STREAM_PAGE_BYTES)
                    .map_err(index_status)?;
                let bytes = self
                    .read_immutable_object(
                        storage_tenant,
                        bucket,
                        tenant_id,
                        bucket_id,
                        &projection_query_run_stream_page_path(partition, hash),
                        hash,
                        MAX_STREAM_PAGE_BYTES,
                    )
                    .await?
                    .ok_or_else(|| {
                        Status::data_loss("v1 atomic acknowledgement query page absent")
                    })?;
                let page = decode_query_run_page(&bytes).map_err(index_status)?;
                pages.insert(hash, bytes);
                match page {
                    QueryRunPage::Branch(children) => {
                        hash = children
                            .last()
                            .ok_or_else(|| {
                                Status::data_loss("v1 atomic acknowledgement query branch empty")
                            })?
                            .hash;
                    }
                    QueryRunPage::Leaf(runs) => {
                        let reference = *runs.last().ok_or_else(|| {
                            Status::data_loss("v1 atomic acknowledgement query leaf empty")
                        })?;
                        let count = usize::try_from(reference.encoded_bytes).map_err(|_| {
                            Status::resource_exhausted(
                                "v1 atomic acknowledgement descriptor overflow",
                            )
                        })?;
                        if count > self.query_block_limits.maximum_run_descriptor_bytes {
                            return Err(Status::resource_exhausted(
                                "v1 atomic acknowledgement descriptor exceeds configured limit",
                            ));
                        }
                        query_credits.reserve(count).map_err(index_status)?;
                        let bytes = self
                            .read_immutable_object(
                                storage_tenant,
                                bucket,
                                tenant_id,
                                bucket_id,
                                &projection_query_run_pack_path(partition, reference.hash),
                                reference.hash,
                                count,
                            )
                            .await?
                            .ok_or_else(|| {
                                Status::data_loss("v1 atomic acknowledgement descriptor absent")
                            })?;
                        latest = Some((reference, bytes));
                        break;
                    }
                }
            }
        }
        let prepared = prepare_atomic_cut_acknowledgement(
            &current.generation,
            current.current.generation_hash,
            through_atomic,
            latest
                .as_ref()
                .map(|(reference, bytes)| (*reference, bytes.as_ref())),
            self.query_block_limits,
            query_credits,
            |hash| {
                pages
                    .get(&hash)
                    .cloned()
                    .ok_or(keldra_index::IndexError::Integrity)
            },
        )
        .map_err(index_status)?;
        let generation = decode_projection_generation(
            &prepared.generation.bytes,
            &prepared.generation.component_directory,
        )
        .map_err(index_status)?;
        let current_record = decode_projection_current(&prepared.current).map_err(index_status)?;
        let mut immutable = BTreeMap::new();
        if let Some(run) = prepared.query_run {
            insert_artifact(
                &mut immutable,
                projection_query_run_pack_path(partition, run.hash),
                keldra_index::v1::ProjectionArtifactKind::QueryRunPack,
                run.hash,
                run.bytes,
            )?;
        }
        for page in &prepared.query_stream_pages {
            insert_artifact(
                &mut immutable,
                projection_query_run_stream_page_path(partition, page.hash),
                keldra_index::v1::ProjectionArtifactKind::QueryRunStreamPage,
                page.hash,
                page.bytes.clone(),
            )?;
        }
        for page in &prepared.generation.component_directory.pages {
            insert_artifact(
                &mut immutable,
                projection_component_page_path(partition, page.hash),
                keldra_index::v1::ProjectionArtifactKind::ComponentPage,
                page.hash,
                page.bytes.clone(),
            )?;
        }
        insert_artifact(
            &mut immutable,
            projection_generation_path(partition, prepared.generation.hash),
            keldra_index::v1::ProjectionArtifactKind::Generation,
            prepared.generation.hash,
            prepared.generation.bytes,
        )?;
        Ok(PendingV1Publication {
            plan: AtomicPublicationPlan {
                immutable: immutable.into_values().collect(),
                current_bytes: prepared.current,
                current: current_record,
                generation,
                sealed_bytes: 0,
                source_positions: 0,
                _publication_credits: None,
                _compaction_metadata: Some(metadata),
            },
            expected_current_version: Some(current.current_object_version),
            previous_generation_hash: Some(current.current.generation_hash),
            checkpointed_source_positions: 0,
            checkpointed_source_payload_bytes: 0,
            _compaction: None,
            _atomic_cut: Some(prepared.query_credits),
        })
    }
}
