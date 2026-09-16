use super::*;
use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;

type StageFuture<'a> = Pin<Box<dyn Future<Output = Result<BlobRef, Status>> + Send + 'a>>;

impl V1ProjectionPublisher {
    /// Resolve the exact published object version before accessing a range.
    /// Local values read only intersecting chunks. Missing local values retain
    /// the ordinary verified peer reconstruction and current-placement fence;
    /// only the requested range is allocated in the caller's working memory.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_exact_artifact_pack_range(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        reference: &ArtifactPackReference,
        offset: u64,
        length: u64,
        maximum_bytes: usize,
        expected_child_hash: [u8; 32],
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Bytes, Status> {
        reference.validate().map_err(index_status)?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Status::data_loss("v1 segment range offset overflow"))?;
        let count = usize::try_from(length).map_err(|_| {
            Status::resource_exhausted("v1 segment range exceeds platform capacity")
        })?;
        if offset > reference.length || end > reference.length {
            return Err(Status::data_loss(
                "v1 segment range exceeds its exact object length",
            ));
        }
        if count > maximum_bytes {
            return Err(Status::resource_exhausted(
                "v1 segment range exceeds its admitted byte bound",
            ));
        }
        let parsed =
            keldra_index::v1::parse_projection_artifact_path(reference.canonical_path.as_ref())
                .map_err(index_status)?;
        if !matches!(
            parsed.kind,
            keldra_index::v1::ProjectionArtifactKind::Pack
                | keldra_index::v1::ProjectionArtifactKind::QueryRunPack
        ) || parsed.content_hash != Some(reference.hash)
        {
            return Err(Status::data_loss(
                "v1 segment path differs from its exact content identity",
            ));
        }
        let key = ObjectKey::new(storage_tenant, bucket, reference.canonical_path.as_ref())
            .map_err(|error| Status::internal(error.to_string()))?;
        let version = self
            .reader
            .exact_version_descriptor_stable(
                &key,
                tenant_id,
                bucket_id,
                VersionId(reference.object_version),
            )
            .await?
            .ok_or_else(|| Status::data_loss("v1 segment exact object version is absent"))?;
        let blob = version
            .blob
            .as_ref()
            .ok_or_else(|| Status::data_loss("v1 segment version has no payload"))?;
        if version.deleted
            || version.id.0 != reference.object_version
            || blob.hash != reference.hash
            || blob.length != reference.length
        {
            return Err(Status::data_loss(
                "v1 segment object version differs from its published identity",
            ));
        }
        let range_account = if deadline.is_some() {
            super::super::working_memory::WorkingMemoryAccount::Query
        } else {
            super::super::working_memory::WorkingMemoryAccount::IndexingPipeline
        };
        let range_memory = self
            .immutable_cache
            .range_memory(range_account)
            .ok_or_else(|| Status::internal("v1 segment range has no working-memory authority"))?;
        // Blocking reads retain their own scratch admission even if the query
        // deadline cancels this future before the RocksDB call completes.
        let local_guard = range_memory.try_reserve(count)?;
        let store = self.store.clone();
        let local_blob = blob.clone();
        let (local_result, local_guard) = tokio::task::spawn_blocking(move || {
            let result = store.read_blob_range_sync(&local_blob, offset, length, maximum_bytes);
            (result, local_guard)
        })
        .await
        .map_err(|error| Status::internal(format!("join local segment range: {error}")))?;
        match local_result {
            Ok(bytes) => {
                let bytes = Bytes::from(bytes);
                // The caller's final-range admission owns the returned bytes.
                drop(local_guard);
                Ok(bytes)
            }
            Err(MutationError::BlobNotFound) => {
                drop(local_guard);
                self.reader
                    .read_verified_blob_range(
                        blob,
                        offset,
                        length,
                        maximum_bytes,
                        expected_child_hash,
                        deadline,
                        range_memory,
                    )
                    .await
                    .map(Bytes::from)
            }
            Err(error) => Err(Status::data_loss(format!(
                "read local segment range: {error}"
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_component_packs(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        packs: &[keldra_index::v1::SealedProjectionDeltaPack],
    ) -> Result<ArtifactPackTable, Status> {
        let inputs = packs
            .iter()
            .map(|pack| {
                (
                    pack.ordinal,
                    projection_pack_path(partition, pack.hash),
                    keldra_index::v1::ProjectionArtifactKind::Pack,
                    pack.hash,
                    pack.bytes.as_slice(),
                )
            })
            .collect();
        self.publish_physical_packs(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            partition,
            inputs,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_query_packs(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        packs: &[keldra_index::v1::UnpublishedArtifactPack],
    ) -> Result<ArtifactPackTable, Status> {
        let inputs = packs
            .iter()
            .map(|pack| {
                (
                    pack.ordinal,
                    projection_query_run_pack_path(partition, pack.hash),
                    keldra_index::v1::ProjectionArtifactKind::QueryRunPack,
                    pack.hash,
                    pack.bytes.as_slice(),
                )
            })
            .collect();
        self.publish_physical_packs(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            partition,
            inputs,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_physical_packs(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        packs: Vec<(
            u32,
            String,
            keldra_index::v1::ProjectionArtifactKind,
            [u8; 32],
            &[u8],
        )>,
    ) -> Result<ArtifactPackTable, Status> {
        if packs.is_empty() {
            return ArtifactPackTable::new(Vec::new()).map_err(index_status);
        }
        // Preflight at each path's stable object coordinator before staging.
        // Content-addressed artifacts are immutable, so an exact existing head
        // is already the required durable result. This avoids restaging and
        // rereplicating packs after retry, producer handoff, or CAS loss in
        // every topology rather than only consulting the local blob store.
        let preflight = packs
            .iter()
            .enumerate()
            .map(|(index, (_, path, _, hash, bytes))| {
                (
                    index,
                    (
                        path.clone(),
                        *hash,
                        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                    ),
                )
            })
            .collect();
        let publisher = self.clone();
        let storage_tenant_owned = storage_tenant.to_owned();
        let bucket_owned = bucket.to_owned();
        let preflight = run_bounded_ordered(
            preflight,
            MAX_PARALLEL_IMMUTABLE_STAGE_WINDOWS,
            move |(path, hash, length)| {
                let publisher = publisher.clone();
                let storage_tenant = storage_tenant_owned.clone();
                let bucket = bucket_owned.clone();
                async move {
                    publisher
                        .preflight_immutable_head(
                            &storage_tenant,
                            &bucket,
                            tenant_id,
                            bucket_id,
                            &path,
                            hash,
                            length,
                        )
                        .await
                }
            },
        )
        .await?;
        let mut versions = vec![None; packs.len()];
        let mut missing = Vec::new();
        for (index, outcome) in preflight {
            match outcome? {
                Some((_, version)) => versions[index] = Some(version),
                None => missing.push(index),
            }
        }
        let staged = self
            .stage_physical_pack_bytes(
                missing
                    .iter()
                    .map(|index| (*index, packs[*index].4))
                    .collect(),
            )
            .await?;
        let mut publications = Vec::with_capacity(staged.len());
        for (index, blob) in staged {
            let (_, path, kind, hash, bytes) = &packs[index];
            if blob.hash != *hash || blob.length != bytes.len() as u64 {
                return Err(Status::data_loss(
                    "staged v1 physical pack changed its exact bytes",
                ));
            }
            publications.push(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                projection_artifact_routing_id(partition.family_id, *kind, *hash)
                    .map_err(index_status)?,
                path.clone(),
                blob,
                None,
            ));
        }
        if !publications.is_empty() {
            let outcomes = self.artifacts.publish_immutable_many(publications).await?;
            if outcomes.len() != missing.len() {
                return Err(Status::data_loss(
                    "v1 physical pack outcomes differ from their missing inputs",
                ));
            }
            for (index, outcome) in missing.into_iter().zip(outcomes) {
                versions[index] = Some(outcome?.version);
            }
        }
        let mut references = Vec::with_capacity(packs.len());
        for ((ordinal, path, _, hash, bytes), version) in packs.into_iter().zip(versions) {
            let version = version.ok_or_else(|| {
                Status::data_loss("v1 physical pack publication omitted an exact object version")
            })?;
            references.push(ArtifactPackReference {
                ordinal,
                canonical_path: path.into(),
                object_version: version.0,
                hash,
                length: bytes.len() as u64,
            });
        }
        ArtifactPackTable::new(references).map_err(index_status)
    }

    /// Stage borrowed physical-pack buffers concurrently without cloning their
    /// already memory-admitted bytes merely to satisfy a `'static` task bound.
    async fn stage_physical_pack_bytes<'a>(
        &'a self,
        packs: Vec<(usize, &'a [u8])>,
    ) -> Result<Vec<(usize, BlobRef)>, Status> {
        let mut pending = VecDeque::<(usize, StageFuture<'a>)>::new();
        for (index, bytes) in packs {
            pending.push_back((index, Box::pin(async move { self.stage(bytes).await })));
        }
        Self::run_bounded_stage_futures(pending, MAX_PARALLEL_IMMUTABLE_STAGE_WINDOWS).await
    }

    async fn run_bounded_stage_futures<'a>(
        mut pending: VecDeque<(usize, StageFuture<'a>)>,
        maximum_parallelism: usize,
    ) -> Result<Vec<(usize, BlobRef)>, Status> {
        let mut active = Vec::<(usize, StageFuture<'a>)>::new();
        let mut staged = Vec::with_capacity(pending.len());
        let maximum_parallelism = maximum_parallelism.max(1);
        while !pending.is_empty() || !active.is_empty() {
            while active.len() < maximum_parallelism {
                let Some(next) = pending.pop_front() else {
                    break;
                };
                active.push(next);
            }
            let (slot, index, result) = poll_fn(|context| {
                for (slot, (index, future)) in active.iter_mut().enumerate() {
                    if let Poll::Ready(result) = future.as_mut().poll(context) {
                        return Poll::Ready((slot, *index, result));
                    }
                }
                Poll::Pending
            })
            .await;
            drop(active.swap_remove(slot));
            staged.push((index, result?));
        }
        staged.sort_unstable_by_key(|(index, _)| *index);
        Ok(staged)
    }

    /// Resolve an immutable content-addressed path at its stable coordinator.
    /// An existing path is reusable only when its live head has exactly the
    /// expected content identity. A conflicting or deleted head is corruption,
    /// never a signal to overwrite an immutable object.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn preflight_immutable_head(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        expected_hash: [u8; 32],
        expected_length: u64,
    ) -> Result<Option<(BlobRef, VersionId)>, Status> {
        let key = ObjectKey::new(storage_tenant, bucket, path)
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(snapshot) = self
            .reader
            .current_head_snapshot_stable(&key, tenant_id, bucket_id)
            .await?
        else {
            return Ok(None);
        };
        if snapshot.version.deleted {
            return Err(Status::data_loss(
                "v1 immutable artifact path has a deleted authoritative head",
            ));
        }
        let blob = snapshot
            .version
            .blob
            .ok_or_else(|| Status::data_loss("v1 immutable artifact head has no payload"))?;
        if blob.hash != expected_hash || blob.length != expected_length {
            return Err(Status::data_loss(
                "v1 immutable artifact path has a conflicting content identity",
            ));
        }
        Ok(Some((blob, snapshot.version.id)))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_exact_artifact_pack(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        reference: &ArtifactPackReference,
    ) -> Result<Bytes, Status> {
        reference.validate().map_err(index_status)?;
        let parsed =
            keldra_index::v1::parse_projection_artifact_path(reference.canonical_path.as_ref())
                .map_err(index_status)?;
        if !matches!(
            parsed.kind,
            keldra_index::v1::ProjectionArtifactKind::Pack
                | keldra_index::v1::ProjectionArtifactKind::QueryRunPack
        ) || parsed.content_hash != Some(reference.hash)
        {
            return Err(Status::data_loss(
                "v1 artifact pack path differs from its exact content identity",
            ));
        }
        let maximum_bytes = usize::try_from(reference.length)
            .map_err(|_| Status::data_loss("v1 artifact pack length is unbounded"))?;
        self.immutable_cache
            .get_or_load(
                tenant_id,
                bucket_id,
                &reference.canonical_path,
                reference.hash,
                Some(reference.object_version),
                maximum_bytes,
                || async {
                    let key =
                        ObjectKey::new(storage_tenant, bucket, reference.canonical_path.as_ref())
                            .map_err(|error| Status::internal(error.to_string()))?;
                    let version = self
                        .reader
                        .exact_version_descriptor_stable(
                            &key,
                            tenant_id,
                            bucket_id,
                            VersionId(reference.object_version),
                        )
                        .await?
                        .ok_or_else(|| Status::data_loss("v1 artifact pack is absent"))?;
                    if version.id.0 != reference.object_version || version.deleted {
                        return Err(Status::data_loss(
                            "v1 artifact pack object version differs from its root reference",
                        ));
                    }
                    let blob = version.blob.as_ref().ok_or_else(|| {
                        Status::data_loss("v1 artifact pack version has no payload")
                    })?;
                    if blob.hash != reference.hash || blob.length != reference.length {
                        return Err(Status::data_loss(
                            "v1 artifact pack object version differs from its root identity",
                        ));
                    }
                    // The exact path/version cache is the one owner for a
                    // physical pack. Going through the content-only blob cache
                    // here would retain and charge the same `Bytes` allocation
                    // twice under two eviction records.
                    let bytes = self
                        .read_blob_local_first_uncached(blob, maximum_bytes)
                        .await?;
                    Ok(Some(Bytes::from(bytes)))
                },
            )
            .await?
            .ok_or_else(|| Status::data_loss("v1 artifact pack is absent"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn borrowed_physical_pack_staging_is_bounded_and_ordered() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut pending = VecDeque::<(usize, StageFuture<'_>)>::new();
        for index in (0..8).rev() {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            pending.push_back((
                index,
                Box::pin(async move {
                    let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                    peak.fetch_max(now, Ordering::AcqRel);
                    tokio::task::yield_now().await;
                    active.fetch_sub(1, Ordering::AcqRel);
                    Ok(BlobRef {
                        hash: [index as u8; 32],
                        length: index as u64,
                    })
                }),
            ));
        }

        let staged = V1ProjectionPublisher::run_bounded_stage_futures(pending, 3)
            .await
            .unwrap();
        assert_eq!(peak.load(Ordering::Acquire), 3);
        assert_eq!(
            staged
                .iter()
                .map(|(index, blob)| (*index, blob.length))
                .collect::<Vec<_>>(),
            (0..8)
                .map(|index| (index, index as u64))
                .collect::<Vec<_>>()
        );
    }
}
