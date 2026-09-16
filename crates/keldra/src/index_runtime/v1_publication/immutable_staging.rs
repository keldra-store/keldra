//! Bounded staging for immutable v1 publication artifacts.

use super::*;

impl V1ProjectionPublisher {
    pub(super) async fn stage_immutable_artifacts(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        artifacts: Vec<ArtifactBytes>,
    ) -> Result<Vec<StagedArtifact>, Status> {
        let telemetry = super::super::v1_telemetry::global();
        let staging_timer = super::super::v1_telemetry::V1PipelineTelemetry::start_phase(
            &telemetry.immutable_staging_nanos,
        );
        let artifact_count = artifacts.len();
        let preflight = artifacts
            .iter()
            .enumerate()
            .map(|(ordinal, artifact)| {
                (
                    ordinal,
                    (
                        artifact.path.clone(),
                        artifact.hash,
                        u64::try_from(artifact.bytes.len()).unwrap_or(u64::MAX),
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
        let mut existing = Vec::new();
        let mut missing_ordinals = BTreeSet::new();
        for (ordinal, outcome) in preflight {
            match outcome? {
                Some((blob, _)) => {
                    let artifact = &artifacts[ordinal];
                    existing.push(StagedArtifact {
                        path: artifact.path.clone(),
                        kind: artifact.kind,
                        hash: artifact.hash,
                        blob,
                        needs_publication: false,
                    });
                }
                None => {
                    missing_ordinals.insert(ordinal);
                }
            }
        }
        let missing = artifacts
            .into_iter()
            .enumerate()
            .filter_map(|(ordinal, artifact)| {
                missing_ordinals.contains(&ordinal).then_some(artifact)
            })
            .collect();
        let work = immutable_stage_work(missing)?;
        let window_count = work.len();
        let publisher = self.clone();
        let outcomes = run_bounded_ordered(
            work,
            MAX_PARALLEL_IMMUTABLE_STAGE_WINDOWS,
            move |(window, artifacts)| {
                let publisher = publisher.clone();
                async move { publisher.stage_immutable_window(window, artifacts).await }
            },
        )
        .await?;
        let mut staged = existing;
        staged.reserve(artifact_count.saturating_sub(staged.len()));
        for (_, outcome) in outcomes {
            staged.extend(outcome?);
        }
        let staging_duration = staging_timer.elapsed();
        drop(staging_timer);
        tracing::info!(
            histogram.keldra_index_v1_stage_immutable_artifacts_duration_seconds =
                staging_duration.as_secs_f64(),
            immutable_artifacts = artifact_count,
            staging_windows = window_count,
            maximum_parallelism = MAX_PARALLEL_IMMUTABLE_STAGE_WINDOWS,
            "keldra_index_v1_immutable_staging"
        );
        Ok(staged)
    }

    async fn stage_immutable_window(
        &self,
        window: ImmutableStageWindow,
        artifacts: Vec<ArtifactBytes>,
    ) -> Result<Vec<StagedArtifact>, Status> {
        match window {
            ImmutableStageWindow::Unary { bytes } => {
                let mut artifacts = artifacts.into_iter();
                let artifact = artifacts.next().ok_or_else(|| {
                    Status::internal("v1 immutable staging plan omitted its unary artifact")
                })?;
                if artifacts.next().is_some() || artifact.bytes.len() != bytes {
                    return Err(Status::internal(
                        "v1 immutable unary staging plan changed its byte association",
                    ));
                }
                let blob = self.stage(&artifact.bytes).await?;
                Ok(vec![staged_artifact(artifact, blob)?])
            }
            ImmutableStageWindow::Inline { items, bytes } => {
                if artifacts.len() != items {
                    return Err(Status::internal(
                        "v1 immutable inline staging plan changed its item association",
                    ));
                }
                let mut inline_identities = Vec::with_capacity(items);
                let mut inline_blobs = Vec::with_capacity(items);
                let mut observed_bytes = 0_usize;
                for artifact in artifacts {
                    let ArtifactBytes {
                        path,
                        kind,
                        hash,
                        bytes,
                    } = artifact;
                    observed_bytes = observed_bytes.checked_add(bytes.len()).ok_or_else(|| {
                        Status::resource_exhausted("v1 inline artifact byte count overflow")
                    })?;
                    inline_identities.push(InlineArtifactIdentity {
                        path,
                        kind,
                        hash,
                        length: bytes.len(),
                    });
                    // Store staging accepts shared immutable buffers, so this
                    // window consumes the already-admitted publication bytes
                    // instead of creating an uncharged second allocation.
                    inline_blobs.push(bytes);
                }
                if observed_bytes != bytes {
                    return Err(Status::internal(
                        "v1 immutable inline staging plan changed its byte association",
                    ));
                }
                let mut staged = Vec::with_capacity(items);
                self.flush_inline_artifacts(&mut inline_identities, &mut inline_blobs, &mut staged)
                    .await?;
                Ok(staged)
            }
        }
    }

    async fn flush_inline_artifacts(
        &self,
        identities: &mut Vec<InlineArtifactIdentity>,
        bytes: &mut Vec<Bytes>,
        staged: &mut Vec<StagedArtifact>,
    ) -> Result<(), Status> {
        if bytes.is_empty() {
            return Ok(());
        }
        let blobs = self
            .store
            .stage_derived_progress_inline_blobs(bytes)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if blobs.len() != identities.len() || blobs.len() != bytes.len() {
            return Err(Status::data_loss(
                "staged v1 inline artifact result count differs from its input",
            ));
        }
        for (identity, blob) in std::mem::take(identities).into_iter().zip(blobs) {
            if blob.hash != identity.hash || blob.length != identity.length as u64 {
                return Err(Status::data_loss(
                    "staged v1 immutable artifact changed its exact bytes",
                ));
            }
            staged.push(StagedArtifact {
                path: identity.path,
                kind: identity.kind,
                hash: identity.hash,
                blob,
                needs_publication: true,
            });
        }
        bytes.clear();
        Ok(())
    }

    pub(super) async fn stage(&self, bytes: &[u8]) -> Result<BlobRef, Status> {
        self.store
            .stage_derived_progress_blob(bytes)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

pub(super) fn immutable_stage_windows(
    lengths: impl IntoIterator<Item = usize>,
) -> Result<Vec<ImmutableStageWindow>, Status> {
    let mut windows = Vec::new();
    let mut inline_items = 0_usize;
    let mut inline_bytes = 0_usize;
    for bytes in lengths {
        if bytes > PAYLOAD_ARTIFACT_CHUNK_BYTES {
            push_inline_stage_window(&mut windows, &mut inline_items, &mut inline_bytes);
            windows.push(ImmutableStageWindow::Unary { bytes });
            continue;
        }
        if !inline_window_fits(inline_items, inline_bytes, bytes) {
            push_inline_stage_window(&mut windows, &mut inline_items, &mut inline_bytes);
        }
        inline_items += 1;
        inline_bytes = inline_bytes.checked_add(bytes).ok_or_else(|| {
            Status::resource_exhausted("v1 inline staging window byte count overflow")
        })?;
    }
    push_inline_stage_window(&mut windows, &mut inline_items, &mut inline_bytes);
    Ok(windows)
}

pub(super) fn immutable_stage_work(
    artifacts: Vec<ArtifactBytes>,
) -> Result<Vec<(usize, (ImmutableStageWindow, Vec<ArtifactBytes>))>, Status> {
    let windows = immutable_stage_windows(artifacts.iter().map(|artifact| artifact.bytes.len()))?;
    let mut artifacts = VecDeque::from(artifacts);
    let mut work = Vec::with_capacity(windows.len());
    for (ordinal, window) in windows.into_iter().enumerate() {
        let items = match window {
            ImmutableStageWindow::Inline { items, .. } => items,
            ImmutableStageWindow::Unary { .. } => 1,
        };
        let mut window_artifacts = Vec::with_capacity(items);
        for _ in 0..items {
            window_artifacts.push(artifacts.pop_front().ok_or_else(|| {
                Status::internal("v1 immutable staging plan omitted an artifact")
            })?);
        }
        work.push((ordinal, (window, window_artifacts)));
    }
    if !artifacts.is_empty() {
        return Err(Status::internal(
            "v1 immutable staging plan left artifacts unassociated",
        ));
    }
    Ok(work)
}

fn push_inline_stage_window(
    windows: &mut Vec<ImmutableStageWindow>,
    items: &mut usize,
    bytes: &mut usize,
) {
    if *items != 0 {
        windows.push(ImmutableStageWindow::Inline {
            items: *items,
            bytes: *bytes,
        });
        *items = 0;
        *bytes = 0;
    }
}

pub(super) fn inline_window_fits(item_count: usize, byte_count: usize, next_bytes: usize) -> bool {
    next_bytes <= PAYLOAD_ARTIFACT_CHUNK_BYTES
        && item_count < MAX_DERIVED_PROGRESS_INLINE_BATCH_ITEMS
        && byte_count
            .checked_add(next_bytes)
            .and_then(|total| u64::try_from(total).ok())
            .is_some_and(|total| total <= MAX_DERIVED_PROGRESS_INLINE_BATCH_BYTES)
}

fn staged_artifact(artifact: ArtifactBytes, blob: BlobRef) -> Result<StagedArtifact, Status> {
    if blob.hash != artifact.hash || blob.length != artifact.bytes.len() as u64 {
        return Err(Status::data_loss(
            "staged v1 immutable artifact changed its exact bytes",
        ));
    }
    Ok(StagedArtifact {
        path: artifact.path,
        kind: artifact.kind,
        hash: artifact.hash,
        blob,
        needs_publication: true,
    })
}
