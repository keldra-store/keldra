use super::*;

use keldra_index::v1::{
    PreparedQueryMutationBatch, QueryBlockCredits, RealtimeOverlayCurrent, RealtimeOverlayEvidence,
    RealtimeOverlayGeneration, RealtimeOverlayRun, bind_realtime_source_position,
    decode_realtime_overlay_current, decode_realtime_overlay_generation,
    encode_realtime_overlay_current, encode_realtime_overlay_generation,
    projection_realtime_overlay_current_path, projection_realtime_overlay_generation_path,
};

#[derive(Clone, Debug)]
pub(crate) struct LoadedRealtimeOverlayGeneration {
    pub(crate) current: RealtimeOverlayCurrent,
    pub(crate) current_object_version: VersionId,
    pub(crate) generation: RealtimeOverlayGeneration,
}

impl V1ProjectionPublisher {
    #[allow(clippy::too_many_arguments)]
    async fn publish_realtime_query_run_descriptor(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        run: &keldra_index::v1::EncodedProjectionQueryRun,
    ) -> Result<(), Status> {
        let run_blob = self.stage(&run.bytes).await?;
        self.artifacts
            .publish(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                projection_artifact_routing_id(
                    partition.family_id,
                    keldra_index::v1::ProjectionArtifactKind::QueryRunPack,
                    run.hash,
                )
                .map_err(index_status)?,
                keldra_index::v1::projection_query_run_pack_path(partition, run.hash),
                run_blob,
                None,
            ))
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_realtime_overlay_generation_by_hash(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        generation_hash: [u8; 32],
    ) -> Result<LoadedRealtimeOverlayGeneration, Status> {
        let path = projection_realtime_overlay_generation_path(partition, generation_hash);
        let (bytes, version) = self
            .read_object(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                &path,
                Some(generation_hash),
                MAX_GENERATION_BYTES,
            )
            .await?
            .ok_or_else(|| {
                Status::failed_precondition(
                    "pinned real-time overlay generation is no longer retained",
                )
            })?;
        let generation = decode_realtime_overlay_generation(&bytes).map_err(index_status)?;
        if generation.partition != partition {
            return Err(Status::data_loss(
                "real-time overlay generation belongs to another partition",
            ));
        }
        Ok(LoadedRealtimeOverlayGeneration {
            current: RealtimeOverlayCurrent {
                partition,
                physical_catalog_generation: generation.physical_catalog_generation,
                generation_hash,
                generation_revision: generation.revision,
            },
            current_object_version: version,
            generation,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_realtime_overlay(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
    ) -> Result<Option<LoadedRealtimeOverlayGeneration>, Status> {
        let Some((current_bytes, current_object_version)) = self
            .read_object(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                &projection_realtime_overlay_current_path(partition),
                None,
                1024,
            )
            .await?
        else {
            return Ok(None);
        };
        let current = decode_realtime_overlay_current(&current_bytes).map_err(index_status)?;
        if current.partition != partition {
            return Err(Status::data_loss(
                "real-time overlay current belongs to another partition",
            ));
        }
        let path = projection_realtime_overlay_generation_path(partition, current.generation_hash);
        let (generation_bytes, _) = self
            .read_object(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                &path,
                Some(current.generation_hash),
                MAX_GENERATION_BYTES,
            )
            .await?
            .ok_or_else(|| Status::data_loss("real-time overlay generation is absent"))?;
        let generation =
            decode_realtime_overlay_generation(&generation_bytes).map_err(index_status)?;
        if generation.partition != partition
            || generation.physical_catalog_generation != current.physical_catalog_generation
            || generation.revision != current.generation_revision
        {
            return Err(Status::data_loss(
                "real-time overlay current and generation disagree",
            ));
        }
        Ok(Some(LoadedRealtimeOverlayGeneration {
            current,
            current_object_version,
            generation,
        }))
    }

    /// Publish one bounded microbatch with exact sparse evidence and at most
    /// one immutable query artifact. Empty query material remains positive
    /// durable no-match evidence.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_realtime_overlay_batch(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        physical_catalog_generation: [u8; 32],
        mut batch_evidence: Vec<RealtimeOverlayEvidence>,
        mut query_batch: Option<PreparedQueryMutationBatch>,
        query_credits: Option<QueryBlockCredits>,
    ) -> Result<LoadedRealtimeOverlayGeneration, Status> {
        batch_evidence.sort_by_key(|entry| {
            (
                entry.source_position,
                entry.atomic_position,
                entry.atomic_unit_hash,
            )
        });
        if batch_evidence
            .windows(2)
            .any(|pair| pair[0].source_position >= pair[1].source_position)
        {
            return Err(Status::invalid_argument(
                "real-time microbatch evidence is empty or non-canonical",
            ));
        }
        if batch_evidence.is_empty() && query_batch.is_some() {
            return Err(Status::invalid_argument(
                "real-time query material requires sparse evidence",
            ));
        }
        let loaded_previous = self
            .load_realtime_overlay(storage_tenant, bucket, tenant_id, bucket_id, partition)
            .await?;
        if loaded_previous.is_none() && batch_evidence.is_empty() {
            return Err(Status::invalid_argument(
                "cannot reconcile an absent real-time overlay",
            ));
        }
        let previous = loaded_previous.as_ref().filter(|previous| {
            previous.generation.physical_catalog_generation == physical_catalog_generation
        });
        let base = self
            .load_current(storage_tenant, bucket, tenant_id, bucket_id, partition)
            .await?;
        let base_next = base.as_ref().map_or(0, |base| base.current.next_offset);
        let base_atomic = base
            .as_ref()
            .map_or(0, |base| base.current.through_atomic_position);

        let query_run = match (&mut query_batch, query_credits) {
            (Some(batch), Some(credits)) => {
                // The extractor already binds each document gate to its event.
                // Preserve compatibility for the one-event convenience path.
                let has_unbound = batch
                    .membership
                    .iter()
                    .flat_map(|delta| delta.gates.iter())
                    .chain(batch.fields.iter().map(|field| &field.delta.presence))
                    .any(|gate| gate.selective_source_position.is_none());
                if batch_evidence.len() == 1 && has_unbound {
                    bind_realtime_source_position(batch, batch_evidence[0].source_position)
                        .map_err(index_status)?;
                } else if has_unbound {
                    return Err(Status::invalid_argument(
                        "real-time microbatch contains an unbound query gate",
                    ));
                }
                let first = batch_evidence[0].source_position;
                let last = batch_evidence.last().unwrap().source_position;
                let through_atomic = batch_evidence
                    .iter()
                    .map(|entry| entry.atomic_position)
                    .max()
                    .unwrap_or(0);
                let prepared = prepare_projection_query_run(
                    partition,
                    physical_catalog_generation,
                    last.checked_add(1).ok_or_else(|| {
                        Status::resource_exhausted("real-time source position overflow")
                    })?,
                    first,
                    last.checked_add(1).ok_or_else(|| {
                        Status::resource_exhausted("real-time source position overflow")
                    })?,
                    through_atomic,
                    query_batch.take().expect("query batch exists"),
                    self.query_block_limits(),
                    credits,
                )
                .map_err(index_status)?;
                let table = self
                    .publish_query_packs(
                        storage_tenant,
                        bucket,
                        tenant_id,
                        bucket_id,
                        partition,
                        prepared.packs(),
                    )
                    .await?;
                let charged = prepared.finalize(table).map_err(index_status)?;
                let run = &charged.artifacts().run;
                self.publish_realtime_query_run_descriptor(
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    partition,
                    run,
                )
                .await?;
                Some(RealtimeOverlayRun {
                    source_positions: batch_evidence
                        .iter()
                        .map(|entry| entry.source_position)
                        .collect(),
                    query_run: keldra_index::v1::QueryRunReference {
                        hash: run.hash,
                        encoded_bytes: u64::try_from(run.bytes.len())
                            .map_err(|_| Status::resource_exhausted("overlay run is too large"))?,
                        sequence: last + 1,
                        level: 0,
                        source_start_offset: first,
                        next_offset: last + 1,
                        through_atomic_position: through_atomic,
                    },
                })
            }
            (None, None) => None,
            _ => {
                return Err(Status::invalid_argument(
                    "real-time query batch and memory credits must be supplied together",
                ));
            }
        };

        let mut evidence = previous
            .map(|previous| {
                previous
                    .generation
                    .unabsorbed_evidence(base_next, base_atomic)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for entry in batch_evidence.iter().cloned() {
            match evidence
                .binary_search_by_key(&entry.source_position, |value| value.source_position)
            {
                Ok(index) if evidence[index] == entry => {}
                Ok(_) => return Err(Status::data_loss("conflicting real-time overlay event")),
                Err(index) => evidence.insert(index, entry),
            }
        }
        let mut runs = previous
            .map(|previous| {
                previous
                    .generation
                    .unabsorbed_runs(base_next, base_atomic)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(run) = query_run {
            if !runs.iter().any(|existing| existing == &run) {
                runs.insert(0, run);
            }
        }
        if batch_evidence.is_empty()
            && previous.is_some_and(|previous| {
                previous.generation.evidence == evidence && previous.generation.runs == runs
            })
        {
            return Ok(previous.cloned().expect("checked present overlay"));
        }
        if evidence.len() > keldra_index::v1::MAX_REALTIME_OVERLAY_EVIDENCE
            || runs.len() > keldra_index::v1::MAX_REALTIME_OVERLAY_RUNS
        {
            return Err(Status::resource_exhausted(
                "real-time overlay requires base absorption or artifact compaction",
            ));
        }
        let revision = loaded_previous
            .as_ref()
            .map_or(1, |previous| previous.generation.revision.saturating_add(1));
        let generation = RealtimeOverlayGeneration {
            partition,
            physical_catalog_generation,
            revision,
            evidence,
            runs,
            previous_generation_hash: loaded_previous
                .as_ref()
                .map(|previous| previous.current.generation_hash),
        };
        let encoded = encode_realtime_overlay_generation(&generation).map_err(index_status)?;
        let generation_blob = self.stage(&encoded.bytes).await?;
        self.artifacts
            .publish(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                projection_artifact_routing_id(
                    partition.family_id,
                    keldra_index::v1::ProjectionArtifactKind::RealtimeOverlayGeneration,
                    encoded.hash,
                )
                .map_err(index_status)?,
                projection_realtime_overlay_generation_path(partition, encoded.hash),
                generation_blob,
                None,
            ))
            .await?;
        let current = RealtimeOverlayCurrent {
            partition,
            physical_catalog_generation,
            generation_hash: encoded.hash,
            generation_revision: revision,
        };
        let current_blob = self
            .stage(&encode_realtime_overlay_current(current).map_err(index_status)?)
            .await?;
        let outcome = self
            .artifacts
            .publish(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                projection_routing_id(partition),
                projection_realtime_overlay_current_path(partition),
                current_blob,
                loaded_previous
                    .as_ref()
                    .map(|previous| previous.current_object_version),
            ))
            .await?;
        let _ = self.changes.send(());
        Ok(LoadedRealtimeOverlayGeneration {
            current,
            current_object_version: outcome.version,
            generation,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn reconcile_realtime_overlay_absorption(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        physical_catalog_generation: [u8; 32],
    ) -> Result<Option<LoadedRealtimeOverlayGeneration>, Status> {
        if self
            .load_realtime_overlay(storage_tenant, bucket, tenant_id, bucket_id, partition)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        self.publish_realtime_overlay_batch(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            partition,
            physical_catalog_generation,
            Vec::new(),
            None,
            None,
        )
        .await
        .map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn compact_realtime_overlay_runs(
        &self,
        cpu: &super::super::cpu::IndexCpuPool,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        physical_catalog_generation: [u8; 32],
        credits: QueryBlockCredits,
    ) -> Result<bool, Status> {
        const TRIGGER: usize = 32;
        const FAN_IN: usize = 8;
        let Some(previous) = self
            .load_realtime_overlay(storage_tenant, bucket, tenant_id, bucket_id, partition)
            .await?
        else {
            return Ok(false);
        };
        if previous.generation.physical_catalog_generation != physical_catalog_generation
            || previous.generation.runs.len() < TRIGGER
        {
            return Ok(false);
        }
        let start = previous.generation.runs.len().saturating_sub(FAN_IN);
        let selected = &previous.generation.runs[start..];
        let mut source_positions = selected
            .iter()
            .flat_map(|run| run.source_positions.iter().copied())
            .collect::<Vec<_>>();
        source_positions.sort_unstable();
        source_positions.dedup();
        let inputs = selected.iter().map(|run| run.query_run).collect::<Vec<_>>();
        let (artifacts, reference, _credits) =
            super::super::v1_compaction::compact_sparse_overlay_runs(
                cpu,
                self,
                storage_tenant.to_owned(),
                bucket.to_owned(),
                tenant_id,
                bucket_id,
                partition,
                physical_catalog_generation,
                source_positions.clone(),
                inputs,
                credits,
            )
            .await?;
        self.publish_realtime_query_run_descriptor(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            partition,
            &artifacts.run,
        )
        .await?;
        let runs = replace_compacted_run_window(
            &previous.generation.runs,
            start,
            RealtimeOverlayRun {
                source_positions,
                query_run: reference,
            },
        )?;
        let generation = RealtimeOverlayGeneration {
            partition,
            physical_catalog_generation,
            revision: previous.generation.revision.saturating_add(1),
            evidence: previous.generation.evidence.clone(),
            runs,
            previous_generation_hash: Some(previous.current.generation_hash),
        };
        let encoded = encode_realtime_overlay_generation(&generation).map_err(index_status)?;
        let generation_blob = self.stage(&encoded.bytes).await?;
        self.artifacts
            .publish(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                projection_artifact_routing_id(
                    partition.family_id,
                    keldra_index::v1::ProjectionArtifactKind::RealtimeOverlayGeneration,
                    encoded.hash,
                )
                .map_err(index_status)?,
                projection_realtime_overlay_generation_path(partition, encoded.hash),
                generation_blob,
                None,
            ))
            .await?;
        let current = RealtimeOverlayCurrent {
            partition,
            physical_catalog_generation,
            generation_hash: encoded.hash,
            generation_revision: generation.revision,
        };
        let current_blob = self
            .stage(&encode_realtime_overlay_current(current).map_err(index_status)?)
            .await?;
        self.artifacts
            .publish(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                projection_routing_id(partition),
                projection_realtime_overlay_current_path(partition),
                current_blob,
                Some(previous.current_object_version),
            ))
            .await?;
        let _ = self.changes.send(());
        Ok(true)
    }
}

fn replace_compacted_run_window(
    runs: &[RealtimeOverlayRun],
    start: usize,
    replacement: RealtimeOverlayRun,
) -> Result<Vec<RealtimeOverlayRun>, Status> {
    if start >= runs.len() {
        return Err(Status::invalid_argument(
            "real-time compaction window is empty",
        ));
    }
    let mut expected = runs[start..]
        .iter()
        .flat_map(|run| run.source_positions.iter().copied())
        .collect::<Vec<_>>();
    expected.sort_unstable();
    expected.dedup();
    if expected != replacement.source_positions {
        return Err(Status::data_loss(
            "real-time compaction changed sparse position coverage",
        ));
    }
    let mut output = runs[..start].to_vec();
    output.push(replacement);
    Ok(output)
}

#[cfg(test)]
mod compaction_tests {
    use super::*;

    fn artifact(hash: u8, positions: &[u64]) -> RealtimeOverlayRun {
        RealtimeOverlayRun {
            source_positions: positions.to_vec(),
            query_run: keldra_index::v1::QueryRunReference {
                hash: [hash; 32],
                encoded_bytes: 1,
                sequence: positions[positions.len() - 1] + 1,
                level: 0,
                source_start_offset: positions[0],
                next_offset: positions[positions.len() - 1] + 1,
                through_atomic_position: 0,
            },
        }
    }

    #[test]
    fn compacted_window_preserves_exact_sparse_position_coverage() {
        let runs = vec![
            artifact(1, &[11]),
            artifact(2, &[7, 9]),
            artifact(3, &[2, 4]),
        ];
        let replacement = artifact(4, &[2, 4, 7, 9]);
        let compacted = replace_compacted_run_window(&runs, 1, replacement.clone()).unwrap();
        assert_eq!(compacted, vec![runs[0].clone(), replacement]);
        assert!(replace_compacted_run_window(&runs, 1, artifact(5, &[2, 7, 9])).is_err());
    }
}
