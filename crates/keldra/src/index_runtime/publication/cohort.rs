//! Bounded physical publication shared across index definitions.

use tracing::Instrument;

use super::*;

pub(super) fn record_definition_guard_outcome(
    outcomes: &mut [Option<IndexArtifactPublicationOutcome>],
    valid: &mut Vec<(usize, IndexArtifactPublish)>,
    index: usize,
    request: IndexArtifactPublish,
    validation: Result<(), Status>,
) -> Result<(), Status> {
    match validation {
        Ok(()) => valid.push((index, request)),
        Err(error) if error.code() == tonic::Code::FailedPrecondition => {
            let slot = outcomes.get_mut(index).ok_or_else(|| {
                Status::data_loss("definition guard outcome index is out of bounds")
            })?;
            if slot.replace(Err(error)).is_some() {
                return Err(Status::data_loss(
                    "definition guard outcome was recorded more than once",
                ));
            }
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

struct GroupedPublishTelemetry {
    span: tracing::Span,
    started: std::time::Instant,
    requested_items: u64,
    requested_bytes: u64,
    groups: u64,
    batches: u64,
    local_batches: u64,
    remote_batches: u64,
    attempted_items: u64,
    attempted_bytes: u64,
    finished: bool,
}

impl GroupedPublishTelemetry {
    fn start(requests: &[IndexArtifactPublish]) -> Self {
        let first = &requests[0];
        let requested_items = requests.len() as u64;
        let requested_bytes = requests.iter().fold(0_u64, |total, request| {
            total.saturating_add(request.blob.length)
        });
        let span = tracing::debug_span!(
            "keldra.index.grouped_publish",
            index.id = first.index_id,
            tenant.id = first.tenant_id,
            bucket.id = first.bucket_id,
            publish.requested_items = requested_items,
            publish.requested_bytes = requested_bytes,
            publish.groups = tracing::field::Empty,
            publish.batches = tracing::field::Empty,
            publish.local_batches = tracing::field::Empty,
            publish.remote_batches = tracing::field::Empty,
            publish.attempted_items = tracing::field::Empty,
            publish.attempted_bytes = tracing::field::Empty,
            publish.elapsed_seconds = tracing::field::Empty,
            publish.outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        span.in_scope(|| {
            tracing::debug!(
                operation = "index_artifact_grouped_publish",
                counter.keldra_index_grouped_publish_active = 1_i64,
                monotonic_counter.keldra_index_grouped_publish_attempts_total = 1_u64,
                "grouped index artifact publication started"
            );
        });
        Self {
            span,
            started: std::time::Instant::now(),
            requested_items,
            requested_bytes,
            groups: 0,
            batches: 0,
            local_batches: 0,
            remote_batches: 0,
            attempted_items: 0,
            attempted_bytes: 0,
            finished: false,
        }
    }

    fn record_batch(&mut self, local: bool, batch: &[(usize, IndexArtifactPublish)]) {
        self.batches = self.batches.saturating_add(1);
        if local {
            self.local_batches = self.local_batches.saturating_add(1);
        } else {
            self.remote_batches = self.remote_batches.saturating_add(1);
        }
        self.attempted_items = self.attempted_items.saturating_add(batch.len() as u64);
        self.attempted_bytes = self.attempted_bytes.saturating_add(
            batch
                .iter()
                .map(|(_, request)| request.blob.length)
                .sum::<u64>(),
        );
    }

    fn finish(&mut self, failed: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        let elapsed_seconds = self.started.elapsed().as_secs_f64();
        let outcome = if failed { "failed" } else { "completed" };
        self.span.record("publish.groups", self.groups);
        self.span.record("publish.batches", self.batches);
        self.span
            .record("publish.local_batches", self.local_batches);
        self.span
            .record("publish.remote_batches", self.remote_batches);
        self.span
            .record("publish.attempted_items", self.attempted_items);
        self.span
            .record("publish.attempted_bytes", self.attempted_bytes);
        self.span.record("publish.elapsed_seconds", elapsed_seconds);
        self.span.record("publish.outcome", outcome);
        self.span
            .record("otel.status_code", if failed { "error" } else { "ok" });
        self.span.in_scope(|| {
            tracing::debug!(
                operation = "index_artifact_grouped_publish",
                counter.keldra_index_grouped_publish_active = -1_i64,
                "grouped index artifact publication released"
            );
            tracing::debug!(
                operation = "index_artifact_grouped_publish",
                publish.outcome = outcome,
                monotonic_counter.keldra_index_grouped_publish_failures_total = u64::from(failed),
                monotonic_counter.keldra_index_grouped_publish_batches_total = self.batches,
                monotonic_counter.keldra_index_grouped_publish_local_batches_total =
                    self.local_batches,
                monotonic_counter.keldra_index_grouped_publish_remote_batches_total =
                    self.remote_batches,
                monotonic_counter.keldra_index_grouped_publish_items_total = self.attempted_items,
                monotonic_counter.keldra_index_grouped_publish_bytes_total = self.attempted_bytes,
                histogram.keldra_index_grouped_publish_requested_items = self.requested_items,
                histogram.keldra_index_grouped_publish_requested_bytes = self.requested_bytes,
                histogram.keldra_index_grouped_publish_replica_groups = self.groups,
                histogram.keldra_index_grouped_publish_batch_count = self.batches,
                histogram.keldra_index_grouped_publish_duration_seconds = elapsed_seconds,
                "grouped index artifact publication finished"
            );
        });
    }
}

impl Drop for GroupedPublishTelemetry {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(true);
        }
    }
}

impl IndexArtifactCoordinator {
    pub(super) async fn publish_immutable_many(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        validate_immutable_batch(&requests)?;
        let first = &requests[0];
        let admission = first.admission;
        let identity = IndexIdentity::new(first.tenant_id, first.bucket_id, first.index_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        for request in &requests {
            let candidate =
                IndexIdentity::new(request.tenant_id, request.bucket_id, request.index_id)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
            self.validate_index_builder(authenticated_builder, &placement, candidate)?;
        }
        let governance = self
            .governance
            .resolve(&first.storage_tenant, &first.bucket)
            .await?;
        if (governance.tenant_id, governance.bucket_id)
            != (identity.tenant_id(), identity.bucket_id())
        {
            return Err(Status::failed_precondition(
                "index artifact mutable names no longer bind the supplied stable IDs",
            ));
        }
        let first_key = first.key()?;
        let group = self.objects.object_replica_group_stable(
            &placement,
            &first_key,
            first.tenant_id,
            first.bucket_id,
        )?;
        if group.coordinator() != self.objects.local_node() {
            return Err(Status::failed_precondition(
                "grouped index artifacts reached the wrong object coordinator",
            ));
        }
        for request in &requests[1..] {
            let key = request.key()?;
            let candidate = self.objects.object_replica_group_stable(
                &placement,
                &key,
                request.tenant_id,
                request.bucket_id,
            )?;
            if candidate != group {
                return Err(Status::invalid_argument(
                    "grouped index artifacts span metadata replica groups",
                ));
            }
        }
        let durability = artifact_durability(
            ArtifactPathKind::Immutable,
            placement.placement_nodes().len(),
        );
        let publishes = requests
            .into_iter()
            .map(|request| {
                Ok(PublishRequest {
                    key: request.key()?,
                    blob: request.blob,
                    content_type: Some(INDEX_ARTIFACT_CONTENT_TYPE.into()),
                    mode: PutMode::PutIfAbsent,
                    command_id: Some(request.command_id),
                    durability,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let results = if admission.is_publication_progress() {
            self.objects
                .publish_many_derived_progress_from_source_with_governance(
                    publishes,
                    authenticated_builder,
                    governance,
                    placement,
                )
                .await?
        } else {
            self.objects
                .publish_many_from_source_with_governance(
                    publishes,
                    authenticated_builder,
                    governance,
                    placement,
                )
                .await?
        };
        Ok(results
            .into_iter()
            .map(|outcome| {
                outcome.map(|receipt| IndexArtifactOutcome {
                    version: receipt.version,
                    replayed: receipt.replayed,
                })
            })
            .collect())
    }

    pub(super) async fn publish_mutable_many(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        validate_guarded_batch(&requests)?;
        let first = &requests[0];
        for request in &requests {
            let identity =
                IndexIdentity::new(request.tenant_id, request.bucket_id, request.index_id)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
            self.validate_index_builder(authenticated_builder, &placement, identity)?;
        }
        let governance = self
            .governance
            .resolve(&first.storage_tenant, &first.bucket)
            .await?;
        if (governance.tenant_id, governance.bucket_id) != (first.tenant_id, first.bucket_id) {
            return Err(Status::failed_precondition(
                "index artifact mutable names no longer bind the supplied stable IDs",
            ));
        }
        let first_key = first.key()?;
        let group = self.objects.object_replica_group_stable(
            &placement,
            &first_key,
            first.tenant_id,
            first.bucket_id,
        )?;
        if group.coordinator() != self.objects.local_node() {
            return Err(Status::failed_precondition(
                "grouped guarded index artifacts reached the wrong object coordinator",
            ));
        }
        for request in &requests[1..] {
            let candidate = self.objects.object_replica_group_stable(
                &placement,
                &request.key()?,
                request.tenant_id,
                request.bucket_id,
            )?;
            if candidate != group {
                return Err(Status::invalid_argument(
                    "grouped guarded index artifacts span metadata replica groups",
                ));
            }
        }
        let durability =
            artifact_durability(ArtifactPathKind::Current, placement.placement_nodes().len());
        let publishes = requests
            .into_iter()
            .map(|request| {
                let mode = request
                    .expected_version
                    .map_or(PutMode::PutIfAbsent, PutMode::PutIfVersion);
                Ok(PublishRequest {
                    key: request.key()?,
                    blob: request.blob,
                    content_type: Some(INDEX_ARTIFACT_CONTENT_TYPE.into()),
                    mode,
                    command_id: Some(request.command_id),
                    durability,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let results = self
            .objects
            .publish_many_derived_progress_from_source_with_governance(
                publishes,
                authenticated_builder,
                governance,
                placement,
            )
            .await?;
        Ok(results
            .into_iter()
            .map(|outcome| {
                outcome.map(|receipt| IndexArtifactOutcome {
                    version: receipt.version,
                    replayed: receipt.replayed,
                })
            })
            .collect())
    }
}

impl IndexArtifactRouter {
    pub(crate) fn guarded_publication_cohort(
        &self,
        request: &IndexArtifactPublish,
    ) -> Result<GuardedIndexArtifactCohort, Status> {
        if request.validate()? != ArtifactPathKind::Current {
            return Err(Status::invalid_argument(
                "guarded publication cohort accepts current pointers only",
            ));
        }
        let placement =
            self.require_local_builder(request.tenant_id, request.bucket_id, request.index_id)?;
        self.guarded_publication_cohort_at(request, &placement)
    }

    fn guarded_publication_cohort_at(
        &self,
        request: &IndexArtifactPublish,
        placement: &ClusterPlacement,
    ) -> Result<GuardedIndexArtifactCohort, Status> {
        let definition_key = request
            .definition_guard
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("guarded publication has no guard"))?
            .key(&request.storage_tenant, &request.bucket)?;
        let definition_group = self.objects.object_replica_group_stable(
            placement,
            &definition_key,
            request.tenant_id,
            request.bucket_id,
        )?;
        let current_group = self.objects.object_replica_group_stable(
            placement,
            &request.key()?,
            request.tenant_id,
            request.bucket_id,
        )?;
        Ok(GuardedIndexArtifactCohort {
            storage_tenant: request.storage_tenant.clone(),
            bucket: request.bucket.clone(),
            tenant_id: request.tenant_id,
            bucket_id: request.bucket_id,
            admission: request.admission,
            fence: placement.fence(),
            definition_replicas: definition_group.replicas().to_vec(),
            current_replicas: current_group.replicas().to_vec(),
        })
    }

    pub(crate) async fn publish_many(
        &self,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactOutcome>, Status> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut telemetry = GroupedPublishTelemetry::start(&requests);
        let span = telemetry.span.clone();
        let result = self
            .publish_many_inner(requests, &mut telemetry)
            .instrument(span)
            .await
            .and_then(|outcomes| outcomes.into_iter().collect());
        telemetry.finish(result.is_err());
        result
    }

    pub(crate) async fn publish_immutable_cohort(
        &self,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut telemetry = GroupedPublishTelemetry::start(&requests);
        let span = telemetry.span.clone();
        let result = self
            .publish_many_inner(requests, &mut telemetry)
            .instrument(span)
            .await;
        telemetry.finish(result.is_err());
        result
    }

    pub(crate) async fn publish_guarded_cohort(
        &self,
        guarded: Vec<GuardedIndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        if guarded.is_empty() {
            return Ok(Vec::new());
        }
        let mut requests = Vec::with_capacity(guarded.len());
        let mut current_guards = Vec::with_capacity(guarded.len());
        for item in guarded {
            if item.current_guard.index_id != item.request.index_id {
                return Err(Status::internal(
                    "guarded publication cohort contains a mismatched current-mutation guard",
                ));
            }
            if item.request.exact_path != current_path(item.request.index_id) {
                return Err(Status::invalid_argument(
                    "guarded publication cohort accepts current pointers only",
                ));
            }
            requests.push(item.request);
            current_guards.push(item.current_guard);
        }
        validate_guarded_batch(&requests)?;
        let first = &requests[0];
        let placement =
            self.require_local_builder(first.tenant_id, first.bucket_id, first.index_id)?;
        let fence = placement.fence();
        let cohort = self.guarded_publication_cohort_at(first, &placement)?;
        for request in &requests {
            let candidate =
                self.require_local_builder(request.tenant_id, request.bucket_id, request.index_id)?;
            if candidate.fence() != fence {
                return Err(Status::unavailable(
                    "index placement changed while routing a guarded publication cohort",
                ));
            }
            if self.guarded_publication_cohort_at(request, &placement)? != cohort {
                return Err(Status::invalid_argument(
                    "guarded publication cohort spans physical routing tuples",
                ));
            }
        }
        let definition_coordinator = cohort.definition_replicas[0];
        let address = (definition_coordinator != self.local_node)
            .then(|| {
                placement.address(definition_coordinator).ok_or_else(|| {
                    Status::unavailable(format!(
                        "ACTIVE definition coordinator {} has no peer address",
                        definition_coordinator.0
                    ))
                })
            })
            .transpose()?;
        let indices = (0..requests.len()).collect::<Vec<_>>();
        let published = if let Some(address) = address {
            self.peers
                .publish_guarded_index_artifacts(
                    definition_coordinator,
                    &address.0,
                    fence,
                    &requests,
                )
                .await
        } else {
            self.coordinator
                .publish_guarded_many(self.local_node, placement.clone(), requests)
                .await
        };
        let published = match self.require_fence(fence) {
            Ok(()) => published,
            Err(error) => Err(error),
        };
        let mut outcomes = std::iter::repeat_with(|| None)
            .take(indices.len())
            .collect::<Vec<_>>();
        record_batch_publication_result(&mut outcomes, indices, published)?;
        // These owned guards deliberately remain live through every routed
        // outcome above. Dropping them only after the physical cohort prevents
        // retention from deciding against an in-flight current publication.
        drop(current_guards);
        ordered_grouped_artifact_outcomes(outcomes)
    }

    async fn publish_many_inner(
        &self,
        requests: Vec<IndexArtifactPublish>,
        telemetry: &mut GroupedPublishTelemetry,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        let first = &requests[0];
        let identity = (
            first.storage_tenant.clone(),
            first.bucket.clone(),
            first.tenant_id,
            first.bucket_id,
            first.index_id,
        );
        let placement = self.require_local_builder(identity.2, identity.3, identity.4)?;
        let fence = placement.fence();
        let mut groups = BTreeMap::<(Vec<NodeId>, bool), Vec<(usize, IndexArtifactPublish)>>::new();
        for (index, request) in requests.into_iter().enumerate() {
            request.validate()?;
            if request.storage_tenant != identity.0
                || request.bucket != identity.1
                || request.tenant_id != identity.2
                || request.bucket_id != identity.3
            {
                return Err(Status::invalid_argument(
                    "one grouped publication cohort must share its governed bucket",
                ));
            }
            let candidate =
                self.require_local_builder(request.tenant_id, request.bucket_id, request.index_id)?;
            if candidate.fence() != fence {
                return Err(Status::unavailable(
                    "index placement changed while routing a publication cohort",
                ));
            }
            let key = request.key()?;
            let group = self.objects.object_replica_group_stable(
                &placement,
                &key,
                request.tenant_id,
                request.bucket_id,
            )?;
            groups
                .entry((
                    group.replicas().to_vec(),
                    request.admission.is_publication_progress(),
                ))
                .or_default()
                .push((index, request));
        }
        telemetry.groups = groups.len() as u64;
        let outcome_count = groups.values().map(Vec::len).sum();
        let mut batches = Vec::new();
        for ((replicas, _publication_progress), group) in groups {
            let coordinator = replicas[0];
            let address = if coordinator == self.local_node {
                None
            } else {
                Some(
                    placement
                        .address(coordinator)
                        .ok_or_else(|| {
                            Status::unavailable(format!(
                                "ACTIVE artifact coordinator {} has no peer address",
                                coordinator.0
                            ))
                        })?
                        .0
                        .clone(),
                )
            };
            for items in bounded_artifact_batches(group)? {
                batches.push(RoutedArtifactBatch {
                    coordinator,
                    address: address.clone(),
                    items,
                });
            }
        }
        let mut outcomes = std::iter::repeat_with(|| None)
            .take(outcome_count)
            .collect::<Vec<_>>();
        for batch in batches {
            telemetry.record_batch(batch.coordinator == self.local_node, &batch.items);
            let (indices, publications): (Vec<_>, Vec<_>) = batch.items.into_iter().unzip();
            let published = match self.require_fence(fence) {
                Ok(()) => {
                    if let Some(address) = batch.address {
                        self.peers
                            .publish_index_artifacts(
                                batch.coordinator,
                                &address,
                                fence,
                                &publications,
                            )
                            .await
                    } else {
                        self.coordinator
                            .publish_many(self.local_node, placement.clone(), publications)
                            .await
                    }
                }
                Err(error) => Err(error),
            };
            let published = match self.require_fence(fence) {
                Ok(()) => published,
                Err(error) => Err(error),
            };
            record_batch_publication_result(&mut outcomes, indices, published)?;
        }
        ordered_grouped_artifact_outcomes(outcomes)
    }
}

struct RoutedArtifactBatch {
    coordinator: NodeId,
    address: Option<String>,
    items: Vec<(usize, IndexArtifactPublish)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guarded_cohort(
        definition_replicas: Vec<NodeId>,
        current_replicas: Vec<NodeId>,
    ) -> GuardedIndexArtifactCohort {
        GuardedIndexArtifactCohort::test_key(definition_replicas, current_replicas)
    }

    fn request(index_id: u64) -> IndexArtifactPublish {
        IndexArtifactPublish {
            storage_tenant: "tenant".into(),
            bucket: "bucket".into(),
            tenant_id: 1,
            bucket_id: 2,
            index_id,
            exact_path: current_path(index_id),
            blob: BlobRef {
                hash: [index_id as u8; 32],
                length: 1,
            },
            expected_version: None,
            command_id: format!("guard-{index_id}"),
            definition_guard: None,
            definition_intent: None,
            admission: DerivedArtifactAdmission::Bounded,
        }
    }

    #[test]
    fn failed_definition_guard_is_indexed_without_poisoning_valid_items() {
        let mut outcomes = std::iter::repeat_with(|| None).take(3).collect::<Vec<_>>();
        let mut valid = Vec::new();
        record_definition_guard_outcome(&mut outcomes, &mut valid, 0, request(10), Ok(())).unwrap();
        record_definition_guard_outcome(
            &mut outcomes,
            &mut valid,
            1,
            request(11),
            Err(Status::failed_precondition("definition changed")),
        )
        .unwrap();
        record_definition_guard_outcome(&mut outcomes, &mut valid, 2, request(12), Ok(())).unwrap();

        assert_eq!(
            valid.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert!(outcomes[0].is_none());
        assert_eq!(
            outcomes[1].as_ref().unwrap().as_ref().unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert!(outcomes[2].is_none());
    }

    #[test]
    fn placement_failure_remains_batch_fatal() {
        let mut outcomes = std::iter::repeat_with(|| None).take(1).collect::<Vec<_>>();
        let error = record_definition_guard_outcome(
            &mut outcomes,
            &mut Vec::new(),
            0,
            request(10),
            Err(Status::unavailable("placement changed")),
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn guarded_cohort_key_batches_only_the_same_physical_routing_tuple() {
        let first = guarded_cohort(vec![NodeId(1), NodeId(2)], vec![NodeId(2), NodeId(3)]);
        let same = guarded_cohort(vec![NodeId(1), NodeId(2)], vec![NodeId(2), NodeId(3)]);
        let another_definition =
            guarded_cohort(vec![NodeId(2), NodeId(3)], vec![NodeId(2), NodeId(3)]);
        let another_current =
            guarded_cohort(vec![NodeId(1), NodeId(2)], vec![NodeId(1), NodeId(3)]);

        assert_eq!(first, same);
        assert_ne!(first, another_definition);
        assert_ne!(first, another_current);
    }

    #[test]
    fn later_physical_batch_error_preserves_an_earlier_success() {
        let mut outcomes = std::iter::repeat_with(|| None).take(3).collect::<Vec<_>>();
        record_batch_publication_result(
            &mut outcomes,
            vec![0],
            Ok(vec![Ok(IndexArtifactOutcome {
                version: VersionId(11),
                replayed: false,
            })]),
        )
        .unwrap();
        record_batch_publication_result(
            &mut outcomes,
            vec![1, 2],
            Err(Status::unavailable("later physical batch failed")),
        )
        .unwrap();

        let outcomes = ordered_grouped_artifact_outcomes(outcomes).unwrap();
        assert_eq!(outcomes[0].as_ref().unwrap().version, VersionId(11));
        assert!(
            outcomes[1..].iter().all(|outcome| {
                outcome.as_ref().unwrap_err().code() == tonic::Code::Unavailable
            })
        );
    }

    #[test]
    fn invalid_later_outcome_count_preserves_an_earlier_success() {
        let mut outcomes = std::iter::repeat_with(|| None).take(2).collect::<Vec<_>>();
        record_batch_publication_result(
            &mut outcomes,
            vec![0],
            Ok(vec![Ok(IndexArtifactOutcome {
                version: VersionId(11),
                replayed: false,
            })]),
        )
        .unwrap();
        record_batch_publication_result(&mut outcomes, vec![1], Ok(Vec::new())).unwrap();

        let outcomes = ordered_grouped_artifact_outcomes(outcomes).unwrap();
        assert_eq!(outcomes[0].as_ref().unwrap().version, VersionId(11));
        assert_eq!(
            outcomes[1].as_ref().unwrap_err().code(),
            tonic::Code::Internal
        );
    }
}
