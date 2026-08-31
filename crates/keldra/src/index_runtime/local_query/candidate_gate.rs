use super::*;

pub(super) struct RuntimeCandidateGate {
    pub(super) storage_tenant: String,
    pub(super) bucket: String,
    pub(super) visibility: Arc<dyn IndexCandidateVisibility>,
    pub(super) statistics: NativeQueryStatisticsRecorder,
    pub(super) projection: Option<ProjectionCandidateGate>,
}

pub(super) struct ProjectionCandidateGate {
    pub(super) publisher: IndexCommitPublisher,
    pub(super) generation: keldra_index::v5::ProjectionGeneration,
    pub(super) source_scope: [u8; 32],
    pub(super) tenant_id: u64,
    pub(super) bucket_id: u64,
}

impl RuntimeCandidateGate {
    pub(super) fn candidate_batch_limit(&self, native_default: usize) -> usize {
        if self.projection.is_some() {
            native_default.min(32)
        } else {
            native_default
        }
    }

    pub(super) fn working_memory_bytes(&self, batch: usize) -> Result<usize, IndexError> {
        runtime_gate_envelope_bytes(
            batch,
            self.storage_tenant.len(),
            self.bucket.len(),
            self.projection.is_some(),
        )
    }
}

/// Additional outer-runtime state retained while one native candidate batch is
/// authorized and checked against exact-current heads. The native executor
/// already charges its pending candidates and `CandidateReference`s; this
/// charge covers the concrete API candidate, object-key, evidence, and snapshot
/// representations created by Keldra around that boundary.
pub(super) fn runtime_gate_envelope_bytes(
    batch: usize,
    tenant_bytes: usize,
    bucket_bytes: usize,
    projection: bool,
) -> Result<usize, IndexError> {
    if tenant_bytes > MAX_OBJECT_TENANT_BYTES || bucket_bytes > MAX_OBJECT_BUCKET_BYTES {
        return Err(IndexError::InvalidQuery(
            "candidate gate scope exceeds object-name bounds".into(),
        ));
    }
    let path_bytes = MAX_OBJECT_PATH_BYTES;
    let object_key_dynamic = tenant_bytes
        .checked_add(bucket_bytes)
        .and_then(|bytes| bytes.checked_add(path_bytes))
        .ok_or(IndexError::OffsetOverflow)?;
    let candidate = std::mem::size_of::<IndexCandidateIdentity>()
        .checked_add(
            path_bytes
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(tenant_bytes))
                .and_then(|bytes| bytes.checked_add(bucket_bytes))
                .ok_or(IndexError::OffsetOverflow)?,
        )
        .ok_or(IndexError::OffsetOverflow)?;
    let authorization_phase = std::mem::size_of::<ObjectKey>()
        .checked_add(std::mem::size_of::<(ObjectKey, ObjectPermission)>())
        .and_then(|bytes| bytes.checked_add(object_key_dynamic.checked_mul(2)?))
        .ok_or(IndexError::OffsetOverflow)?;
    let current_phase = std::mem::size_of::<ObjectKey>()
        .checked_add(std::mem::size_of::<usize>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Option<CurrentObjectSnapshot>>()))
        .and_then(|bytes| bytes.checked_add(object_key_dynamic))
        .and_then(|bytes| bytes.checked_add(path_bytes))
        .and_then(|bytes| bytes.checked_add(MAX_CONTENT_TYPE_BYTES))
        .ok_or(IndexError::OffsetOverflow)?;
    let mut per_candidate = candidate
        .checked_add(std::mem::size_of::<bool>())
        .and_then(|bytes| bytes.checked_add(authorization_phase.max(current_phase)))
        .ok_or(IndexError::OffsetOverflow)?;
    if projection {
        // The component lookup retains encoded head bytes while decoding the
        // selected head. Charge both representations at the format bound; the
        // query budget therefore remains a hard limit even for hostile packs.
        per_candidate = per_candidate
            .checked_add(
                keldra_index::v4::INDEX_COMPONENT_BYTES
                    .checked_mul(2)
                    .ok_or(IndexError::OffsetOverflow)?,
            )
            .ok_or(IndexError::OffsetOverflow)?;
    }
    // Candidate/source/check/evidence in the authorization phase; candidate/
    // source/evidence/positions/snapshots in the exact-current phase.
    let vector_headers = 5usize
        .checked_mul(std::mem::size_of::<Vec<()>>())
        .ok_or(IndexError::OffsetOverflow)?;
    batch
        .checked_mul(per_candidate)
        .and_then(|bytes| bytes.checked_add(vector_headers))
        .ok_or(IndexError::OffsetOverflow)
}

impl CandidateGate for RuntimeCandidateGate {
    type Error = Status;

    fn evaluate(
        &self,
        candidates: &[CandidateReference],
    ) -> impl std::future::Future<Output = Result<CandidateGateEvidence, Self::Error>> + Send {
        async move {
            if let Some(projection) = &self.projection {
                return self.evaluate_projection(projection, candidates).await;
            }
            let references = candidates.to_vec();
            let candidates = candidates
                .iter()
                .map(|candidate| IndexCandidateIdentity {
                    source_path: candidate.source.path.clone(),
                    source_version: candidate.source.version,
                    result: IndexQueryHit {
                        address: Some(ObjectAddress {
                            tenant: self.storage_tenant.clone(),
                            bucket: self.bucket.clone(),
                            path: candidate.result.path.clone(),
                        }),
                        object_version: candidate.result.version,
                        score: None,
                    },
                })
                .collect::<Vec<_>>();
            let started = std::time::Instant::now();
            let result = self.visibility.evaluate(&candidates).await;
            self.statistics
                .phase_elapsed(NativeQueryPhase::CandidateVisibility, started.elapsed());
            let CandidateVisibilityEvidence {
                visible,
                authorization_revision,
                denied,
                stale,
            } = result?;
            let resolved = references
                .into_iter()
                .zip(visible)
                .map(|(candidate, visible)| visible.then_some(candidate))
                .collect();
            Ok(CandidateGateEvidence {
                resolved,
                authorization_revision,
                denied,
                stale,
            })
        }
    }
}

impl RuntimeCandidateGate {
    async fn evaluate_projection(
        &self,
        projection: &ProjectionCandidateGate,
        candidates: &[CandidateReference],
    ) -> Result<CandidateGateEvidence, Status> {
        use keldra_index::v5::{ComponentIdentity, StableDocumentKey, decode_document_head};

        let mut stable_keys = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            stable_keys.push(
                StableDocumentKey::from_query_cache_identity(&candidate.source)
                    .map_err(index_status)?,
            );
        }
        stable_keys.sort_unstable();
        stable_keys.dedup();
        let encoded = projection
            .publisher
            .load_projection_component_records(
                &self.storage_tenant,
                &self.bucket,
                projection.tenant_id,
                projection.bucket_id,
                &projection.generation,
                ComponentIdentity::DocumentHead,
                &stable_keys,
            )
            .await?;

        let mut exact = Vec::with_capacity(candidates.len());
        let mut visible_input = Vec::with_capacity(candidates.len());
        let mut visible_positions = Vec::with_capacity(candidates.len());
        let mut projection_stale = 0_u64;
        for (position, candidate) in candidates.iter().enumerate() {
            let stable_key = StableDocumentKey::from_query_cache_identity(&candidate.source)
                .map_err(index_status)?;
            let Some(bytes) = encoded
                .get(&stable_key)
                .and_then(|record| record.as_deref())
            else {
                exact.push(None);
                projection_stale = projection_stale.saturating_add(1);
                continue;
            };
            let head = decode_document_head(projection.source_scope, stable_key, bytes)
                .map_err(index_status)?;
            let Some(reference) = projection_reference(candidate, &head) else {
                exact.push(None);
                projection_stale = projection_stale.saturating_add(1);
                continue;
            };
            let result = reference.result.clone();
            visible_input.push(IndexCandidateIdentity {
                source_path: reference.source.path.clone(),
                source_version: reference.source.version,
                result: IndexQueryHit {
                    address: Some(ObjectAddress {
                        tenant: self.storage_tenant.clone(),
                        bucket: self.bucket.clone(),
                        path: result.path,
                    }),
                    object_version: result.version,
                    score: None,
                },
            });
            visible_positions.push(position);
            exact.push(Some(reference));
        }

        let started = std::time::Instant::now();
        let result = self.visibility.evaluate(&visible_input).await;
        self.statistics
            .phase_elapsed(NativeQueryPhase::CandidateVisibility, started.elapsed());
        let CandidateVisibilityEvidence {
            visible,
            authorization_revision,
            denied,
            stale,
        } = result?;
        if visible.len() != visible_positions.len() {
            return Err(Status::data_loss(
                "candidate visibility returned an unaligned projection result",
            ));
        }
        for (position, visible) in visible_positions.into_iter().zip(visible) {
            if !visible {
                exact[position] = None;
            }
        }
        Ok(CandidateGateEvidence {
            resolved: exact,
            authorization_revision,
            denied,
            stale: stale.saturating_add(projection_stale),
        })
    }
}

fn projection_reference(
    candidate: &CandidateReference,
    head: &keldra_index::v5::DocumentHead,
) -> Option<CandidateReference> {
    if !head.live || head.material_source_version != candidate.source.version {
        return None;
    }
    Some(CandidateReference {
        source: keldra_index::v4::ObjectIdentity {
            path: head.source_path.clone(),
            version: head.source_version,
        },
        result: head.result_or_source(),
    })
}

#[cfg(test)]
mod tests {
    use keldra_index::v4::ObjectIdentity;
    use keldra_index::v5::DocumentHead;

    use super::*;

    #[test]
    fn stable_cache_candidate_resolves_the_newest_projection_preserving_version() {
        let scope = [7; 32];
        let mut head = DocumentHead::new(
            scope,
            "objects/source".into(),
            0,
            19,
            Some(ObjectIdentity {
                path: "objects/result".into(),
                version: 23,
            }),
            true,
        )
        .unwrap();
        head.material_source_version = 7;
        let cache = head.stable_key.query_cache_identity(7).unwrap();
        let candidate = CandidateReference {
            source: cache.clone(),
            result: cache,
        };

        let resolved = projection_reference(&candidate, &head).unwrap();
        assert_eq!(resolved.source.path, "objects/source");
        assert_eq!(resolved.source.version, 19);
        assert_eq!(resolved.result.path, "objects/result");
        assert_eq!(resolved.result.version, 23);
    }

    #[test]
    fn material_change_or_tombstone_rejects_the_old_cache_candidate() {
        let scope = [8; 32];
        let mut head =
            DocumentHead::new(scope, "objects/source".into(), 0, 19, None, true).unwrap();
        head.material_source_version = 11;
        let old = head.stable_key.query_cache_identity(7).unwrap();
        let candidate = CandidateReference {
            source: old.clone(),
            result: old,
        };
        assert!(projection_reference(&candidate, &head).is_none());
        head.material_source_version = 7;
        head.live = false;
        assert!(projection_reference(&candidate, &head).is_none());
    }
}
