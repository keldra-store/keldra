//! Mandatory Zanzibar and exact-current validation for index candidates.
//!
//! Format-v4 query plans call this boundary before admitting arbitrary-order
//! candidates to a top-K heap and while refilling a physically ordered page.
//! Keeping the operation inside the executor makes it impossible for another
//! query surface to omit authorization or liveness as optional post-processing.

use std::sync::Arc;

use keldra_api::v1::{IndexKind, IndexQueryHit};
use keldra_store::ObjectKey;
use tonic::Status;

use super::boundary::{IndexAuthorization, IndexLiveVersionReader, ResolvedIndexCurrentSnapshot};
use crate::authentication::{Caller, PluginObjectScope};
use crate::authorization::ObjectPermission;
use crate::object_path_access;

pub(crate) const MAX_CANDIDATE_VISIBILITY_BATCH: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexCandidateIdentity {
    /// Ordinary source object whose current version controls this projection.
    pub(crate) source_path: String,
    pub(crate) source_version: u64,
    /// Public object returned and Zanzibar-authorized if this candidate wins.
    pub(crate) result: IndexQueryHit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CandidateVisibilityEvidence {
    pub(crate) visible: Vec<bool>,
    pub(crate) authorization_revision: u64,
    pub(crate) denied: u64,
    pub(crate) stale: u64,
}

#[tonic::async_trait]
pub(crate) trait IndexCandidateVisibility: Send + Sync + 'static {
    async fn evaluate(
        &self,
        candidates: &[IndexCandidateIdentity],
    ) -> Result<CandidateVisibilityEvidence, Status>;
}

#[derive(Clone)]
pub(crate) struct AuthorizedCurrentCandidates {
    caller: Caller,
    authorization_revision: u64,
    bucket: String,
    path_prefix: String,
    kind: IndexKind,
    tenant_id: u64,
    bucket_id: u64,
    deadline: tokio::time::Instant,
    plugin_scope: Option<PluginObjectScope>,
    authorization: Arc<dyn IndexAuthorization>,
    live_versions: Arc<dyn IndexLiveVersionReader>,
}

impl AuthorizedCurrentCandidates {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        caller: Caller,
        authorization_revision: u64,
        bucket: String,
        path_prefix: String,
        kind: IndexKind,
        tenant_id: u64,
        bucket_id: u64,
        deadline: tokio::time::Instant,
        plugin_scope: Option<PluginObjectScope>,
        authorization: Arc<dyn IndexAuthorization>,
        live_versions: Arc<dyn IndexLiveVersionReader>,
    ) -> Self {
        Self {
            caller,
            authorization_revision,
            bucket,
            path_prefix,
            kind,
            tenant_id,
            bucket_id,
            deadline,
            plugin_scope,
            authorization,
            live_versions,
        }
    }

    fn validate_candidate(
        &self,
        candidate: &IndexCandidateIdentity,
    ) -> Result<(ObjectKey, ObjectKey), Status> {
        let address = candidate
            .result
            .address
            .as_ref()
            .ok_or_else(|| Status::data_loss("index candidate has no object address"))?;
        let references_another_object =
            matches!(self.kind, IndexKind::GitSource | IndexKind::Tensor);
        if candidate.source_version == 0
            || candidate.result.object_version == 0
            || candidate
                .result
                .score
                .is_some_and(|score| !score.is_finite())
            || !super::path_matches_prefix(&candidate.source_path, &self.path_prefix)
            || candidate
                .source_path
                .split('/')
                .any(|segment| segment == "_keldra")
            || address.tenant != self.caller.storage_tenant().as_str()
            || address.bucket != self.bucket
            || (!references_another_object
                && !super::path_matches_prefix(&address.path, &self.path_prefix))
            || address.path.split('/').any(|segment| segment == "_keldra")
        {
            return Err(Status::data_loss(
                "index candidate is invalid or outside the definition scope",
            ));
        }
        let result = ObjectKey::new(&address.tenant, &address.bucket, &address.path)
            .map_err(|_| Status::data_loss("index candidate has an invalid result address"))?;
        let source = ObjectKey::new(
            self.caller.storage_tenant().as_str(),
            &self.bucket,
            &candidate.source_path,
        )
        .map_err(|_| Status::data_loss("index candidate has an invalid source address"))?;
        Ok((source, result))
    }

    fn capability_allows(&self, key: &ObjectKey) -> bool {
        object_path_access::require_public_key(key).is_ok()
            && self
                .plugin_scope
                .as_ref()
                .is_none_or(|scope| scope.allows(key.tenant(), key.bucket(), key.path()))
    }
}

#[tonic::async_trait]
impl IndexCandidateVisibility for AuthorizedCurrentCandidates {
    async fn evaluate(
        &self,
        candidates: &[IndexCandidateIdentity],
    ) -> Result<CandidateVisibilityEvidence, Status> {
        if candidates.len() > MAX_CANDIDATE_VISIBILITY_BATCH {
            return Err(Status::resource_exhausted(
                "index candidate visibility batch exceeds its bound",
            ));
        }
        if self.authorization_revision == 0 {
            return Err(Status::data_loss(
                "index candidate visibility has no Zanzibar admission revision",
            ));
        }
        if candidates.is_empty() {
            return Ok(CandidateVisibilityEvidence {
                visible: Vec::new(),
                authorization_revision: self.authorization_revision,
                denied: 0,
                stale: 0,
            });
        }

        let mut sources = Vec::with_capacity(candidates.len());
        let mut results = Vec::with_capacity(candidates.len());
        let mut capability_allowed = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let (source, result) = self.validate_candidate(candidate)?;
            capability_allowed
                .push(self.capability_allows(&source) && self.capability_allows(&result));
            sources.push(source);
            results.push(result);
        }
        let source_snapshots = self
            .live_versions
            .resolved_current_snapshots(
                &sources,
                self.tenant_id,
                self.bucket_id,
                crate::v05::deadline_remaining(self.deadline)?,
            )
            .await?;
        let result_snapshots = if results == sources {
            source_snapshots.clone()
        } else {
            self.live_versions
                .resolved_current_snapshots(
                    &results,
                    self.tenant_id,
                    self.bucket_id,
                    crate::v05::deadline_remaining(self.deadline)?,
                )
                .await?
        };
        if source_snapshots.len() != candidates.len() || result_snapshots.len() != candidates.len()
        {
            return Err(Status::data_loss(
                "resolved current object batch returned the wrong result count",
            ));
        }
        for ((allowed, source), result) in capability_allowed
            .iter_mut()
            .zip(&source_snapshots)
            .zip(&result_snapshots)
        {
            *allowed &= self.capability_allows(&source.canonical)
                && self.capability_allows(&result.canonical);
        }
        let checks = result_snapshots
            .iter()
            .map(|resolved| (resolved.canonical.clone(), ObjectPermission::Get))
            .collect::<Vec<_>>();
        let evidence = self
            .authorization
            .allows_objects_with_evidence(&self.caller, &checks)
            .await?;
        if evidence.revision == 0 || evidence.allowed.len() != checks.len() {
            return Err(Status::data_loss(
                "Zanzibar returned invalid index authorization evidence",
            ));
        }
        if evidence.revision != self.authorization_revision {
            return Err(Status::failed_precondition(
                "authorization revision changed during index execution",
            ));
        }
        let mut visible = evidence
            .allowed
            .into_iter()
            .zip(capability_allowed)
            .map(|(authorized, capability)| authorized && capability)
            .collect::<Vec<_>>();
        let denied = u64::try_from(visible.iter().filter(|allowed| !**allowed).count())
            .map_err(|_| Status::resource_exhausted("candidate count exceeds u64"))?;
        let (authorized_positions, authorized_keys) = retain_authorized_sources(&visible, sources);
        if authorized_keys.is_empty() {
            return Ok(CandidateVisibilityEvidence {
                visible,
                authorization_revision: evidence.revision,
                denied,
                stale: 0,
            });
        }
        let snapshots = authorized_positions
            .iter()
            .map(|position| source_snapshots[*position].clone())
            .collect();
        let mut stale = apply_current_snapshots(
            &mut visible,
            &authorized_positions,
            &authorized_keys,
            snapshots,
            self.tenant_id,
            self.bucket_id,
            |position| candidates[position].source_version,
        )?;

        let result_positions = (0..candidates.len()).collect::<Vec<_>>();
        stale = stale.saturating_add(apply_current_snapshots(
            &mut visible,
            &result_positions,
            &results,
            result_snapshots,
            self.tenant_id,
            self.bucket_id,
            |position| candidates[position].result.object_version,
        )?);
        Ok(CandidateVisibilityEvidence {
            visible,
            authorization_revision: evidence.revision,
            denied,
            stale,
        })
    }
}

fn apply_current_snapshots(
    visible: &mut [bool],
    positions: &[usize],
    keys: &[ObjectKey],
    snapshots: Vec<ResolvedIndexCurrentSnapshot>,
    tenant_id: u64,
    bucket_id: u64,
    mut expected_version: impl FnMut(usize) -> u64,
) -> Result<u64, Status> {
    if positions.len() != keys.len() || snapshots.len() != positions.len() {
        return Err(Status::data_loss(
            "current object batch returned the wrong result count",
        ));
    }
    let mut stale = 0_u64;
    for ((position, key), resolved) in positions.iter().copied().zip(keys).zip(snapshots) {
        if !visible[position] {
            continue;
        }
        let expected_version = expected_version(position);
        let Some(snapshot) = resolved.snapshot else {
            visible[position] = false;
            stale = stale.saturating_add(1);
            continue;
        };
        snapshot
            .validate()
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if snapshot.tenant_id != tenant_id
            || snapshot.bucket_id != bucket_id
            || resolved.canonical.tenant() != key.tenant()
            || resolved.canonical.bucket() != key.bucket()
            || snapshot.exact_path != resolved.canonical.path()
        {
            return Err(Status::data_loss(format!(
                "current object batch returned another identity at position {position}"
            )));
        }
        if snapshot.head.deleted
            || snapshot.version.deleted
            || snapshot.head.version.0 != expected_version
            || snapshot.version.id.0 != expected_version
        {
            visible[position] = false;
            stale = stale.saturating_add(1);
        }
    }
    Ok(stale)
}

/// Retain authorized source keys in their existing allocation and separately
/// preserve their positions in the candidate wave. The subsequent exact-head
/// read therefore owns one source key per authorized candidate, not two cloned
/// key vectors.
fn retain_authorized_sources(
    visible: &[bool],
    mut sources: Vec<ObjectKey>,
) -> (Vec<usize>, Vec<ObjectKey>) {
    debug_assert_eq!(visible.len(), sources.len());
    let mut positions = Vec::with_capacity(visible.iter().filter(|allowed| **allowed).count());
    let mut position = 0usize;
    sources.retain(|_| {
        let authorized = visible[position];
        if authorized {
            positions.push(position);
        }
        position += 1;
        authorized
    });
    (positions, sources)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use keldra_api::v1::ObjectAddress;
    use keldra_store::{BlobRef, CurrentObjectSnapshot, Head, StorageTenantId, Version, VersionId};

    use super::*;
    use crate::index_service::IndexAuthorizationEvidence;

    struct TestAuthorization;

    #[tonic::async_trait]
    impl IndexAuthorization for TestAuthorization {
        async fn allows_objects_with_evidence(
            &self,
            _caller: &Caller,
            requests: &[(ObjectKey, ObjectPermission)],
        ) -> Result<IndexAuthorizationEvidence, Status> {
            Ok(IndexAuthorizationEvidence {
                allowed: requests
                    .iter()
                    .map(|(key, _)| key.path() != "docs/denied")
                    .collect(),
                revision: 9,
            })
        }
    }

    struct RecordingAuthorization {
        seen: Mutex<Vec<String>>,
    }

    #[tonic::async_trait]
    impl IndexAuthorization for RecordingAuthorization {
        async fn allows_objects_with_evidence(
            &self,
            _caller: &Caller,
            requests: &[(ObjectKey, ObjectPermission)],
        ) -> Result<IndexAuthorizationEvidence, Status> {
            *self.seen.lock().unwrap() = requests
                .iter()
                .map(|(key, _)| key.path().to_owned())
                .collect();
            Ok(IndexAuthorizationEvidence {
                allowed: vec![true; requests.len()],
                revision: 9,
            })
        }
    }

    struct TestLiveVersions;

    #[tonic::async_trait]
    impl IndexLiveVersionReader for TestLiveVersions {
        async fn resolved_current_snapshots(
            &self,
            keys: &[ObjectKey],
            _tenant_id: u64,
            _bucket_id: u64,
            _budget: Duration,
        ) -> Result<Vec<ResolvedIndexCurrentSnapshot>, Status> {
            Ok(keys
                .iter()
                .map(|key| {
                    let version = if matches!(key.path(), "docs/stale" | "payloads/stale.bin") {
                        2
                    } else {
                        1
                    };
                    ResolvedIndexCurrentSnapshot {
                        canonical: key.clone(),
                        snapshot: Some(snapshot(key.path(), version)),
                    }
                })
                .collect())
        }
    }

    struct RecordingLiveVersions {
        batches: Mutex<Vec<Vec<String>>>,
    }

    struct ReservedCanonicalLiveVersions;

    #[tonic::async_trait]
    impl IndexLiveVersionReader for ReservedCanonicalLiveVersions {
        async fn resolved_current_snapshots(
            &self,
            keys: &[ObjectKey],
            _tenant_id: u64,
            _bucket_id: u64,
            _budget: Duration,
        ) -> Result<Vec<ResolvedIndexCurrentSnapshot>, Status> {
            Ok(keys
                .iter()
                .map(|key| {
                    let canonical =
                        ObjectKey::new(key.tenant(), key.bucket(), "_keldra/private-target")
                            .unwrap();
                    ResolvedIndexCurrentSnapshot {
                        snapshot: Some(snapshot(canonical.path(), 1)),
                        canonical,
                    }
                })
                .collect())
        }
    }

    #[tonic::async_trait]
    impl IndexLiveVersionReader for RecordingLiveVersions {
        async fn resolved_current_snapshots(
            &self,
            keys: &[ObjectKey],
            _tenant_id: u64,
            _bucket_id: u64,
            _budget: Duration,
        ) -> Result<Vec<ResolvedIndexCurrentSnapshot>, Status> {
            self.batches
                .lock()
                .unwrap()
                .push(keys.iter().map(|key| key.path().to_owned()).collect());
            Ok(keys
                .iter()
                .map(|key| ResolvedIndexCurrentSnapshot {
                    canonical: key.clone(),
                    snapshot: Some(snapshot(key.path(), 1)),
                })
                .collect())
        }
    }

    fn snapshot(path: &str, version_id: u64) -> CurrentObjectSnapshot {
        let version = Version {
            id: VersionId(version_id),
            blob: Some(BlobRef {
                hash: [1; 32],
                length: 1,
            }),
            content_type: Some("application/octet-stream".into()),
            deleted: false,
            committed_at_unix_millis: 1,
            protected_link_descriptor: false,
        };
        CurrentObjectSnapshot {
            tenant_id: 11,
            bucket_id: 12,
            exact_path: path.into(),
            head: Head {
                version: version.id,
                deleted: false,
                mutation_stamp: None,
            },
            version,
            alias_registry: None,
        }
    }

    fn candidate(path: &str) -> IndexCandidateIdentity {
        IndexCandidateIdentity {
            source_path: path.into(),
            source_version: 1,
            result: IndexQueryHit {
                address: Some(ObjectAddress {
                    tenant: "tenant".into(),
                    bucket: "objects".into(),
                    path: path.into(),
                }),
                object_version: 1,
                score: None,
            },
        }
    }

    fn visibility() -> AuthorizedCurrentCandidates {
        visibility_for(IndexKind::Path)
    }

    fn visibility_for(kind: IndexKind) -> AuthorizedCurrentCandidates {
        AuthorizedCurrentCandidates::new(
            Caller::from_authenticated_application(
                StorageTenantId::parse("tenant").unwrap(),
                "application",
            )
            .unwrap(),
            9,
            "objects".into(),
            "docs/".into(),
            kind,
            11,
            12,
            tokio::time::Instant::now() + Duration::from_secs(5),
            None,
            Arc::new(TestAuthorization),
            Arc::new(TestLiveVersions),
        )
    }

    #[tokio::test]
    async fn authorization_and_exact_current_filter_one_bounded_batch() {
        let result = visibility()
            .evaluate(&[
                candidate("docs/live"),
                candidate("docs/denied"),
                candidate("docs/stale"),
            ])
            .await
            .unwrap();

        assert_eq!(result.authorization_revision, 9);
        assert_eq!(result.visible, vec![true, false, false]);
        assert_eq!(result.denied, 1);
        assert_eq!(result.stale, 1);
    }

    #[tokio::test]
    async fn malformed_scope_and_oversized_batches_fail_closed() {
        assert_eq!(
            visibility()
                .evaluate(&[candidate("outside")])
                .await
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
        let oversized = vec![candidate("docs/live"); MAX_CANDIDATE_VISIBILITY_BATCH + 1];
        assert_eq!(
            visibility().evaluate(&oversized).await.unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[tokio::test]
    async fn public_candidate_cannot_resolve_through_alias_to_reserved_target() {
        let mut visibility = visibility();
        visibility.live_versions = Arc::new(ReservedCanonicalLiveVersions);
        let result = visibility
            .evaluate(&[candidate("docs/live")])
            .await
            .unwrap();
        assert_eq!(result.visible, [false]);
        assert_eq!(result.denied, 1);
    }

    #[test]
    fn authorized_source_selection_moves_keys_without_cloning_paths() {
        let sources = vec![
            ObjectKey::new("tenant", "objects", "docs/first").unwrap(),
            ObjectKey::new("tenant", "objects", "docs/denied").unwrap(),
            ObjectKey::new("tenant", "objects", "docs/last").unwrap(),
        ];
        let retained_path_pointers = [sources[0].path().as_ptr(), sources[2].path().as_ptr()];

        let (positions, retained) = retain_authorized_sources(&[true, false, true], sources);

        assert_eq!(positions, [0, 2]);
        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].path().as_ptr(), retained_path_pointers[0]);
        assert_eq!(retained[1].path().as_ptr(), retained_path_pointers[1]);
    }

    #[tokio::test]
    async fn admission_revision_is_retained_for_empty_batches_and_pins_later_checks() {
        let visibility = visibility();
        assert_eq!(
            visibility.evaluate(&[]).await.unwrap(),
            CandidateVisibilityEvidence {
                visible: Vec::new(),
                authorization_revision: 9,
                denied: 0,
                stale: 0,
            }
        );

        let mut changed = visibility;
        changed.authorization_revision = 8;
        assert_eq!(
            changed
                .evaluate(&[candidate("docs/live")])
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn reference_projection_requires_current_source_and_distinct_result() {
        for kind in [IndexKind::GitSource, IndexKind::Tensor] {
            let mut live_candidate = candidate("docs/source.json");
            live_candidate.result.address.as_mut().unwrap().path = "payloads/referenced.bin".into();

            let result = visibility_for(kind)
                .evaluate(&[live_candidate])
                .await
                .unwrap();
            assert_eq!(result.visible, vec![true]);
            assert_eq!(result.authorization_revision, 9);

            let mut stale_source = candidate("docs/stale");
            stale_source.result.address.as_mut().unwrap().path = "payloads/referenced.bin".into();
            let result = visibility_for(kind)
                .evaluate(&[stale_source])
                .await
                .unwrap();
            assert_eq!(result.visible, vec![false]);
            assert_eq!(result.stale, 1);

            let mut stale_result = candidate("docs/source.json");
            stale_result.result.address.as_mut().unwrap().path = "payloads/stale.bin".into();
            let result = visibility_for(kind)
                .evaluate(&[stale_result])
                .await
                .unwrap();
            assert_eq!(result.visible, vec![false]);
            assert_eq!(result.stale, 1);
        }
    }

    #[tokio::test]
    async fn identical_source_and_result_identity_needs_one_current_read() {
        let live_versions = Arc::new(RecordingLiveVersions {
            batches: Mutex::new(Vec::new()),
        });
        let visibility = AuthorizedCurrentCandidates::new(
            Caller::from_authenticated_application(
                StorageTenantId::parse("tenant").unwrap(),
                "application",
            )
            .unwrap(),
            9,
            "objects".into(),
            "docs/".into(),
            IndexKind::Path,
            11,
            12,
            tokio::time::Instant::now() + Duration::from_secs(5),
            None,
            Arc::new(TestAuthorization),
            live_versions.clone(),
        );

        let result = visibility
            .evaluate(&[candidate("docs/live")])
            .await
            .unwrap();
        assert_eq!(result.visible, vec![true]);
        assert_eq!(
            *live_versions.batches.lock().unwrap(),
            vec![vec!["docs/live".to_owned()]]
        );
    }

    #[tokio::test]
    async fn candidate_batches_do_not_repeat_definition_admission() {
        let authorization = Arc::new(RecordingAuthorization {
            seen: Mutex::new(Vec::new()),
        });
        let visibility = AuthorizedCurrentCandidates::new(
            Caller::from_authenticated_application(
                StorageTenantId::parse("tenant").unwrap(),
                "application",
            )
            .unwrap(),
            9,
            "objects".into(),
            "docs/".into(),
            IndexKind::Path,
            11,
            12,
            tokio::time::Instant::now() + Duration::from_secs(5),
            None,
            authorization.clone(),
            Arc::new(TestLiveVersions),
        );

        visibility
            .evaluate(&[candidate("docs/live")])
            .await
            .unwrap();
        assert_eq!(
            *authorization.seen.lock().unwrap(),
            vec!["docs/live".to_owned()]
        );
    }

    #[test]
    fn referenced_results_remain_tenant_bucket_and_namespace_scoped() {
        let visibility = visibility_for(IndexKind::GitSource);
        let mut wrong_tenant = candidate("docs/source.json");
        wrong_tenant.result.address.as_mut().unwrap().tenant = "another".into();
        let mut wrong_bucket = candidate("docs/source.json");
        wrong_bucket.result.address.as_mut().unwrap().bucket = "another".into();
        let mut reserved = candidate("docs/source.json");
        reserved.result.address.as_mut().unwrap().path = "payloads/_keldra/private".into();

        for invalid in [wrong_tenant, wrong_bucket, reserved] {
            assert_eq!(
                visibility.validate_candidate(&invalid).unwrap_err().code(),
                tonic::Code::DataLoss
            );
        }
    }
}
