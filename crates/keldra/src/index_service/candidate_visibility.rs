//! Mandatory Zanzibar validation for candidates already proven current by a
//! pinned immutable index snapshot.
//!
//! Format-v1 query plans call this boundary before admitting arbitrary-order
//! candidates to a top-K heap and while refilling a physically ordered page.
//! Keeping the operation inside the executor makes it impossible for another
//! query surface to omit authorization as optional post-processing. Liveness
//! and version currentness belong to the same pinned gate snapshot as the
//! postings; consulting a later object-store cut here would violate snapshot
//! semantics and add two redundant read batches per candidate wave.

use std::sync::Arc;

use keldra_api::v1::{IndexKind, IndexQueryHit};
use keldra_store::ObjectKey;
use tonic::Status;

use super::boundary::IndexAuthorization;
use crate::authentication::{Caller, PluginObjectScope};
use crate::authorization::ObjectPermission;
use crate::object_path_access;

// The authoritative authorization API accepts at most 1,000 checks. Preserve
// that bound here so query admission can use one full authorization round per
// authoritative batch instead of imposing a smaller, redundant subdivision.
pub(crate) const MAX_CANDIDATE_VISIBILITY_BATCH: usize = crate::authz_service::MAX_CHECKS;
const _: () = assert!(
    keldra_index::v1::MAX_QUERY_CANDIDATE_ADMISSION_BATCH <= MAX_CANDIDATE_VISIBILITY_BATCH
);

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexCandidateIdentity {
    /// Exact source identity retained by the immutable projection snapshot.
    pub(crate) source_path: String,
    /// Canonical source identity used at the authorization boundary. This is
    /// distinct from `source_path` when the indexed object was reached through
    /// a transparent alias.
    pub(crate) authorization_source_path: String,
    /// Current source version recorded by that same pinned gate.
    pub(crate) source_version: u64,
    /// Public object returned and Zanzibar-authorized if this candidate wins.
    pub(crate) result: IndexQueryHit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CandidateVisibilityEvidence {
    pub(crate) visible: Vec<bool>,
    pub(crate) authorization_revision: u64,
    pub(crate) denied: u64,
}

#[tonic::async_trait]
pub(crate) trait IndexCandidateVisibility: Send + Sync + 'static {
    async fn evaluate(
        &self,
        candidates: &[IndexCandidateIdentity],
    ) -> Result<CandidateVisibilityEvidence, Status>;
}

#[derive(Clone)]
pub(crate) struct AuthorizedSnapshotCandidates {
    caller: Caller,
    authorization_revision: u64,
    bucket: String,
    path_prefix: String,
    kind: IndexKind,
    plugin_scope: Option<PluginObjectScope>,
    authorization: Arc<dyn IndexAuthorization>,
}

impl AuthorizedSnapshotCandidates {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        caller: Caller,
        authorization_revision: u64,
        bucket: String,
        path_prefix: String,
        kind: IndexKind,
        plugin_scope: Option<PluginObjectScope>,
        authorization: Arc<dyn IndexAuthorization>,
    ) -> Self {
        Self {
            caller,
            authorization_revision,
            bucket,
            path_prefix,
            kind,
            plugin_scope,
            authorization,
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
            &candidate.authorization_source_path,
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
impl IndexCandidateVisibility for AuthorizedSnapshotCandidates {
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
            });
        }

        let mut checks = Vec::with_capacity(candidates.len());
        let mut capability_allowed = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let (source, result) = self.validate_candidate(candidate)?;
            capability_allowed
                .push(self.capability_allows(&source) && self.capability_allows(&result));
            let resource = if matches!(self.kind, IndexKind::GitSource | IndexKind::Tensor) {
                result
            } else {
                source
            };
            checks.push((resource, ObjectPermission::Get));
        }
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
        let visible = evidence
            .allowed
            .into_iter()
            .zip(capability_allowed)
            .map(|(authorized, capability)| authorized && capability)
            .collect::<Vec<_>>();
        let denied = u64::try_from(visible.iter().filter(|allowed| !**allowed).count())
            .map_err(|_| Status::resource_exhausted("candidate count exceeds u64"))?;
        Ok(CandidateVisibilityEvidence {
            visible,
            authorization_revision: evidence.revision,
            denied,
        })
    }
}

#[cfg(test)]
mod tests {
    use keldra_api::v1::ObjectAddress;
    use keldra_store::StorageTenantId;
    use std::sync::Mutex;

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

    fn candidate(path: &str) -> IndexCandidateIdentity {
        IndexCandidateIdentity {
            source_path: path.into(),
            authorization_source_path: path.into(),
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

    fn visibility() -> AuthorizedSnapshotCandidates {
        visibility_for(IndexKind::Path)
    }

    fn visibility_for(kind: IndexKind) -> AuthorizedSnapshotCandidates {
        AuthorizedSnapshotCandidates::new(
            Caller::from_authenticated_application(
                StorageTenantId::parse("tenant").unwrap(),
                "application",
            )
            .unwrap(),
            9,
            "objects".into(),
            "docs/".into(),
            kind,
            None,
            Arc::new(TestAuthorization),
        )
    }

    #[tokio::test]
    async fn authorization_filters_one_snapshot_current_batch() {
        let result = visibility()
            .evaluate(&[
                candidate("docs/live"),
                candidate("docs/denied"),
                candidate("docs/live-too"),
            ])
            .await
            .unwrap();

        assert_eq!(result.authorization_revision, 9);
        assert_eq!(result.visible, vec![true, false, true]);
        assert_eq!(result.denied, 1);
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
    async fn maximum_authoritative_batch_is_admitted_without_subdivision() {
        let candidates = vec![candidate("docs/live"); MAX_CANDIDATE_VISIBILITY_BATCH];

        let result = visibility().evaluate(&candidates).await.unwrap();

        assert_eq!(result.visible.len(), MAX_CANDIDATE_VISIBILITY_BATCH);
        assert!(result.visible.iter().all(|visible| *visible));
        assert_eq!(result.denied, 0);
    }

    #[tokio::test]
    async fn canonical_alias_target_cannot_bypass_reserved_path_policy() {
        let visibility = visibility();
        let mut aliased = candidate("docs/live");
        aliased.authorization_source_path = "_keldra/private-target".into();
        let result = visibility.evaluate(&[aliased]).await.unwrap();
        assert_eq!(result.visible, [false]);
        assert_eq!(result.denied, 1);
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
    async fn reference_projection_authorizes_the_distinct_result() {
        for kind in [IndexKind::GitSource, IndexKind::Tensor] {
            let mut live_candidate = candidate("docs/source.json");
            live_candidate.result.address.as_mut().unwrap().path = "payloads/referenced.bin".into();

            let result = visibility_for(kind)
                .evaluate(&[live_candidate])
                .await
                .unwrap();
            assert_eq!(result.visible, vec![true]);
            assert_eq!(result.authorization_revision, 9);
        }
    }

    #[tokio::test]
    async fn candidate_batches_do_not_repeat_definition_admission() {
        let authorization = Arc::new(RecordingAuthorization {
            seen: Mutex::new(Vec::new()),
        });
        let visibility = AuthorizedSnapshotCandidates::new(
            Caller::from_authenticated_application(
                StorageTenantId::parse("tenant").unwrap(),
                "application",
            )
            .unwrap(),
            9,
            "objects".into(),
            "docs/".into(),
            IndexKind::Path,
            None,
            authorization.clone(),
        );

        let mut aliased = candidate("docs/live");
        aliased.authorization_source_path = "canonical/live".into();
        visibility.evaluate(&[aliased]).await.unwrap();
        assert_eq!(
            *authorization.seen.lock().unwrap(),
            vec!["canonical/live".to_owned()]
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
