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

use keldra_api::v1::index_result_authorization::Policy as ApiResultAuthorizationPolicy;
use keldra_api::v1::{IndexAuthorizationTarget, IndexDefinition, IndexKind, IndexQueryHit};
use keldra_authz::{ExactPath, ObjectRef, RealmId};
use keldra_store::{AuthzScope, ObjectKey, StorageTenantId};
use tonic::Status;

use super::boundary::{
    IndexAuthorization, IndexQueryAuthorizationEvidence, IndexResultAuthorizationPolicy,
    RealmIndexAuthorizationTarget, RealmIndexResultAuthorizationPolicy,
};
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
    tenant_id: u64,
    admission: IndexQueryAuthorizationEvidence,
    result_policy: IndexResultAuthorizationPolicy,
    authorization_subject: Option<ObjectRef>,
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
        tenant_id: u64,
        admission: IndexQueryAuthorizationEvidence,
        result_policy: IndexResultAuthorizationPolicy,
        authorization_subject: Option<ObjectRef>,
        bucket: String,
        path_prefix: String,
        kind: IndexKind,
        plugin_scope: Option<PluginObjectScope>,
        authorization: Arc<dyn IndexAuthorization>,
    ) -> Self {
        Self {
            caller,
            tenant_id,
            admission,
            result_policy,
            authorization_subject,
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

    fn realm_resource(
        &self,
        policy: &RealmIndexResultAuthorizationPolicy,
        source: &ObjectKey,
        result: &ObjectKey,
    ) -> Result<ObjectRef, Status> {
        let target = match policy.target {
            RealmIndexAuthorizationTarget::CanonicalSourcePath => source,
            RealmIndexAuthorizationTarget::ResultPath => result,
        };
        ObjectRef::exact_path(
            policy.resource_namespace.clone(),
            ExactPath::new(target.tenant(), target.bucket(), target.path())
                .map_err(crate::authz_api::authz_status)?,
        )
        .map_err(crate::authz_api::authz_status)
    }
}

pub(crate) fn result_authorization_policy(
    definition: &IndexDefinition,
) -> Result<IndexResultAuthorizationPolicy, Status> {
    let policy = definition
        .result_authorization
        .as_ref()
        .and_then(|authorization| authorization.policy.as_ref())
        .ok_or_else(|| Status::data_loss("index definition has no result authorization policy"))?;
    match policy {
        ApiResultAuthorizationPolicy::Application(_) => {
            Ok(IndexResultAuthorizationPolicy::Application)
        }
        ApiResultAuthorizationPolicy::Realm(policy) => {
            RealmId::custom(&policy.realm).map_err(crate::authz_api::authz_status)?;
            ObjectRef::opaque(&policy.resource_namespace, "validation")
                .map_err(crate::authz_api::authz_status)?;
            keldra_authz::UsersetRef::new(
                ObjectRef::opaque(&policy.resource_namespace, "validation")
                    .map_err(crate::authz_api::authz_status)?,
                &policy.relation,
            )
            .map_err(crate::authz_api::authz_status)?;
            let target = match IndexAuthorizationTarget::try_from(policy.target) {
                Ok(IndexAuthorizationTarget::CanonicalSourcePath) => {
                    RealmIndexAuthorizationTarget::CanonicalSourcePath
                }
                Ok(IndexAuthorizationTarget::ResultPath) => {
                    RealmIndexAuthorizationTarget::ResultPath
                }
                Ok(IndexAuthorizationTarget::Unspecified) | Err(_) => {
                    return Err(Status::data_loss(
                        "index definition has an invalid result authorization target",
                    ));
                }
            };
            Ok(IndexResultAuthorizationPolicy::Realm(
                RealmIndexResultAuthorizationPolicy {
                    realm: policy.realm.clone(),
                    resource_namespace: policy.resource_namespace.clone(),
                    relation: policy.relation.clone(),
                    target,
                },
            ))
        }
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
        if self.admission.system_revision == 0 {
            return Err(Status::data_loss(
                "index candidate visibility has no Zanzibar admission revision",
            ));
        }
        if candidates.is_empty() {
            return Ok(CandidateVisibilityEvidence {
                visible: Vec::new(),
                authorization_revision: self.admission.system_revision,
                denied: 0,
            });
        }

        let mut checks = Vec::with_capacity(candidates.len());
        let mut capability_allowed = Vec::with_capacity(candidates.len());
        let mut realm_resources = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let (source, result) = self.validate_candidate(candidate)?;
            capability_allowed
                .push(self.capability_allows(&source) && self.capability_allows(&result));
            let resource = if matches!(self.kind, IndexKind::GitSource | IndexKind::Tensor) {
                result.clone()
            } else {
                source.clone()
            };
            checks.push((resource, ObjectPermission::Get));
            realm_resources.push(match &self.result_policy {
                IndexResultAuthorizationPolicy::Application => None,
                IndexResultAuthorizationPolicy::Realm(policy) => {
                    Some(self.realm_resource(policy, &source, &result)?)
                }
            });
        }
        let evidence = self
            .authorization
            .allows_objects_at_revision_with_evidence(
                &self.caller,
                &checks,
                self.admission.system_revision,
            )
            .await?;
        if evidence.revision == 0 || evidence.allowed.len() != checks.len() {
            return Err(Status::data_loss(
                "Zanzibar returned invalid index authorization evidence",
            ));
        }
        if evidence.revision != self.admission.system_revision {
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
        if let IndexResultAuthorizationPolicy::Realm(policy) = &self.result_policy {
            let required = self.admission.result.as_ref().ok_or_else(|| {
                Status::data_loss("custom-realm query has no authorization evidence")
            })?;
            let subject = self.authorization_subject.as_ref().ok_or_else(|| {
                Status::permission_denied("custom-realm query requires an end-user subject")
            })?;
            let scope = AuthzScope::new(
                StorageTenantId::parse(self.caller.storage_tenant().as_str())
                    .map_err(|error| Status::data_loss(error.to_string()))?,
                RealmId::custom(&policy.realm).map_err(crate::authz_api::authz_status)?,
            )
            .map_err(|error| Status::data_loss(error.to_string()))?;
            let admitted_indexes = visible
                .iter()
                .enumerate()
                .filter_map(|(index, admitted)| admitted.then_some(index))
                .collect::<Vec<_>>();
            if !admitted_indexes.is_empty() {
                let resources = admitted_indexes
                    .iter()
                    .map(|index| {
                        realm_resources[*index].clone().ok_or_else(|| {
                            Status::internal("custom-realm candidate mapping is missing")
                        })
                    })
                    .collect::<Result<Vec<_>, Status>>()?;
                let checked = self
                    .authorization
                    .allows_realm_results_with_evidence(
                        &self.caller,
                        self.tenant_id,
                        &scope,
                        subject,
                        &policy.relation,
                        &resources,
                        self.admission.system_revision,
                        required,
                    )
                    .await?;
                if checked.revision != required.revision
                    || checked.allowed.len() != admitted_indexes.len()
                {
                    return Err(Status::data_loss(
                        "custom realm returned invalid index authorization evidence",
                    ));
                }
                for (index, allowed) in admitted_indexes.into_iter().zip(checked.allowed) {
                    visible[index] &= allowed;
                }
            }
        } else if self.admission.result.is_some() || self.authorization_subject.is_some() {
            return Err(Status::data_loss(
                "application-authorized query carries custom-realm evidence",
            ));
        }
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
    use keldra_api::v1::{
        ApplicationIndexResultAuthorization, IndexResultAuthorization, ObjectAddress,
        RealmIndexResultAuthorization, index_result_authorization,
    };
    use keldra_authz::ObjectId;
    use keldra_store::{SchemaDigest, SchemaId, SchemaRef, StorageTenantId};
    use std::sync::Mutex;

    use super::*;
    use crate::index_service::{IndexAuthorizationEvidence, IndexRealmAuthorizationEvidence};

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

    struct CustomRealmAuthorization;

    struct IntersectingRealmAuthorization {
        realm_resources: Mutex<Vec<String>>,
    }

    #[tonic::async_trait]
    impl IndexAuthorization for CustomRealmAuthorization {
        async fn allows_objects_with_evidence(
            &self,
            _caller: &Caller,
            requests: &[(ObjectKey, ObjectPermission)],
        ) -> Result<IndexAuthorizationEvidence, Status> {
            Ok(IndexAuthorizationEvidence {
                allowed: vec![true; requests.len()],
                revision: 9,
            })
        }

        async fn allows_realm_results_with_evidence(
            &self,
            _caller: &Caller,
            _stable_tenant_id: u64,
            _scope: &AuthzScope,
            _authorization_subject: &ObjectRef,
            relation: &str,
            resources: &[ObjectRef],
            required_system_revision: u64,
            required: &IndexRealmAuthorizationEvidence,
        ) -> Result<IndexAuthorizationEvidence, Status> {
            assert_eq!(relation, "view");
            assert_eq!(required_system_revision, 9);
            Ok(IndexAuthorizationEvidence {
                allowed: resources
                    .iter()
                    .map(|resource| match &resource.id {
                        ObjectId::ExactPath(path) => path.path != "canonical/denied",
                        ObjectId::Opaque(_) => false,
                    })
                    .collect(),
                revision: required.revision,
            })
        }
    }

    #[tonic::async_trait]
    impl IndexAuthorization for IntersectingRealmAuthorization {
        async fn allows_objects_with_evidence(
            &self,
            _caller: &Caller,
            requests: &[(ObjectKey, ObjectPermission)],
        ) -> Result<IndexAuthorizationEvidence, Status> {
            Ok(IndexAuthorizationEvidence {
                allowed: requests
                    .iter()
                    .map(|(key, _)| key.path() != "docs/system-denied")
                    .collect(),
                revision: 9,
            })
        }

        async fn allows_realm_results_with_evidence(
            &self,
            _caller: &Caller,
            _stable_tenant_id: u64,
            _scope: &AuthzScope,
            _authorization_subject: &ObjectRef,
            _relation: &str,
            resources: &[ObjectRef],
            _required_system_revision: u64,
            required: &IndexRealmAuthorizationEvidence,
        ) -> Result<IndexAuthorizationEvidence, Status> {
            *self.realm_resources.lock().unwrap() = resources
                .iter()
                .map(|resource| match &resource.id {
                    ObjectId::ExactPath(path) => path.path.clone(),
                    ObjectId::Opaque(id) => id.clone(),
                })
                .collect();
            Ok(IndexAuthorizationEvidence {
                allowed: vec![true; resources.len()],
                revision: required.revision,
            })
        }
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
            11,
            IndexQueryAuthorizationEvidence {
                system_revision: 9,
                result: None,
            },
            IndexResultAuthorizationPolicy::Application,
            None,
            "objects".into(),
            "docs/".into(),
            kind,
            None,
            Arc::new(TestAuthorization),
        )
    }

    fn realm_evidence() -> IndexRealmAuthorizationEvidence {
        IndexRealmAuthorizationEvidence {
            revision: 17,
            binding_generation: 3,
            schema_ref: SchemaRef {
                schema_id: SchemaId::parse("documents").unwrap(),
                schema_revision: 5,
                schema_digest: SchemaDigest([7; 32]),
            },
        }
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
        changed.admission.system_revision = 8;
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
            11,
            IndexQueryAuthorizationEvidence {
                system_revision: 9,
                result: None,
            },
            IndexResultAuthorizationPolicy::Application,
            None,
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

    #[tokio::test]
    async fn custom_realm_intersects_system_access_and_uses_canonical_source_mapping() {
        let policy = RealmIndexResultAuthorizationPolicy {
            realm: "workspace".into(),
            resource_namespace: "document".into(),
            relation: "view".into(),
            target: RealmIndexAuthorizationTarget::CanonicalSourcePath,
        };
        let visibility = AuthorizedSnapshotCandidates::new(
            Caller::from_authenticated_application(
                StorageTenantId::parse("tenant").unwrap(),
                "application",
            )
            .unwrap(),
            11,
            IndexQueryAuthorizationEvidence {
                system_revision: 9,
                result: Some(realm_evidence()),
            },
            IndexResultAuthorizationPolicy::Realm(policy),
            Some(ObjectRef::opaque("user", "alice").unwrap()),
            "objects".into(),
            "docs/".into(),
            IndexKind::Path,
            None,
            Arc::new(CustomRealmAuthorization),
        );
        let mut allowed = candidate("docs/allowed");
        allowed.authorization_source_path = "canonical/allowed".into();
        let mut denied = candidate("docs/denied");
        denied.authorization_source_path = "canonical/denied".into();

        let result = visibility.evaluate(&[allowed, denied]).await.unwrap();

        assert_eq!(result.visible, [true, false]);
        assert_eq!(result.denied, 1);
    }

    #[tokio::test]
    async fn custom_realm_cannot_restore_a_candidate_denied_by_application_access() {
        let authorization = Arc::new(IntersectingRealmAuthorization {
            realm_resources: Mutex::new(Vec::new()),
        });
        let visibility = AuthorizedSnapshotCandidates::new(
            Caller::from_authenticated_application(
                StorageTenantId::parse("tenant").unwrap(),
                "application",
            )
            .unwrap(),
            11,
            IndexQueryAuthorizationEvidence {
                system_revision: 9,
                result: Some(realm_evidence()),
            },
            IndexResultAuthorizationPolicy::Realm(RealmIndexResultAuthorizationPolicy {
                realm: "workspace".into(),
                resource_namespace: "document".into(),
                relation: "view".into(),
                target: RealmIndexAuthorizationTarget::CanonicalSourcePath,
            }),
            Some(ObjectRef::opaque("user", "alice").unwrap()),
            "objects".into(),
            "docs/".into(),
            IndexKind::Path,
            None,
            authorization.clone(),
        );

        let result = visibility
            .evaluate(&[
                candidate("docs/system-denied"),
                candidate("docs/system-allowed"),
            ])
            .await
            .unwrap();

        assert_eq!(result.visible, [false, true]);
        assert_eq!(result.denied, 1);
        assert_eq!(
            *authorization.realm_resources.lock().unwrap(),
            ["docs/system-allowed"]
        );
    }

    #[tokio::test]
    async fn result_path_policy_authorizes_the_public_result_not_its_source() {
        let policy = RealmIndexResultAuthorizationPolicy {
            realm: "workspace".into(),
            resource_namespace: "document".into(),
            relation: "view".into(),
            target: RealmIndexAuthorizationTarget::ResultPath,
        };
        let visibility = AuthorizedSnapshotCandidates::new(
            Caller::from_authenticated_application(
                StorageTenantId::parse("tenant").unwrap(),
                "application",
            )
            .unwrap(),
            11,
            IndexQueryAuthorizationEvidence {
                system_revision: 9,
                result: Some(realm_evidence()),
            },
            IndexResultAuthorizationPolicy::Realm(policy),
            Some(ObjectRef::opaque("user", "alice").unwrap()),
            "objects".into(),
            "docs/".into(),
            IndexKind::GitSource,
            None,
            Arc::new(CustomRealmAuthorization),
        );
        let mut candidate = candidate("docs/source.json");
        candidate.authorization_source_path = "canonical/denied".into();
        candidate.result.address.as_mut().unwrap().path = "payloads/visible.bin".into();

        assert_eq!(
            visibility.evaluate(&[candidate]).await.unwrap().visible,
            [true]
        );
    }

    #[test]
    fn definition_policy_decoding_has_no_implicit_or_unspecified_fallback() {
        let mut definition = IndexDefinition::default();
        assert_eq!(
            result_authorization_policy(&definition).unwrap_err().code(),
            tonic::Code::DataLoss
        );
        definition.result_authorization = Some(IndexResultAuthorization {
            policy: Some(index_result_authorization::Policy::Application(
                ApplicationIndexResultAuthorization {},
            )),
        });
        assert_eq!(
            result_authorization_policy(&definition).unwrap(),
            IndexResultAuthorizationPolicy::Application
        );
        definition.result_authorization = Some(IndexResultAuthorization {
            policy: Some(index_result_authorization::Policy::Realm(
                RealmIndexResultAuthorization {
                    realm: "workspace".into(),
                    resource_namespace: "document".into(),
                    relation: "view".into(),
                    target: IndexAuthorizationTarget::Unspecified as i32,
                },
            )),
        });
        assert_eq!(
            result_authorization_policy(&definition).unwrap_err().code(),
            tonic::Code::DataLoss
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
