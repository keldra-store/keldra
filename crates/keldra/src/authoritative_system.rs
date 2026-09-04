//! Fresh protected-system Zanzibar checks for public cluster operations.
//!
//! The protected realm is one ordinary tenant-wide Zanzibar group keyed by
//! the stable `_keldra` tenant ID. Customer tenant and bucket IDs only bind a
//! permission's mutable names to the physical object range being accessed.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use keldra_authz::{AuthorizationCheck, ObjectRef};
use keldra_consensus::{DecisionRaft, NodeId};
use keldra_store::{AuthzConsistency, AuthzRevision, AuthzScope, ObjectKey};
use tonic::Status;

use crate::authentication::Caller;
use crate::authorization::{
    ObjectPermission, SYSTEM_STABLE_TENANT_ID, bucket_policy_authorization_check,
    object_authorization_checks,
};
use crate::authz_distribution::ZanzibarDistribution;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::logical_name_resolution::LogicalNameResolver;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;

const AUTHORIZATION_READ_TIMEOUT: Duration = Duration::from_secs(30);
const AUTHORIZATION_RETRY_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StableBucketAuthorization {
    pub(crate) storage_tenant: String,
    pub(crate) bucket: String,
    pub(crate) expected_tenant_id: u64,
    pub(crate) expected_bucket_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FreshAuthorizationResult {
    pub(crate) allowed: Vec<bool>,
    pub(crate) revision: AuthzRevision,
    pub(crate) binding_generation: u64,
}

#[derive(Clone)]
pub(crate) struct AuthoritativeSystemAuthorization {
    local_node: NodeId,
    decisions: DecisionRaft,
    zanzibar: Arc<ZanzibarDistribution>,
    peers: ClusterPeerTransport,
    names: LogicalNameResolver,
}

impl AuthoritativeSystemAuthorization {
    pub(crate) fn new(
        local_node: NodeId,
        decisions: DecisionRaft,
        zanzibar: Arc<ZanzibarDistribution>,
        peers: ClusterPeerTransport,
        names: LogicalNameResolver,
    ) -> Self {
        Self {
            local_node,
            decisions,
            zanzibar,
            peers,
            names,
        }
    }

    /// Evaluate one deduplicated bucket grant per bucket and permission, then
    /// exact-object grants only for entries whose bucket denied access. Both
    /// passes use one authoritative revision, so a concurrent Zanzibar
    /// mutation retries/fails instead of mixing views.
    pub(crate) async fn allows_objects(
        &self,
        caller: &Caller,
        requests: &[(ObjectKey, ObjectPermission)],
    ) -> Result<Vec<bool>, Status> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        self.allows_objects_with_evidence(caller, requests)
            .await
            .map(|result| result.allowed)
    }

    /// The same bucket-then-exact evaluation as [`Self::allows_objects`], plus
    /// the authoritative revision that must bind index pagination and result
    /// filtering. This never accepts an ingress-supplied revision.
    pub(crate) async fn allows_objects_with_evidence(
        &self,
        caller: &Caller,
        requests: &[(ObjectKey, ObjectPermission)],
    ) -> Result<FreshAuthorizationResult, Status> {
        if requests.is_empty() {
            return Err(Status::invalid_argument(
                "authorization check batch must not be empty",
            ));
        }
        let mut exact = Vec::with_capacity(requests.len());
        let mut bucket_checks = Vec::new();
        let mut bucket_indexes = BTreeMap::<(&str, &str, ObjectPermission), usize>::new();
        let mut request_bucket_indexes = Vec::with_capacity(requests.len());
        for (key, permission) in requests {
            if key.tenant() != caller.storage_tenant().as_str() {
                return Err(Status::permission_denied(
                    "object operation does not belong to the authenticated tenant",
                ));
            }
            let [object, bucket] = object_authorization_checks(caller.subject(), key, *permission)
                .map_err(crate::authz_api::authz_status)?;
            exact.push(object);
            let bucket_identity = (key.tenant(), key.bucket(), *permission);
            let bucket_index = match bucket_indexes.get(&bucket_identity) {
                Some(index) => *index,
                None => {
                    let index = bucket_checks.len();
                    bucket_checks.push(bucket);
                    bucket_indexes.insert(bucket_identity, index);
                    index
                }
            };
            request_bucket_indexes.push(bucket_index);
        }
        let bindings = self.stable_bucket_bindings(requests).await?;
        let first = self
            .fresh_system_checks(AuthzConsistency::Latest, bucket_checks, bindings.clone())
            .await?;
        let mut allowed = request_bucket_indexes
            .iter()
            .map(|index| first.allowed[*index])
            .collect::<Vec<_>>();
        if allowed.iter().all(|allowed| *allowed) {
            return Ok(FreshAuthorizationResult {
                allowed,
                revision: first.revision,
                binding_generation: first.binding_generation,
            });
        }

        let denied = allowed
            .iter()
            .enumerate()
            .filter_map(|(index, allowed)| (!allowed).then_some((index, exact[index].clone())))
            .collect::<Vec<_>>();
        let second = self
            .fresh_system_checks(
                AuthzConsistency::Exact(first.revision),
                denied.iter().map(|(_, check)| check.clone()).collect(),
                bindings,
            )
            .await?;
        if second.revision != first.revision
            || second.binding_generation != first.binding_generation
        {
            return Err(Status::unavailable(
                "authorization view changed while applying exact-object fallbacks",
            ));
        }
        for ((index, _), exact_allowed) in denied.into_iter().zip(second.allowed) {
            allowed[index] = exact_allowed;
        }
        Ok(FreshAuthorizationResult {
            allowed,
            revision: first.revision,
            binding_generation: first.binding_generation,
        })
    }

    pub(crate) async fn allows_object(
        &self,
        caller: &Caller,
        key: &ObjectKey,
        permission: ObjectPermission,
    ) -> Result<bool, Status> {
        self.allows_objects(caller, &[(key.clone(), permission)])
            .await
            .map(|allowed| allowed[0])
    }

    pub(crate) async fn allows_bucket_policy(
        &self,
        caller: &Caller,
        tenant: &str,
        bucket: &str,
    ) -> Result<bool, Status> {
        if tenant != caller.storage_tenant().as_str() {
            return Err(Status::permission_denied(
                "bucket policy does not belong to the authenticated tenant",
            ));
        }
        self.allows_bucket_policy_with_evidence(caller, tenant, bucket)
            .await
            .map(|result| result.allowed[0])
    }

    pub(crate) async fn allows_bucket_policy_with_evidence(
        &self,
        caller: &Caller,
        tenant: &str,
        bucket: &str,
    ) -> Result<FreshAuthorizationResult, Status> {
        if tenant != caller.storage_tenant().as_str() {
            return Err(Status::permission_denied(
                "bucket policy does not belong to the authenticated tenant",
            ));
        }
        let (tenant_id, bucket_id) = self.names.resolve_bucket_ids(tenant, bucket).await?;
        let check = bucket_policy_authorization_check(caller.subject(), tenant, bucket)
            .map_err(crate::authz_api::authz_status)?;
        self.fresh_system_checks(
            AuthzConsistency::Latest,
            vec![check],
            vec![StableBucketAuthorization {
                storage_tenant: tenant.to_owned(),
                bucket: bucket.to_owned(),
                expected_tenant_id: tenant_id,
                expected_bucket_id: bucket_id,
            }],
        )
        .await
    }

    pub(crate) async fn fresh_system_check(
        &self,
        check: AuthorizationCheck,
    ) -> Result<FreshAuthorizationResult, Status> {
        self.fresh_system_checks(AuthzConsistency::Latest, vec![check], Vec::new())
            .await
    }

    /// Fresh check in one customer tenant's ordinary Zanzibar replica group.
    pub(crate) async fn fresh_tenant_check(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        check: AuthorizationCheck,
    ) -> Result<FreshAuthorizationResult, Status> {
        self.fresh_tenant_checks(stable_tenant_id, scope, vec![check])
            .await
    }

    pub(crate) async fn fresh_tenant_checks(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        checks: Vec<AuthorizationCheck>,
    ) -> Result<FreshAuthorizationResult, Status> {
        if checks.is_empty() {
            return Err(Status::invalid_argument(
                "authorization check batch must not be empty",
            ));
        }
        self.fresh_checks_with_placement_retry(
            stable_tenant_id,
            &scope,
            AuthzConsistency::Latest,
            &checks,
            &[],
        )
        .await
    }

    async fn stable_bucket_bindings(
        &self,
        requests: &[(ObjectKey, ObjectPermission)],
    ) -> Result<Vec<StableBucketAuthorization>, Status> {
        let mut buckets = BTreeMap::<(&str, &str), (u64, u64)>::new();
        for (key, _) in requests {
            let identity = (key.tenant(), key.bucket());
            if !buckets.contains_key(&identity) {
                let resolved = self
                    .names
                    .resolve_bucket_ids(key.tenant(), key.bucket())
                    .await?;
                buckets.insert(identity, resolved);
            }
        }
        Ok(buckets
            .into_iter()
            .map(
                |((storage_tenant, bucket), (expected_tenant_id, expected_bucket_id))| {
                    StableBucketAuthorization {
                        storage_tenant: storage_tenant.to_owned(),
                        bucket: bucket.to_owned(),
                        expected_tenant_id,
                        expected_bucket_id,
                    }
                },
            )
            .collect())
    }

    pub(crate) async fn fresh_system_checks(
        &self,
        consistency: AuthzConsistency,
        checks: Vec<AuthorizationCheck>,
        stable_buckets: Vec<StableBucketAuthorization>,
    ) -> Result<FreshAuthorizationResult, Status> {
        if checks.is_empty() {
            return Err(Status::invalid_argument(
                "authorization check batch must not be empty",
            ));
        }
        self.fresh_checks_with_placement_retry(
            SYSTEM_STABLE_TENANT_ID,
            &AuthzScope::system(),
            consistency,
            &checks,
            &stable_buckets,
        )
        .await
    }

    async fn fresh_checks_with_placement_retry(
        &self,
        stable_tenant_id: u64,
        scope: &AuthzScope,
        consistency: AuthzConsistency,
        checks: &[AuthorizationCheck],
        stable_buckets: &[StableBucketAuthorization],
    ) -> Result<FreshAuthorizationResult, Status> {
        let deadline = tokio::time::Instant::now() + AUTHORIZATION_READ_TIMEOUT;
        loop {
            let attempt = async {
                let placement = self.placement()?;
                let group = MutableRecordReplicaGroup::select(
                    PlacementKind::ZanzibarRealm,
                    placement.cluster_id(),
                    &stable_tenant_id.to_be_bytes(),
                    placement.placement_nodes(),
                )
                .ok_or_else(|| Status::unavailable("cluster has no Zanzibar replica"))?;
                self.fresh_checks_from_replicas(
                    &placement,
                    &group,
                    stable_tenant_id,
                    scope,
                    consistency,
                    checks,
                    stable_buckets,
                )
                .await
            };
            let result = tokio::time::timeout_at(deadline, attempt)
                .await
                .map_err(|_| Status::deadline_exceeded("authorization read deadline exceeded"))?;
            match result {
                Ok(result) => return Ok(result),
                Err(error) if retryable_authorization_availability(&error) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Err(error);
                    }
                    tokio::time::sleep_until((now + AUTHORIZATION_RETRY_INTERVAL).min(deadline))
                        .await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn fresh_checks_from_replicas(
        &self,
        placement: &ClusterPlacement,
        group: &MutableRecordReplicaGroup,
        stable_tenant_id: u64,
        scope: &AuthzScope,
        consistency: AuthzConsistency,
        checks: &[AuthorizationCheck],
        stable_buckets: &[StableBucketAuthorization],
    ) -> Result<FreshAuthorizationResult, Status> {
        let fence = placement.fence();
        let targets = authorization_read_targets_local_first(group.replicas(), self.local_node);
        let started = Instant::now();
        let mut last_unavailable = None;
        for (index, target) in targets.iter().copied().enumerate() {
            let timeout = authorization_attempt_timeout(started, targets.len() - index)?;
            let result = if target == self.local_node {
                tokio::time::timeout(timeout, async {
                    self.verify_stable_buckets(stable_buckets).await?;
                    let (allowed, revision, binding_generation) = self
                        .zanzibar
                        .fresh_checks_with_generation(
                            stable_tenant_id,
                            scope.clone(),
                            consistency,
                            checks.to_vec(),
                        )
                        .await?;
                    Ok::<_, Status>(FreshAuthorizationResult {
                        allowed,
                        revision,
                        binding_generation,
                    })
                })
                .await
                .unwrap_or_else(|_| {
                    Err(Status::deadline_exceeded(
                        "fresh authorization read timed out",
                    ))
                })
            } else {
                let address = placement.address(target).ok_or_else(|| {
                    Status::unavailable("authorization replica has no current peer address")
                })?;
                self.peers
                    .fresh_authorization_checks(
                        target,
                        &address.0,
                        stable_tenant_id,
                        scope,
                        consistency,
                        checks,
                        stable_buckets,
                        fence,
                        timeout,
                    )
                    .await
            };
            match result {
                Ok(result) => {
                    let current = self.placement()?;
                    let current_group = MutableRecordReplicaGroup::select(
                        PlacementKind::ZanzibarRealm,
                        current.cluster_id(),
                        &stable_tenant_id.to_be_bytes(),
                        current.placement_nodes(),
                    )
                    .ok_or_else(|| Status::unavailable("cluster has no Zanzibar replica"))?;
                    if current.fence() != fence || current_group != *group {
                        return Err(Status::unavailable(
                            "authorization placement changed during the request",
                        ));
                    }
                    return Ok(result);
                }
                Err(error) if retryable_authorization_availability(&error) => {
                    last_unavailable = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_unavailable.unwrap_or_else(|| {
            Status::unavailable("authorization realm has no available read replica")
        }))
    }

    async fn verify_stable_buckets(
        &self,
        stable_buckets: &[StableBucketAuthorization],
    ) -> Result<(), Status> {
        for binding in stable_buckets {
            let observed = self
                .names
                .resolve_bucket_ids(&binding.storage_tenant, &binding.bucket)
                .await?;
            if observed != (binding.expected_tenant_id, binding.expected_bucket_id) {
                return Err(Status::unavailable(
                    "bucket identity changed while authorizing the request",
                ));
            }
        }
        Ok(())
    }

    fn placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

fn authorization_read_targets_local_first(replicas: &[NodeId], local_node: NodeId) -> Vec<NodeId> {
    let mut targets = replicas.to_vec();
    if let Some(index) = targets.iter().position(|node| *node == local_node) {
        targets.swap(0, index);
    }
    targets
}

fn authorization_attempt_timeout(
    started: Instant,
    remaining_targets: usize,
) -> Result<Duration, Status> {
    let remaining = AUTHORIZATION_READ_TIMEOUT
        .checked_sub(started.elapsed())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| Status::deadline_exceeded("authorization read deadline exceeded"))?;
    let divisor = u32::try_from(remaining_targets.max(1)).unwrap_or(u32::MAX);
    Ok(remaining / divisor)
}

fn retryable_authorization_availability(error: &Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    )
}

#[tonic::async_trait]
impl crate::index_service::IndexAuthorization for AuthoritativeSystemAuthorization {
    async fn allows_objects_with_evidence(
        &self,
        caller: &Caller,
        requests: &[(ObjectKey, ObjectPermission)],
    ) -> Result<crate::index_service::IndexAuthorizationEvidence, Status> {
        let evidence =
            AuthoritativeSystemAuthorization::allows_objects_with_evidence(self, caller, requests)
                .await?;
        Ok(crate::index_service::IndexAuthorizationEvidence {
            allowed: evidence.allowed,
            revision: evidence.revision.0,
        })
    }
}

pub(crate) fn manage_system_check(subject: &ObjectRef) -> Result<AuthorizationCheck, Status> {
    Ok(AuthorizationCheck::new(
        subject.clone(),
        ObjectRef::opaque("system", keldra_store::SYSTEM_STORAGE_TENANT_ID)
            .map_err(crate::authz_api::authz_status)?,
        "manage_system",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authorization::{bucket_resource, caller_subject, object_resource};

    #[test]
    fn object_checks_keep_exact_then_bucket_fallback_order() {
        let caller = caller_subject("app").unwrap();
        let key = ObjectKey::new("acme", "objects", "reports/one.json").unwrap();
        let [exact, bucket] =
            object_authorization_checks(&caller, &key, ObjectPermission::Get).unwrap();
        assert_eq!(exact.object, object_resource(&key).unwrap());
        assert_eq!(bucket.object, bucket_resource("acme", "objects").unwrap());
        assert_eq!(exact.relation, "get");
        assert_eq!(bucket.relation, "get_object");
    }

    #[test]
    fn protected_zanzibar_group_has_one_stable_identity() {
        assert_eq!(SYSTEM_STABLE_TENANT_ID, 1);
    }

    #[test]
    fn authorization_reads_prefer_the_local_selected_replica() {
        assert_eq!(
            authorization_read_targets_local_first(&[NodeId(1), NodeId(2), NodeId(3)], NodeId(2),),
            vec![NodeId(2), NodeId(1), NodeId(3)]
        );
        assert_eq!(
            authorization_read_targets_local_first(&[NodeId(1), NodeId(2), NodeId(3)], NodeId(9),),
            vec![NodeId(1), NodeId(2), NodeId(3)]
        );
    }

    #[test]
    fn authorization_failover_retries_only_availability_failures() {
        assert!(retryable_authorization_availability(&Status::unavailable(
            "down"
        )));
        assert!(retryable_authorization_availability(
            &Status::deadline_exceeded("slow")
        ));
        for code in [
            tonic::Code::PermissionDenied,
            tonic::Code::Unauthenticated,
            tonic::Code::DataLoss,
            tonic::Code::InvalidArgument,
            tonic::Code::Internal,
        ] {
            assert!(!retryable_authorization_availability(&Status::new(
                code, "closed"
            )));
        }
    }
}
