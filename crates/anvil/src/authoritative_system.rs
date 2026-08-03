//! Fresh protected-system Zanzibar checks for public cluster operations.
//!
//! The protected realm is one ordinary tenant-wide Zanzibar group keyed by
//! the stable `_anvil` tenant ID. Customer tenant and bucket IDs only bind a
//! permission's mutable names to the physical object range being accessed.

use std::collections::BTreeMap;
use std::sync::Arc;

use anvil_authz::{AuthorizationCheck, ObjectRef};
use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{AuthzConsistency, AuthzRevision, AuthzScope, ObjectKey};
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

    /// Evaluate exact-object grants first and bucket fallbacks only for the
    /// denied entries. The fallback is pinned to the exact first revision, so
    /// a concurrent Zanzibar mutation retries/fails instead of mixing views.
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

    /// The same exact-then-bucket evaluation as [`Self::allows_objects`], plus
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
        let mut fallback = Vec::with_capacity(requests.len());
        for (key, permission) in requests {
            if key.tenant() != caller.storage_tenant().as_str() {
                return Err(Status::permission_denied(
                    "object operation does not belong to the authenticated tenant",
                ));
            }
            let [object, bucket] = object_authorization_checks(caller.subject(), key, *permission)
                .map_err(crate::authz_api::authz_status)?;
            exact.push(object);
            fallback.push(bucket);
        }
        let bindings = self.stable_bucket_bindings(requests).await?;
        let first = self
            .fresh_system_checks(AuthzConsistency::Latest, exact, bindings.clone())
            .await?;
        if first.allowed.iter().all(|allowed| *allowed) {
            return Ok(first);
        }

        let denied = first
            .allowed
            .iter()
            .enumerate()
            .filter_map(|(index, allowed)| (!allowed).then_some((index, fallback[index].clone())))
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
                "authorization view changed while applying bucket fallbacks",
            ));
        }
        let mut allowed = first.allowed;
        for ((index, _), bucket_allowed) in denied.into_iter().zip(second.allowed) {
            allowed[index] = bucket_allowed;
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
        .map(|result| result.allowed[0])
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
        let placement = self.placement()?;
        let fence = placement.fence();
        let group = MutableRecordReplicaGroup::select(
            PlacementKind::ZanzibarRealm,
            placement.cluster_id(),
            &stable_tenant_id.to_be_bytes(),
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no tenant Zanzibar replica"))?;
        let coordinator = group.coordinator();
        let result = if coordinator == self.local_node {
            let (allowed, revision, binding_generation) = self
                .zanzibar
                .fresh_checks_with_generation(
                    stable_tenant_id,
                    scope,
                    AuthzConsistency::Latest,
                    checks.clone(),
                )
                .await?;
            FreshAuthorizationResult {
                allowed,
                revision,
                binding_generation,
            }
        } else {
            let address = placement.address(coordinator).ok_or_else(|| {
                Status::unavailable("tenant Zanzibar coordinator has no current peer address")
            })?;
            self.peers
                .fresh_authorization_checks(
                    coordinator,
                    &address.0,
                    stable_tenant_id,
                    &scope,
                    AuthzConsistency::Latest,
                    &checks,
                    &[],
                    fence,
                )
                .await?
        };
        if self.placement()?.fence() != fence {
            return Err(Status::unavailable(
                "authorization placement changed during the request",
            ));
        }
        Ok(result)
    }

    async fn stable_bucket_bindings(
        &self,
        requests: &[(ObjectKey, ObjectPermission)],
    ) -> Result<Vec<StableBucketAuthorization>, Status> {
        let mut buckets = BTreeMap::<(String, String), (u64, u64)>::new();
        for (key, _) in requests {
            let identity = (key.tenant().to_owned(), key.bucket().to_owned());
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
                        storage_tenant,
                        bucket,
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
        let placement = self.placement()?;
        let fence = placement.fence();
        let group = MutableRecordReplicaGroup::select(
            PlacementKind::ZanzibarRealm,
            placement.cluster_id(),
            &SYSTEM_STABLE_TENANT_ID.to_be_bytes(),
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no system Zanzibar replica"))?;
        let coordinator = group.coordinator();
        let result = if coordinator == self.local_node {
            self.verify_stable_buckets(&stable_buckets).await?;
            let (allowed, revision, binding_generation) = self
                .zanzibar
                .fresh_checks_with_generation(
                    SYSTEM_STABLE_TENANT_ID,
                    AuthzScope::system(),
                    consistency,
                    checks,
                )
                .await?;
            FreshAuthorizationResult {
                allowed,
                revision,
                binding_generation,
            }
        } else {
            let address = placement.address(coordinator).ok_or_else(|| {
                Status::unavailable("system Zanzibar coordinator has no current peer address")
            })?;
            self.peers
                .fresh_authorization_checks(
                    coordinator,
                    &address.0,
                    SYSTEM_STABLE_TENANT_ID,
                    &AuthzScope::system(),
                    consistency,
                    &checks,
                    &stable_buckets,
                    fence,
                )
                .await?
        };
        if self.placement()?.fence() != fence {
            return Err(Status::unavailable(
                "authorization placement changed during the request",
            ));
        }
        Ok(result)
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
        ObjectRef::opaque("system", anvil_store::SYSTEM_STORAGE_TENANT_ID)
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
}
