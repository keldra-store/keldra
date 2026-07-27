use crate::{
    core_store::CoreMutationPrecondition,
    partition_fence::{
        AcquireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal, OwnershipResource,
        OwnershipResourceKind, RenewOwnership, commit_implicit_ownership_plan_with_assignment,
        ownership_fence_precondition, ownership_fence_predicate_mvcc,
        plan_acquire_ownership_in_transaction, plan_renew_ownership_in_transaction,
        read_ownership_fence_mvcc,
    },
    storage::Storage,
    task_execution_guard::TaskExecutionGuard,
};
use anyhow::{Result, anyhow};
use std::future::Future;

#[derive(Debug, Clone, Copy)]
pub(crate) struct DirectRepairIndexBuildAuthority<'a> {
    mvcc: &'a crate::mvcc_bootstrap::MvccSubsystem,
    assignment: Option<&'a crate::mvcc_worker_authority::AssignmentGuard>,
}

impl<'a> DirectRepairIndexBuildAuthority<'a> {
    pub(crate) fn new(mvcc: &'a crate::mvcc_bootstrap::MvccSubsystem) -> Self {
        Self {
            mvcc,
            assignment: None,
        }
    }

    pub(crate) fn for_assignment(
        mvcc: &'a crate::mvcc_bootstrap::MvccSubsystem,
        assignment: &'a crate::mvcc_worker_authority::AssignmentGuard,
    ) -> Self {
        Self {
            mvcc,
            assignment: Some(assignment),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum IndexBuildAuthority<'a> {
    Task(&'a TaskExecutionGuard),
    DirectRepair(DirectRepairIndexBuildAuthority<'a>),
}

#[derive(Debug, Clone)]
pub(crate) struct IndexBuildOwnership {
    resource: OwnershipResource,
    owner: OwnershipPrincipal,
    fence: u64,
    assignment: crate::mvcc_worker_authority::AssignmentGuard,
}

impl IndexBuildOwnership {
    pub(crate) async fn acquire(
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        tenant_id: i64,
        bucket_id: i64,
        index_storage_id: &str,
        builder_node_id: &str,
        signing_key: &[u8],
        authority: IndexBuildAuthority<'_>,
    ) -> Result<Self> {
        let assignment = authority
            .work_assignment("index-build", index_storage_id)
            .await?;
        let resource = OwnershipResource {
            resource_kind: OwnershipResourceKind::IndexPartition,
            resource_id: format!(
                "tenant/{tenant_id}/bucket/{bucket_id}/index_build/{index_storage_id}"
            ),
        };
        let owner = OwnershipPrincipal::node(builder_node_id);
        let now_nanos = current_time_nanos()?;
        let ttl_nanos = i64::try_from(MAX_OWNERSHIP_LEASE_MS)
            .map_err(|_| anyhow!("index build ownership TTL exceeds i64"))?
            .checked_mul(1_000_000)
            .ok_or_else(|| anyhow!("index build ownership TTL overflow"))?;

        let transaction_principal = format!("node:{builder_node_id}");
        let existing = read_ownership_fence_mvcc(mvcc, owner.tenant_id, &resource, signing_key)?;
        let record = if let Some(record) = existing.as_ref().filter(|record| {
            record.owner.same_security_owner(&owner) && record.is_active_unexpired(now_nanos)
        }) {
            let idempotency_key = internal_ownership_idempotency_key(
                "index-build",
                "renew",
                &resource,
                &owner,
                record.generation,
                record.fence,
            );
            let request = RenewOwnership {
                request_id: idempotency_key.clone(),
                resource: resource.clone(),
                owner: owner.clone(),
                current_fence: record.fence,
                now_nanos,
                ttl_nanos,
            };
            commit_implicit_ownership_plan_with_assignment(
                mvcc,
                &transaction_principal,
                &idempotency_key,
                now_nanos,
                owner.tenant_id,
                &resource,
                signing_key,
                Some(&assignment),
                |transaction_id| {
                    plan_renew_ownership_in_transaction(
                        mvcc,
                        transaction_id,
                        &transaction_principal,
                        request,
                        signing_key,
                    )
                },
            )
            .await?
            .record
        } else {
            let (observed_generation, observed_fence) = existing
                .as_ref()
                .map(|record| (record.generation, record.fence))
                .unwrap_or_default();
            let idempotency_key = internal_ownership_idempotency_key(
                "index-build",
                "acquire",
                &resource,
                &owner,
                observed_generation,
                observed_fence,
            );
            let request = AcquireOwnership {
                request_id: idempotency_key.clone(),
                idempotency_key: idempotency_key.clone(),
                resource: resource.clone(),
                owner: owner.clone(),
                now_nanos,
                ttl_nanos,
            };
            commit_implicit_ownership_plan_with_assignment(
                mvcc,
                &transaction_principal,
                &idempotency_key,
                now_nanos,
                owner.tenant_id,
                &resource,
                signing_key,
                Some(&assignment),
                |transaction_id| {
                    plan_acquire_ownership_in_transaction(
                        mvcc,
                        transaction_id,
                        &transaction_principal,
                        request,
                        signing_key,
                    )
                },
            )
            .await?
            .record
        };
        Ok(Self {
            resource,
            owner,
            fence: record.fence,
            assignment,
        })
    }

    pub(crate) fn assignment(&self) -> &crate::mvcc_worker_authority::AssignmentGuard {
        &self.assignment
    }

    async fn precondition(
        &self,
        storage: &Storage,
        signing_key: &[u8],
    ) -> Result<CoreMutationPrecondition> {
        ownership_fence_precondition(
            storage,
            self.owner.tenant_id,
            &self.resource,
            &self.owner,
            self.fence,
            current_time_nanos()?,
            signing_key,
        )
        .await
    }

    fn mvcc_precondition(
        &self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        signing_key: &[u8],
    ) -> Result<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )> {
        ownership_fence_predicate_mvcc(
            mvcc,
            self.owner.tenant_id,
            &self.resource,
            &self.owner,
            self.fence,
            current_time_nanos()?,
            signing_key,
        )
    }
}

pub(super) fn internal_ownership_idempotency_key(
    scope: &str,
    operation: &str,
    resource: &OwnershipResource,
    owner: &OwnershipPrincipal,
    observed_generation: u64,
    observed_fence: u64,
) -> String {
    fn hash_component(hasher: &mut blake3::Hasher, value: &[u8]) {
        hasher.update(&(value.len() as u64).to_be_bytes());
        hasher.update(value);
    }

    let mut hasher = blake3::Hasher::new();
    for component in [
        scope,
        operation,
        resource.resource_kind.as_str(),
        resource.resource_id.as_str(),
        owner.principal_kind.as_str(),
        owner.principal_id.as_str(),
        owner.actor_instance_id.as_str(),
    ] {
        hash_component(&mut hasher, component.as_bytes());
    }
    hash_component(&mut hasher, &owner.tenant_id.to_be_bytes());
    hash_component(&mut hasher, &observed_generation.to_be_bytes());
    hash_component(&mut hasher, &observed_fence.to_be_bytes());
    format!(
        "{scope}-ownership-{operation}-{}",
        hasher.finalize().to_hex()
    )
}

impl<'a> IndexBuildAuthority<'a> {
    pub(crate) fn mvcc(&self) -> Result<&crate::mvcc_bootstrap::MvccSubsystem> {
        match self {
            Self::Task(guard) => guard.mvcc(),
            Self::DirectRepair(authority) => Ok(authority.mvcc),
        }
    }

    pub(crate) async fn deterministic_payload_actor(self, direct_actor: &str) -> String {
        match self {
            Self::Task(guard) => {
                let lease = guard.snapshot().await;
                format!(
                    "index-build-task:{}",
                    blake3::hash(lease.task_id.as_bytes()).to_hex()
                )
            }
            Self::DirectRepair(_) => direct_actor.to_string(),
        }
    }

    pub(crate) fn assignment(self) -> Option<&'a crate::mvcc_worker_authority::AssignmentGuard> {
        match self {
            Self::Task(guard) => Some(guard.assignment()),
            Self::DirectRepair(authority) => authority.assignment,
        }
    }

    pub(crate) async fn work_assignment(
        self,
        kind: &str,
        logical_identity: &str,
    ) -> Result<crate::mvcc_worker_authority::AssignmentGuard> {
        if let Some(assignment) = self.assignment() {
            self.mvcc()?.validate_assignment(assignment)?;
            return Ok(assignment.clone());
        }
        self.mvcc()?
            .reconcile_work_assignment(kind, logical_identity)
            .await?
            .ok_or_else(|| anyhow!("index build is assigned to another node"))
    }

    /// Publishes one authoritative mutation with a fresh ownership CAS and,
    /// for task execution, a fresh exact temporal task-lease fence.
    pub(crate) async fn publish_with<T, F, Fut>(
        self,
        storage: &Storage,
        ownership: &IndexBuildOwnership,
        signing_key: &[u8],
        publication: F,
    ) -> Result<T>
    where
        F: FnOnce(Vec<CoreMutationPrecondition>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let ownership_precondition = ownership.precondition(storage, signing_key).await?;
        match self {
            Self::Task(guard) => {
                guard.check().await?;
                publication(vec![ownership_precondition]).await
            }
            Self::DirectRepair(_) => publication(vec![ownership_precondition]).await,
        }
    }

    /// Revalidates the mesh ownership lease at admission and, for task
    /// execution, publishes with the exact MVCC task-lease predicate.
    pub(crate) async fn publish_mvcc_with<T, F, Fut>(
        self,
        _storage: &Storage,
        ownership: &IndexBuildOwnership,
        signing_key: &[u8],
        publication: F,
    ) -> Result<T>
    where
        F: FnOnce(
            Vec<(
                crate::mvcc_transaction::LogicalKey,
                crate::mvcc_transaction::PredicateKind,
            )>,
        ) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let ownership_predicate = ownership.mvcc_precondition(self.mvcc()?, signing_key)?;
        match self {
            Self::Task(guard) => {
                guard
                    .publish_mvcc_with(|predicate| {
                        publication(vec![ownership_predicate, predicate])
                    })
                    .await
            }
            Self::DirectRepair(_) => publication(vec![ownership_predicate]).await,
        }
    }
}

fn current_time_nanos() -> Result<i64> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("index build timestamp cannot be represented in nanoseconds"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_ownership_keys_are_stable_for_retries_and_advance_with_fence_state() {
        let resource = OwnershipResource {
            resource_kind: OwnershipResourceKind::IndexPartition,
            resource_id: "tenant/7/bucket/9/index_build/example".to_string(),
        };
        let owner = OwnershipPrincipal::node("node-a");
        let key =
            internal_ownership_idempotency_key("index-build", "renew", &resource, &owner, 4, 2);
        assert_eq!(
            key,
            internal_ownership_idempotency_key("index-build", "renew", &resource, &owner, 4, 2,)
        );
        assert_ne!(
            key,
            internal_ownership_idempotency_key("index-build", "renew", &resource, &owner, 5, 2,)
        );
        assert_ne!(
            key,
            internal_ownership_idempotency_key("index-build", "acquire", &resource, &owner, 4, 2,)
        );
    }
}
