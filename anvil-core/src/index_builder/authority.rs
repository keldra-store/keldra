use crate::{
    core_store::CoreMutationPrecondition,
    partition_fence::{
        AcquireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal, OwnershipResource,
        OwnershipResourceKind, RenewOwnership, commit_implicit_ownership_plan,
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
}

impl<'a> DirectRepairIndexBuildAuthority<'a> {
    pub(crate) fn new(mvcc: &'a crate::mvcc_bootstrap::MvccSubsystem) -> Self {
        Self { mvcc }
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
}

impl IndexBuildOwnership {
    pub(crate) async fn acquire(
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        tenant_id: i64,
        bucket_id: i64,
        index_storage_id: &str,
        builder_node_id: &str,
        signing_key: &[u8],
    ) -> Result<Self> {
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
        let record = if let Some(record) =
            read_ownership_fence_mvcc(mvcc, owner.tenant_id, &resource, signing_key)?
            && record.owner.same_security_owner(&owner)
            && record.is_active_unexpired(now_nanos)
        {
            let request = RenewOwnership {
                request_id: format!("index-build-renew-{}", resource.resource_id),
                resource: resource.clone(),
                owner: owner.clone(),
                current_fence: record.fence,
                now_nanos,
                ttl_nanos,
            };
            let idempotency_key = request.request_id.clone();
            commit_implicit_ownership_plan(
                mvcc,
                &transaction_principal,
                &idempotency_key,
                now_nanos,
                owner.tenant_id,
                &resource,
                signing_key,
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
            let request = AcquireOwnership {
                request_id: format!("index-build-acquire-{}", resource.resource_id),
                idempotency_key: format!("index-build-owner-{}", resource.resource_id),
                resource: resource.clone(),
                owner: owner.clone(),
                now_nanos,
                ttl_nanos,
            };
            let idempotency_key = request.idempotency_key.clone();
            commit_implicit_ownership_plan(
                mvcc,
                &transaction_principal,
                &idempotency_key,
                now_nanos,
                owner.tenant_id,
                &resource,
                signing_key,
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
        })
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

impl IndexBuildAuthority<'_> {
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
