use crate::{
    core_store::CoreMutationPrecondition,
    partition_fence::{
        AcquireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal, OwnershipResource,
        OwnershipResourceKind, RenewOwnership, acquire_ownership, ownership_fence_precondition,
        read_ownership_fence, renew_ownership,
    },
    storage::Storage,
    task_execution_guard::TaskExecutionGuard,
};
use anyhow::{Result, anyhow};
use std::future::Future;

#[derive(Debug, Clone, Copy)]
pub(crate) struct DirectRepairIndexBuildAuthority {
    _private: (),
}

impl DirectRepairIndexBuildAuthority {
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum IndexBuildAuthority<'a> {
    Task(&'a TaskExecutionGuard),
    DirectRepair(DirectRepairIndexBuildAuthority),
}

#[derive(Debug, Clone)]
pub(crate) struct IndexBuildOwnership {
    resource: OwnershipResource,
    owner: OwnershipPrincipal,
    fence: u64,
}

impl IndexBuildOwnership {
    pub(crate) async fn acquire(
        storage: &Storage,
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

        let record = if let Some(record) =
            read_ownership_fence(storage, owner.tenant_id, &resource, signing_key).await?
            && record.owner.same_security_owner(&owner)
            && record.is_active_unexpired(now_nanos)
        {
            renew_ownership(
                storage,
                RenewOwnership {
                    request_id: format!("index-build-renew-{}", resource.resource_id),
                    resource: resource.clone(),
                    owner: owner.clone(),
                    current_fence: record.fence,
                    now_nanos,
                    ttl_nanos,
                },
                signing_key,
            )
            .await?
            .record
        } else {
            acquire_ownership(
                storage,
                AcquireOwnership {
                    request_id: format!("index-build-acquire-{}", resource.resource_id),
                    idempotency_key: format!("index-build-owner-{}", resource.resource_id),
                    resource: resource.clone(),
                    owner: owner.clone(),
                    now_nanos,
                    ttl_nanos,
                },
                signing_key,
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
}

impl IndexBuildAuthority<'_> {
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
                let permit = guard.publication_permit().await?;
                permit
                    .publish_with(|task_precondition| {
                        publication(vec![ownership_precondition, task_precondition])
                    })
                    .await
            }
            Self::DirectRepair(_) => publication(vec![ownership_precondition]).await,
        }
    }
}

fn current_time_nanos() -> Result<i64> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("index build timestamp cannot be represented in nanoseconds"))
}
