//! Coordinator-reconciled resolution of mutable tenant and bucket names.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use keldra_store::{LogicalRecordId, LogicalRecordValue, StorageTenantId};
use tonic::Status;

use crate::cluster_peer::ClusterPeerTransport;
use crate::logical_record_distribution::LogicalRecordDistribution;

const LOGICAL_NAME_READ_TIMEOUT: Duration = Duration::from_secs(30);
const LOGICAL_NAME_RETRY_INTERVAL: Duration = Duration::from_millis(25);

#[tonic::async_trait]
pub(crate) trait LogicalNameResolution: Send + Sync + 'static {
    async fn resolve_tenant_id(
        &self,
        storage_tenant: &StorageTenantId,
    ) -> Result<Option<u64>, Status>;

    async fn resolve_bucket_id(&self, tenant_id: u64, bucket: &str) -> Result<Option<u64>, Status>;
}

/// Fail-closed bridge shared by the peer listener, which starts before the
/// serving fence makes coordinator-reconciled logical reads available.
#[derive(Clone, Default)]
pub(crate) struct LateBoundLogicalNameResolution {
    inner: Arc<OnceLock<Arc<dyn LogicalNameResolution>>>,
}

impl LateBoundLogicalNameResolution {
    pub(crate) fn install(
        &self,
        resolver: Arc<dyn LogicalNameResolution>,
    ) -> Result<(), Arc<dyn LogicalNameResolution>> {
        self.inner.set(resolver)
    }

    fn resolver(&self) -> Result<Arc<dyn LogicalNameResolution>, Status> {
        self.inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("logical name resolution is not ready"))
    }
}

#[tonic::async_trait]
impl LogicalNameResolution for LateBoundLogicalNameResolution {
    async fn resolve_tenant_id(
        &self,
        storage_tenant: &StorageTenantId,
    ) -> Result<Option<u64>, Status> {
        self.resolver()?.resolve_tenant_id(storage_tenant).await
    }

    async fn resolve_bucket_id(&self, tenant_id: u64, bucket: &str) -> Result<Option<u64>, Status> {
        self.resolver()?.resolve_bucket_id(tenant_id, bucket).await
    }
}

#[derive(Clone)]
pub(crate) struct LogicalNameResolver {
    records: LogicalRecordDistribution,
    peers: ClusterPeerTransport,
}

impl LogicalNameResolver {
    pub(crate) fn new(records: LogicalRecordDistribution, peers: ClusterPeerTransport) -> Self {
        Self { records, peers }
    }

    pub(crate) async fn resolve_bucket_ids(
        &self,
        tenant: &str,
        bucket: &str,
    ) -> Result<(u64, u64), Status> {
        let tenant = StorageTenantId::parse(tenant)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let tenant_id = self
            .resolve_tenant_id(&tenant)
            .await?
            .ok_or_else(|| Status::not_found("tenant does not exist"))?;
        let bucket_id = self
            .resolve_bucket_id(tenant_id, bucket)
            .await?
            .ok_or_else(|| Status::not_found("bucket does not exist"))?;
        Ok((tenant_id, bucket_id))
    }

    /// Resolve stable IDs back to their current mutable names through the
    /// existing quorum-reconciled bucket record. Internal maintenance uses
    /// this immediately before entering the ordinary object path; it is not a
    /// second name authority or a cached reverse catalogue.
    pub(crate) async fn resolve_bucket_names(
        &self,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<(String, String), Status> {
        if tenant_id == 0 || bucket_id == 0 {
            return Err(Status::invalid_argument(
                "tenant and bucket IDs must be non-zero",
            ));
        }
        match self
            .read_record(&LogicalRecordId::BucketRecord {
                tenant_id,
                bucket_id,
            })
            .await?
        {
            Some(LogicalRecordValue::BucketRecord(record))
                if record.tenant_id == tenant_id && record.bucket_id == bucket_id =>
            {
                Ok((record.storage_tenant.as_str().to_owned(), record.bucket))
            }
            Some(_) => Err(Status::data_loss(
                "bucket-record coordinator returned another logical record",
            )),
            None => Err(Status::unavailable(
                "bucket record is unavailable during internal maintenance",
            )),
        }
    }

    async fn read_record(
        &self,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordValue>, Status> {
        let started = Instant::now();
        let mut last_unavailable = None;
        loop {
            let targets = match self.records.read_targets_local_first(id) {
                Ok(targets) => targets,
                Err(error) if retryable_read_availability(&error) => {
                    last_unavailable = Some(error);
                    wait_for_logical_name_retry(started).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            for (index, target) in targets.iter().enumerate() {
                let timeout = read_attempt_timeout(started, targets.len() - index)?;
                let result = if self.records.is_local_read_target(target) {
                    tokio::time::timeout(timeout, self.records.read(id))
                        .await
                        .unwrap_or_else(|_| {
                            Err(Status::deadline_exceeded("logical-name read timed out"))
                        })
                } else {
                    self.peers.read_logical_name(target, id, timeout).await
                };
                match result {
                    Ok(value) => {
                        self.records.require_replica_read_target(id, target)?;
                        return Ok(value);
                    }
                    Err(error) if retryable_read_availability(&error) => {
                        last_unavailable = Some(error);
                    }
                    Err(error) => return Err(error),
                }
            }
            if let Err(deadline) = wait_for_logical_name_retry(started).await {
                return Err(last_unavailable.unwrap_or(deadline));
            }
        }
    }
}

async fn wait_for_logical_name_retry(started: Instant) -> Result<(), Status> {
    let remaining = LOGICAL_NAME_READ_TIMEOUT
        .checked_sub(started.elapsed())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| Status::deadline_exceeded("logical-name read deadline exceeded"))?;
    tokio::time::sleep(LOGICAL_NAME_RETRY_INTERVAL.min(remaining)).await;
    Ok(())
}

fn read_attempt_timeout(started: Instant, remaining_targets: usize) -> Result<Duration, Status> {
    let remaining = LOGICAL_NAME_READ_TIMEOUT
        .checked_sub(started.elapsed())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| Status::deadline_exceeded("logical-name read deadline exceeded"))?;
    let divisor = u32::try_from(remaining_targets.max(1)).unwrap_or(u32::MAX);
    Ok(remaining / divisor)
}

fn retryable_read_availability(error: &Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    )
}

#[tonic::async_trait]
impl LogicalNameResolution for LogicalNameResolver {
    async fn resolve_tenant_id(
        &self,
        storage_tenant: &StorageTenantId,
    ) -> Result<Option<u64>, Status> {
        let id = LogicalRecordId::TenantNameClaim {
            storage_tenant: storage_tenant.clone(),
        };
        match self.read_record(&id).await? {
            None => Ok(None),
            Some(LogicalRecordValue::TenantNameClaim {
                storage_tenant: resolved,
                tenant_id,
            }) if resolved == *storage_tenant && tenant_id != 0 => Ok(Some(tenant_id)),
            Some(_) => Err(Status::data_loss(
                "tenant-name coordinator returned another logical record",
            )),
        }
    }

    async fn resolve_bucket_id(&self, tenant_id: u64, bucket: &str) -> Result<Option<u64>, Status> {
        if tenant_id == 0 {
            return Err(Status::invalid_argument("tenant ID must be non-zero"));
        }
        let id = LogicalRecordId::BucketNameClaim {
            tenant_id,
            bucket: bucket.to_owned(),
        };
        match self.read_record(&id).await? {
            None => Ok(None),
            Some(LogicalRecordValue::BucketNameClaim {
                tenant_id: resolved_tenant,
                bucket: resolved_bucket,
                bucket_id,
            }) if resolved_tenant == tenant_id && resolved_bucket == bucket && bucket_id != 0 => {
                Ok(Some(bucket_id))
            }
            Some(_) => Err(Status::data_loss(
                "bucket-name coordinator returned another logical record",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_name_failover_retries_only_availability_failures() {
        assert!(retryable_read_availability(&Status::unavailable("down")));
        assert!(retryable_read_availability(&Status::deadline_exceeded(
            "slow"
        )));
        for code in [
            tonic::Code::NotFound,
            tonic::Code::PermissionDenied,
            tonic::Code::DataLoss,
            tonic::Code::InvalidArgument,
            tonic::Code::Internal,
        ] {
            assert!(!retryable_read_availability(&Status::new(code, "closed")));
        }
    }
}
