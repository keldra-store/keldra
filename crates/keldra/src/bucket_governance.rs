//! Coordinator-reconciled bucket options and admission policy.
//!
//! Bucket governance remains ordinary complete logical records. Exact object
//! owners receive the typed current values; they never depend on an unrelated
//! replica-local `bucket_options` or `policies` column-family entry.

use std::time::{Duration, Instant};

use keldra_store::{
    BucketPolicy, LogicalRecordId, LogicalRecordValue, ObjectMutationGovernance, ObjectVersioning,
};
use tonic::Status;

use crate::cluster_peer::ClusterPeerTransport;
use crate::logical_name_resolution::LogicalNameResolver;
use crate::logical_record_distribution::{LogicalRecordDistribution, LogicalRecordReadTarget};

const GOVERNANCE_READ_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct BucketGovernance {
    records: LogicalRecordDistribution,
    peers: ClusterPeerTransport,
    names: LogicalNameResolver,
}

impl BucketGovernance {
    pub(crate) fn new(
        records: LogicalRecordDistribution,
        peers: ClusterPeerTransport,
        names: LogicalNameResolver,
    ) -> Self {
        Self {
            records,
            peers,
            names,
        }
    }

    pub(crate) async fn resolve(
        &self,
        tenant: &str,
        bucket: &str,
    ) -> Result<ObjectMutationGovernance, Status> {
        let (tenant_id, bucket_id) = self.names.resolve_bucket_ids(tenant, bucket).await?;
        let options_id = LogicalRecordId::BucketOptions {
            tenant_id,
            bucket_id,
        };
        let policy_id = LogicalRecordId::BucketPolicy {
            tenant_id,
            bucket_id,
        };
        let (options, policy) =
            tokio::try_join!(self.read_record(&options_id), self.read_record(&policy_id))?;
        let versioning = match options {
            Some(LogicalRecordValue::BucketOptions {
                tenant_id: observed_tenant,
                bucket_id: observed_bucket,
                versioning,
            }) if observed_tenant == tenant_id && observed_bucket == bucket_id => versioning,
            None => {
                return Err(Status::data_loss(
                    "bucket identity exists without its options record",
                ));
            }
            Some(_) => {
                return Err(Status::data_loss(
                    "bucket options coordinator returned another logical record",
                ));
            }
        };
        let policy = match policy {
            Some(LogicalRecordValue::BucketPolicy {
                tenant_id: observed_tenant,
                bucket_id: observed_bucket,
                policy,
            }) if observed_tenant == tenant_id && observed_bucket == bucket_id => policy,
            None => BucketPolicy::default(),
            Some(_) => {
                return Err(Status::data_loss(
                    "bucket policy coordinator returned another logical record",
                ));
            }
        };
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning,
            policy,
        };
        governance
            .validate()
            .map_err(|error| Status::data_loss(error.to_string()))?;
        Ok(governance)
    }

    pub(crate) async fn set_policy_local(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        policy: BucketPolicy,
    ) -> Result<(), Status> {
        policy
            .validate()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let value = LogicalRecordValue::BucketPolicy {
            tenant_id,
            bucket_id,
            policy,
        };
        if self.records.read_target(&value.id())?.is_some() {
            return Err(Status::failed_precondition(
                "bucket policy request did not reach its logical-record coordinator",
            ));
        }
        self.records.mutate(value).await?;
        Ok(())
    }

    pub(crate) fn policy_target(
        &self,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<LogicalRecordReadTarget>, Status> {
        self.records.read_target(&LogicalRecordId::BucketPolicy {
            tenant_id,
            bucket_id,
        })
    }

    pub(crate) fn require_policy_target(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        expected: &LogicalRecordReadTarget,
    ) -> Result<(), Status> {
        self.records.require_read_target(
            &LogicalRecordId::BucketPolicy {
                tenant_id,
                bucket_id,
            },
            expected,
        )
    }

    async fn read_record(
        &self,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordValue>, Status> {
        let targets = self.records.read_targets_local_first(id)?;
        let started = Instant::now();
        let mut last_unavailable = None;
        for (index, target) in targets.iter().enumerate() {
            let timeout = governance_attempt_timeout(started, targets.len() - index)?;
            let result = if self.records.is_local_read_target(target) {
                tokio::time::timeout(timeout, self.records.read(id))
                    .await
                    .unwrap_or_else(|_| {
                        Err(Status::deadline_exceeded(
                            "bucket-governance read timed out",
                        ))
                    })
            } else {
                self.peers
                    .read_coordinated_logical_record(target.node_id, &target.address, id, timeout)
                    .await
            };
            match result {
                Ok(value) => {
                    self.records.require_replica_read_target(id, target)?;
                    return Ok(value);
                }
                Err(error) if retryable_governance_availability(&error) => {
                    last_unavailable = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_unavailable.unwrap_or_else(|| {
            Status::unavailable("bucket governance has no available read replica")
        }))
    }
}

fn governance_attempt_timeout(
    started: Instant,
    remaining_targets: usize,
) -> Result<Duration, Status> {
    let remaining = GOVERNANCE_READ_TIMEOUT
        .checked_sub(started.elapsed())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| Status::deadline_exceeded("bucket-governance read deadline exceeded"))?;
    let divisor = u32::try_from(remaining_targets.max(1)).unwrap_or(u32::MAX);
    Ok(remaining / divisor)
}

fn retryable_governance_availability(error: &Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    )
}

#[cfg(test)]
mod failover_tests {
    use super::*;

    #[test]
    fn governance_failover_retries_only_availability_failures() {
        assert!(retryable_governance_availability(&Status::unavailable(
            "down"
        )));
        assert!(retryable_governance_availability(
            &Status::deadline_exceeded("slow")
        ));
        for code in [
            tonic::Code::NotFound,
            tonic::Code::PermissionDenied,
            tonic::Code::DataLoss,
            tonic::Code::InvalidArgument,
            tonic::Code::Internal,
        ] {
            assert!(!retryable_governance_availability(&Status::new(
                code, "closed"
            )));
        }
    }
}

pub(crate) fn require_versioning_enabled(
    governance: &ObjectMutationGovernance,
) -> Result<(), Status> {
    if governance.versioning == ObjectVersioning::Enabled {
        Ok(())
    } else {
        Err(Status::failed_precondition(
            "bucket versioning is not enabled",
        ))
    }
}
