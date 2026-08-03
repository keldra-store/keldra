use anvil_store::{LogicalRecordId, LogicalRecordValue, StorageTenantId};
use tonic::{Request, Response, Status};

use super::{
    CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, ClusterPeerTransport,
    MAX_CLUSTER_OPERATION_TIME, require_response_schema, wire,
};
use crate::logical_name_resolution::LogicalNameResolution;
use crate::logical_record_distribution::LogicalRecordReadTarget;

impl ClusterPeerService {
    pub(super) async fn resolve_tenant_name_call(
        &self,
        request: Request<wire::ResolveTenantNameRequest>,
    ) -> Result<Response<wire::ResolveTenantNameResult>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let tenant = StorageTenantId::parse(&request.get_ref().storage_tenant)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let tenant_id = tokio::time::timeout(
            admitted.timeout,
            self.name_resolution.resolve_tenant_id(&tenant),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("tenant-name resolution deadline exceeded"))??;
        if tenant_id == Some(0) {
            return Err(Status::data_loss(
                "tenant-name coordinator returned a zero stable ID",
            ));
        }
        Ok(Response::new(wire::ResolveTenantNameResult {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            present: tenant_id.is_some(),
            tenant_id: tenant_id.unwrap_or_default(),
        }))
    }

    pub(super) async fn resolve_bucket_name_call(
        &self,
        request: Request<wire::ResolveBucketNameRequest>,
    ) -> Result<Response<wire::ResolveBucketNameResult>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let raw = request.get_ref();
        if raw.tenant_id == 0 || raw.bucket.is_empty() {
            return Err(Status::invalid_argument(
                "bucket-name lookup requires a non-zero tenant ID and bucket",
            ));
        }
        let bucket_id = tokio::time::timeout(
            admitted.timeout,
            self.name_resolution
                .resolve_bucket_id(raw.tenant_id, &raw.bucket),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("bucket-name resolution deadline exceeded"))??;
        if bucket_id == Some(0) {
            return Err(Status::data_loss(
                "bucket-name coordinator returned a zero stable ID",
            ));
        }
        Ok(Response::new(wire::ResolveBucketNameResult {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            present: bucket_id.is_some(),
            bucket_id: bucket_id.unwrap_or_default(),
        }))
    }
}

impl ClusterPeerTransport {
    pub(crate) async fn read_logical_name(
        &self,
        target: &LogicalRecordReadTarget,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordValue>, Status> {
        match id {
            LogicalRecordId::TenantNameClaim { storage_tenant } => {
                let response = self
                    .client(target.node_id, &target.address)?
                    .resolve_tenant_name(wire::ResolveTenantNameRequest {
                        peer: Some(self.context(
                            target.placement_fence,
                            0,
                            MAX_CLUSTER_OPERATION_TIME,
                        )?),
                        storage_tenant: storage_tenant.as_str().to_owned(),
                    })
                    .await?
                    .into_inner();
                require_response_schema(response.schema_version)?;
                decode_tenant_name(storage_tenant, response)
            }
            LogicalRecordId::BucketNameClaim { tenant_id, bucket } => {
                let response = self
                    .client(target.node_id, &target.address)?
                    .resolve_bucket_name(wire::ResolveBucketNameRequest {
                        peer: Some(self.context(
                            target.placement_fence,
                            0,
                            MAX_CLUSTER_OPERATION_TIME,
                        )?),
                        tenant_id: *tenant_id,
                        bucket: bucket.clone(),
                    })
                    .await?
                    .into_inner();
                require_response_schema(response.schema_version)?;
                decode_bucket_name(*tenant_id, bucket, response)
            }
            _ => Err(Status::invalid_argument(
                "typed name lookup supports only tenant and bucket name claims",
            )),
        }
    }
}

fn decode_tenant_name(
    tenant: &StorageTenantId,
    response: wire::ResolveTenantNameResult,
) -> Result<Option<LogicalRecordValue>, Status> {
    match (response.present, response.tenant_id) {
        (false, 0) => Ok(None),
        (true, tenant_id) if tenant_id != 0 => Ok(Some(LogicalRecordValue::TenantNameClaim {
            storage_tenant: tenant.clone(),
            tenant_id,
        })),
        _ => Err(Status::data_loss(
            "tenant-name response presence and stable ID disagree",
        )),
    }
}

fn decode_bucket_name(
    tenant_id: u64,
    bucket: &str,
    response: wire::ResolveBucketNameResult,
) -> Result<Option<LogicalRecordValue>, Status> {
    match (response.present, response.bucket_id) {
        (false, 0) => Ok(None),
        (true, bucket_id) if bucket_id != 0 => Ok(Some(LogicalRecordValue::BucketNameClaim {
            tenant_id,
            bucket: bucket.to_owned(),
            bucket_id,
        })),
        _ => Err(Status::data_loss(
            "bucket-name response presence and stable ID disagree",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_response_requires_canonical_presence_pair() {
        let tenant = StorageTenantId::parse("tenant").unwrap();
        assert_eq!(
            decode_tenant_name(
                &tenant,
                wire::ResolveTenantNameResult {
                    schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                    present: false,
                    tenant_id: 0,
                },
            )
            .unwrap(),
            None
        );
        assert!(
            decode_tenant_name(
                &tenant,
                wire::ResolveTenantNameResult {
                    schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                    present: false,
                    tenant_id: 9,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn bucket_response_returns_only_the_requested_typed_claim() {
        assert_eq!(
            decode_bucket_name(
                7,
                "objects",
                wire::ResolveBucketNameResult {
                    schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                    present: true,
                    bucket_id: 11,
                },
            )
            .unwrap(),
            Some(LogicalRecordValue::BucketNameClaim {
                tenant_id: 7,
                bucket: "objects".into(),
                bucket_id: 11,
            })
        );
        assert!(
            decode_bucket_name(
                7,
                "objects",
                wire::ResolveBucketNameResult {
                    schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                    present: true,
                    bucket_id: 0,
                },
            )
            .is_err()
        );
    }
}
