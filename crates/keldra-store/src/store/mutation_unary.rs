use super::journal_capacity::SourceJournalAdmission;
use super::*;
use crate::model::{CoordinatedObjectMutation, ObjectMutationContext, ObjectMutationGovernance};

impl Store {
    pub async fn put(&self, request: PutRequest) -> Result<MutationReceipt, MutationError> {
        unary(self.bulk_write(vec![BatchOperation::Put(request)]).await)
    }

    pub async fn publish(&self, request: PublishRequest) -> Result<MutationReceipt, MutationError> {
        unary(
            self.bulk_write(vec![BatchOperation::Publish(request)])
                .await,
        )
    }

    pub async fn clone_object(
        &self,
        request: CloneRequest,
    ) -> Result<MutationReceipt, MutationError> {
        unary(self.bulk_write(vec![BatchOperation::Clone(request)]).await)
    }

    pub async fn delete(&self, request: DeleteRequest) -> Result<MutationReceipt, MutationError> {
        unary(self.bulk_write(vec![BatchOperation::Delete(request)]).await)
    }

    /// Coordinates a distributed publish whose payload evidence was verified
    /// by the cluster layer; the metadata coordinator need not hold its bytes.
    pub async fn coordinate_distributed_publish(
        &self,
        request: PublishRequest,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        let _policy_guard = self.policy_gate.read().await;
        let identity = self.resolve_bucket_identity(request.key.tenant(), request.key.bucket())?;
        let governance = ObjectMutationGovernance {
            tenant_id: identity.tenant_id.0,
            bucket_id: identity.bucket_id.0,
            versioning: self.bucket_versioning_by_key(&identity.encode())?,
            policy: self
                .bucket_policy_by_key(&identity.encode())?
                .unwrap_or_default(),
        };
        self.coordinate_distributed_publish_with_governance(request, governance, context)
            .await
    }

    pub async fn coordinate_distributed_publish_with_governance(
        &self,
        request: PublishRequest,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        let identity = BucketIdentity {
            tenant_id: TenantId(governance.tenant_id),
            bucket_id: BucketId(governance.bucket_id),
        };
        let prepared = self.prepare_verified_distributed_publish(request, identity)?;
        self.coordinate_prepared_object_mutation(
            prepared,
            context,
            governance,
            None,
            SourceJournalAdmission::Bounded,
        )
        .await
    }

    pub(super) fn prepare_verified_distributed_publish(
        &self,
        request: PublishRequest,
        identity: BucketIdentity,
    ) -> Result<PreparedOperation, MutationError> {
        validate_command_id(request.command_id.as_deref())?;
        let fingerprint = publish_fingerprint(&request, identity);
        Ok(PreparedOperation::Publish {
            request,
            identity,
            fingerprint,
        })
    }
}

fn unary(mut outcomes: Vec<BatchOutcome>) -> Result<MutationReceipt, MutationError> {
    outcomes
        .pop()
        .expect("one operation has one outcome")
        .result
}
