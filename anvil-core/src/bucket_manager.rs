use crate::{
    access_control, auth, bucket_journal,
    permissions::AnvilAction,
    persistence::{Bucket, Persistence},
    storage::Storage,
    tasks::TaskType,
    validation,
};
use tonic::Status;

#[derive(Debug, Clone)]
pub struct BucketManager {
    persistence: Persistence,
    storage: Storage,
}

impl BucketManager {
    pub fn new(persistence: Persistence, storage: Storage) -> Self {
        Self {
            persistence,
            storage,
        }
    }

    pub async fn create_bucket(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        region: &str,
    ) -> Result<Bucket, Status> {
        tracing::debug!(
            "[manager] ENTERING create_bucket for bucket: {}",
            bucket_name
        );
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::BucketCreate,
            bucket_name,
        )
        .await?;

        tracing::debug!("[manager] Creating bucket metadata: {}", bucket_name);
        let bucket = self
            .persistence
            .create_bucket(claims.tenant_id, bucket_name, region)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        access_control::grant_bucket_defaults(
            &self.persistence,
            &bucket,
            &claims.sub,
            &claims.sub,
            "grant creator bucket owner",
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        tracing::debug!(
            "[manager] EXITING create_bucket for bucket: {}",
            bucket_name
        );
        Ok(bucket)
    }

    pub async fn delete_bucket(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
    ) -> Result<Bucket, Status> {
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::BucketDelete,
            bucket_name,
        )
        .await?;

        let existing_bucket =
            bucket_journal::read_current_bucket(&self.storage, claims.tenant_id, bucket_name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Bucket not found"))?;
        if self
            .persistence
            .bucket_has_retained_objects_or_uploads(existing_bucket.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            return Err(Status::failed_precondition("Bucket not empty"));
        }

        let bucket = self
            .persistence
            .soft_delete_bucket(claims.tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        // Enqueue a task for physical deletion
        let payload = serde_json::json!({ "bucket_id": bucket.id });
        self.persistence
            .enqueue_task(TaskType::DeleteBucket, payload, 100)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(bucket)
    }

    pub async fn authorize_bucket_list(&self, claims: &auth::Claims) -> Result<(), Status> {
        let tenant_id = claims.tenant_id;
        tracing::debug!(
            "[manager] authorizing bucket list for tenant: {}",
            tenant_id
        );
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::BucketList,
            "*",
        )
        .await?;
        Ok(())
    }

    pub async fn get_bucket_policy(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
    ) -> Result<serde_json::Value, Status> {
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::BucketRead,
            bucket_name,
        )
        .await?;

        let bucket =
            bucket_journal::read_current_bucket(&self.storage, claims.tenant_id, bucket_name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Bucket not found"))?;

        Ok(serde_json::json!({
            "is_public_read": bucket.is_public_read,
        }))
    }

    pub async fn set_bucket_public_access(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        is_public: bool,
    ) -> Result<Bucket, Status> {
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::BucketWrite,
            bucket_name,
        )
        .await?;

        let bucket = self
            .persistence
            .set_bucket_public_access(claims.tenant_id, bucket_name, is_public)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        access_control::write_bucket_public_read_tuple(
            &self.persistence,
            &bucket,
            is_public,
            &claims.sub,
            "bucket public-read policy update",
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(bucket)
    }
}
