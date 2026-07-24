use super::*;

impl Persistence {
    pub async fn create_model_artifact(
        &self,
        artifact_id: &str,
        bucket_id: i64,
        key: &str,
        manifest: &crate::anvil_api::ModelManifest,
    ) -> Result<()> {
        let permit = self.model_write_permit().await?;
        model_journal::create_model_artifact_with_permit(
            &self.storage,
            artifact_id,
            bucket_id,
            key,
            manifest,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn create_model_tensors(
        &self,
        artifact_id: &str,
        tensors: &[crate::anvil_api::TensorIndexRow],
    ) -> Result<()> {
        let permit = self.model_write_permit().await?;
        model_journal::create_model_tensors_with_permit(
            &self.storage,
            artifact_id,
            tensors,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_tensor_page(
        &self,
        artifact_id: &str,
        after_cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<model_journal::ModelTensorPage> {
        model_journal::list_tensor_page(&self.storage, artifact_id, after_cursor, limit).await
    }

    pub async fn get_tensor_metadata(
        &self,
        artifact_id: &str,
        tensor_name: &str,
    ) -> Result<Option<crate::anvil_api::TensorIndexRow>> {
        model_journal::get_tensor_metadata(&self.storage, artifact_id, tensor_name).await
    }

    pub async fn get_model_artifact(
        &self,
        artifact_id: &str,
    ) -> Result<Option<crate::anvil_api::ModelManifest>> {
        model_journal::get_model_artifact(&self.storage, artifact_id).await
    }

    pub async fn get_tensor_metadata_recursive(
        &self,
        artifact_id: &str,
        tensor_name: &str,
    ) -> Result<Option<crate::anvil_api::TensorIndexRow>> {
        let mut current = artifact_id.to_string();
        let mut seen = HashSet::new();
        while seen.insert(current.clone()) {
            if let Some(tensor) = self.get_tensor_metadata(&current, tensor_name).await? {
                return Ok(Some(tensor));
            }
            let Some(manifest) = self.get_model_artifact(&current).await? else {
                break;
            };
            if manifest.base_artifact_id.is_empty() {
                break;
            }
            current = manifest.base_artifact_id;
        }
        Ok(None)
    }
}
