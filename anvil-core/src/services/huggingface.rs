use crate::{AppState, access_control, auth, permissions::AnvilAction, tasks::TaskType};
use tonic::{Request, Response, Status};

use crate::anvil_api as api;

#[tonic::async_trait]

impl api::hugging_face_key_service_server::HuggingFaceKeyService for AppState {
    async fn create_key(
        &self,
        request: Request<api::CreateHfKeyRequest>,
    ) -> Result<Response<api::CreateHfKeyResponse>, Status> {
        let (_metadata, extensions, req) = request.into_parts();
        let claims = auth::try_get_claims_from_extensions(&extensions)
            .ok_or_else(|| Status::unauthenticated("Missing authentication claims"))?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::HfKeyCreate,
            &req.name,
        )
        .await?;

        if req.name.trim().is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }

        let enc = self
            .secret_keyring
            .encrypt(req.token.as_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;

        let note_opt = if req.note.is_empty() {
            None
        } else {
            Some(req.note.as_str())
        };

        self.persistence
            .hf_create_key(claims.tenant_id, &req.name, &enc, note_opt)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;

        let resp = api::CreateHfKeyResponse {
            name: req.name,
            note: req.note,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        Ok(Response::new(resp))
    }

    async fn delete_key(
        &self,
        request: Request<api::DeleteHfKeyRequest>,
    ) -> Result<Response<api::DeleteHfKeyResponse>, Status> {
        let (_metadata, extensions, req) = request.into_parts();
        let claims = auth::try_get_claims_from_extensions(&extensions)
            .ok_or_else(|| Status::unauthenticated("Missing authentication claims"))?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::HfKeyDelete,
            &req.name,
        )
        .await?;

        let n = self
            .persistence
            .hf_delete_key(claims.tenant_id, &req.name)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;

        if n == 0 {
            return Err(Status::not_found("key not found"));
        }

        Ok(Response::new(api::DeleteHfKeyResponse {}))
    }

    async fn list_keys(
        &self,
        request: Request<api::ListHfKeysRequest>,
    ) -> Result<Response<api::ListHfKeysResponse>, Status> {
        let (_metadata, extensions, req) = request.into_parts();
        let claims = auth::try_get_claims_from_extensions(&extensions)
            .ok_or_else(|| Status::unauthenticated("Missing authentication claims"))?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::HfKeyList,
            "*",
        )
        .await?;

        let page_size = crate::services::collection_cursor::page_size(req.page.as_ref())?;
        let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
        let revision = crate::hf_journal::hf_collection_revision(&self.storage)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        let binding = crate::services::collection_cursor::CollectionCursorBinding {
            service_method: "anvil.HfKeyService/ListKeys",
            filters: &[],
            principal_scope: &principal_scope,
            page_size,
            revision: &revision,
            sort: "name.asc",
        };
        let after_cursor = crate::services::collection_cursor::decode_page_token(
            req.page.as_ref(),
            &binding,
            self.config.jwt_secret.as_bytes(),
        )?
        .map(hex::decode)
        .transpose()
        .map_err(|_| Status::invalid_argument("invalid Hugging Face key cursor"))?;
        let page = self
            .persistence
            .hf_list_key_page(claims.tenant_id, after_cursor.as_deref(), page_size)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        if crate::hf_journal::hf_collection_revision(&self.storage)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            != revision
        {
            return Err(Status::aborted(
                "Hugging Face key collection changed while reading this page",
            ));
        }
        let next_page_token = page.next_cursor.map_or(Ok(String::new()), |cursor| {
            crate::services::collection_cursor::encode_next_page_token(
                &hex::encode(cursor),
                &binding,
                self.config.jwt_secret.as_bytes(),
            )
        })?;
        let keys = page
            .keys
            .into_iter()
            .map(|key| api::HfKey {
                name: key.name,
                note: key.note.unwrap_or_default(),
                created_at: key.created_at.to_rfc3339(),
                updated_at: key.updated_at.to_rfc3339(),
            })
            .collect();
        Ok(Response::new(api::ListHfKeysResponse {
            keys,
            page: Some(api::PageResponse { next_page_token }),
        }))
    }
}

#[tonic::async_trait]
impl api::hf_ingestion_service_server::HfIngestionService for AppState {
    async fn start_ingestion(
        &self,
        request: Request<api::StartHfIngestionRequest>,
    ) -> Result<Response<api::StartHfIngestionResponse>, Status> {
        tracing::info!(?request, "ENTERED start_ingestion");
        let (_metadata, extensions, req) = request.into_parts();
        if req.key_name.is_empty() || req.repo.is_empty() || req.target_bucket.is_empty() {
            return Err(Status::invalid_argument(
                "key_name, repo and target_bucket required",
            ));
        }

        let claims = auth::try_get_claims_from_extensions(&extensions)
            .ok_or_else(|| Status::unauthenticated("Missing authentication claims"))?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::HfIngestionCreate,
            "*",
        )
        .await?;

        tracing::info!("Authorization successful for start_ingestion");
        // Lookup key id
        let Some((key_id, _enc)) = self
            .persistence
            .hf_get_key_encrypted(claims.tenant_id, &req.key_name)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?
        else {
            return Err(Status::not_found("key not found"));
        };
        let app_id = claims
            .sub
            .parse::<i64>()
            .map_err(|_| Status::unauthenticated("Invalid app ID in token"))?;

        let app = self
            .persistence
            .get_app_by_id(app_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::unauthenticated("Invalid app ID in token"))?;

        let ingestion_id = self
            .persistence
            .hf_create_ingestion(
                key_id,
                claims.tenant_id,
                app.id,
                &req.repo,
                if req.revision.is_empty() {
                    None
                } else {
                    Some(req.revision.as_str())
                },
                &req.target_bucket,
                &req.target_region,
                if req.target_prefix.is_empty() {
                    None
                } else {
                    Some(req.target_prefix.as_str())
                },
                &req.include_globs,
                &req.exclude_globs,
            )
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        // Enqueue task
        let payload = serde_json::json!({"ingestion_id": ingestion_id});
        self.persistence
            .enqueue_task(TaskType::HFIngestion, payload, 100)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        Ok(Response::new(api::StartHfIngestionResponse {
            ingestion_id: ingestion_id.to_string(),
        }))
    }

    async fn get_ingestion_status(
        &self,
        request: Request<api::GetHfIngestionStatusRequest>,
    ) -> Result<Response<api::GetHfIngestionStatusResponse>, Status> {
        let (_metadata, extensions, req) = request.into_parts();
        let claims = auth::try_get_claims_from_extensions(&extensions)
            .ok_or_else(|| Status::unauthenticated("Missing authentication claims"))?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::HfIngestionRead,
            &req.ingestion_id,
        )
        .await?;

        let id: i64 = req
            .ingestion_id
            .parse()
            .map_err(|_| Status::invalid_argument("invalid id"))?;
        let _job = self
            .persistence
            .hf_get_ingestion_job(id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .filter(|job| job.tenant_id == claims.tenant_id)
            .ok_or_else(|| Status::not_found("ingestion not found"))?;
        let status = self
            .persistence
            .hf_get_ingestion_status(id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(api::GetHfIngestionStatusResponse {
            state: status.state.as_str().to_string(),
            queued: status.queued as u64,
            downloading: status.downloading as u64,
            stored: status.stored as u64,
            failed: status.failed as u64,
            error: status.error.unwrap_or_default(),
            created_at: status.created_at.to_rfc3339(),
            started_at: status
                .started_at
                .map(|d: chrono::DateTime<chrono::Utc>| d.to_rfc3339())
                .unwrap_or_default(),
            finished_at: status
                .finished_at
                .map(|d: chrono::DateTime<chrono::Utc>| d.to_rfc3339())
                .unwrap_or_default(),
        }))
    }

    async fn cancel_ingestion(
        &self,
        request: Request<api::CancelHfIngestionRequest>,
    ) -> Result<Response<api::CancelHfIngestionResponse>, Status> {
        let (_metadata, extensions, req) = request.into_parts();
        let claims = auth::try_get_claims_from_extensions(&extensions)
            .ok_or_else(|| Status::unauthenticated("Missing authentication claims"))?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::HfIngestionDelete,
            &req.ingestion_id,
        )
        .await?;

        let id: i64 = req
            .ingestion_id
            .parse()
            .map_err(|_| Status::invalid_argument("invalid id"))?;
        self.persistence
            .hf_get_ingestion_job(id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .filter(|job| job.tenant_id == claims.tenant_id)
            .ok_or_else(|| Status::not_found("ingestion not found"))?;
        let _ = self
            .persistence
            .hf_cancel_ingestion(id)
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        Ok(Response::new(api::CancelHfIngestionResponse {}))
    }
}
