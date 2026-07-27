use crate::{AppState, access_control, auth, permissions::AnvilAction};
use std::time::Duration;
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
        let transaction = begin_hf_mutation(
            self,
            &claims,
            req.options.as_ref(),
            &format!("hf-key-create:{}:{}", claims.tenant_id, req.name),
        )
        .await?;
        if transaction.replayed {
            let key = crate::hf_journal::get_key_record(
                &self.mvcc,
                claims.tenant_id,
                &req.name,
            )
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::failed_precondition("committed HF key is unavailable"))?;
            return Ok(Response::new(api::CreateHfKeyResponse {
                name: key.name,
                note: key.note.unwrap_or_default(),
                created_at: key.created_at.to_rfc3339(),
                write_state: api::WriteState::Committed as i32,
            }));
        }
        self.persistence
            .hf_stage_create_key(
                claims.tenant_id,
                &req.name,
                &enc,
                note_opt,
                &transaction.id,
                &transaction.principal,
                transaction.now_unix_ms,
            )
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        commit_hf_mutation(self, &transaction).await?;
        let created_at = if transaction.internal {
            crate::hf_journal::get_key_record(&self.mvcc, claims.tenant_id, &req.name)
                .map_err(|error| Status::internal(error.to_string()))?
                .map(|key| key.created_at.to_rfc3339())
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339())
        } else {
            chrono::Utc::now().to_rfc3339()
        };
        let resp = api::CreateHfKeyResponse {
            name: req.name,
            note: req.note,
            created_at,
            write_state: transaction.write_state(),
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

        let transaction = begin_hf_mutation(
            self,
            &claims,
            req.options.as_ref(),
            &format!("hf-key-delete:{}:{}", claims.tenant_id, req.name),
        )
        .await?;
        if transaction.replayed {
            return Ok(Response::new(api::DeleteHfKeyResponse {
                write_state: api::WriteState::Committed as i32,
            }));
        }
        let n = self
            .persistence
            .hf_stage_delete_key(
                claims.tenant_id,
                &req.name,
                &transaction.id,
                &transaction.principal,
                transaction.now_unix_ms,
            )
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;

        if n == 0 {
            return Err(Status::not_found("key not found"));
        }

        commit_hf_mutation(self, &transaction).await?;
        Ok(Response::new(api::DeleteHfKeyResponse {
            write_state: transaction.write_state(),
        }))
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
        let revision = crate::hf_journal::hf_collection_revision(
            self.persistence
                .mvcc()
                .map_err(|error| Status::internal(error.to_string()))?,
        )
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
        if crate::hf_journal::hf_collection_revision(
            self.persistence
                .mvcc()
                .map_err(|error| Status::internal(error.to_string()))?,
        )
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

        let transaction = begin_hf_mutation(
            self,
            &claims,
            req.options.as_ref(),
            &format!(
                "hf-ingestion-start:{}:{}:{}:{}",
                claims.tenant_id, req.repo, req.target_bucket, req.target_prefix
            ),
        )
        .await?;
        if transaction.replayed {
            let record = self
                .mvcc
                .runtime
                .local_store()
                .find_hf_ingestion_postcommit_by_transaction(&transaction.id)
                .map_err(|error| Status::internal(error.to_string()))?
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "committed HF ingestion outcome is unavailable",
                    )
                })?;
            return Ok(Response::new(api::StartHfIngestionResponse {
                ingestion_id: record.job.ingestion_id.to_string(),
                write_state: api::WriteState::Committed as i32,
            }));
        }
        let ingestion_id = self
            .persistence
            .hf_stage_create_ingestion(
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
                &transaction.id,
                &transaction.principal,
                transaction.now_unix_ms,
            )
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        let job = crate::hf_ingestion_postcommit_job::HfIngestionPostCommitJob {
            schema: crate::hf_ingestion_postcommit_job::HfIngestionPostCommitJob::SCHEMA.into(),
            cluster_id: self.mvcc.cluster_id().to_string(),
            transaction_id: transaction.id.clone(),
            ingestion_id,
            tenant_id: claims.tenant_id,
            priority: 100,
        };
        self.mvcc
            .open_transactions
            .add_job(
                &transaction.id,
                job.encode()
                    .map_err(|error| Status::internal(error.to_string()))?,
                transaction.now_unix_ms,
            )
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        commit_hf_mutation(self, &transaction).await?;
        Ok(Response::new(api::StartHfIngestionResponse {
            ingestion_id: ingestion_id.to_string(),
            write_state: transaction.write_state(),
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
        let transaction = begin_hf_mutation(
            self,
            &claims,
            req.options.as_ref(),
            &format!("hf-ingestion-cancel:{}:{id}", claims.tenant_id),
        )
        .await?;
        if transaction.replayed {
            return Ok(Response::new(api::CancelHfIngestionResponse {
                write_state: api::WriteState::Committed as i32,
            }));
        }
        let _ = self
            .persistence
            .hf_stage_cancel_ingestion(
                id,
                &transaction.id,
                &transaction.principal,
                transaction.now_unix_ms,
            )
            .await
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        commit_hf_mutation(self, &transaction).await?;
        Ok(Response::new(api::CancelHfIngestionResponse {
            write_state: transaction.write_state(),
        }))
    }
}

impl AppState {
    pub async fn run_hf_ingestion_postcommit_loop(self) {
        loop {
            if let Err(error) = self.run_hf_ingestion_postcommit_once().await {
                tracing::warn!(%error, "HF ingestion postcommit attempt failed");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn run_hf_ingestion_postcommit_once(&self) -> anyhow::Result<bool> {
        let worker_id = format!(
            "hf-ingestion-postcommit/{}",
            self.persistence.owner_node_id()
        );
        let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
        let Some((job_id, record)) = self
            .mvcc
            .runtime
            .local_store()
            .claim_hf_ingestion_postcommit_authorized(&worker_id, now, 30_000, |record| {
                self.mvcc
                    .claim_assignment(
                        "hf-ingestion-postcommit",
                        &record.job.target_logical_identity(),
                    )
                    .ok()
                    .flatten()
                    .map(|guard| guard.lease_owner(&worker_id))
            })?
        else {
            return Ok(false);
        };
        let guard = self
            .mvcc
            .claim_assignment(
                "hf-ingestion-postcommit",
                &record.job.target_logical_identity(),
            )?
            .ok_or_else(|| anyhow::anyhow!("HF ingestion postcommit assignment changed"))?;
        let lease_owner = guard.lease_owner(&worker_id);
        let payload = serde_json::json!({"ingestion_id": record.job.ingestion_id});
        let result = self
            .persistence
            .enqueue_task_if_absent(
                crate::tasks::TaskType::HFIngestion,
                payload,
                record.job.priority,
            )
            .await;
        match result {
            Ok(_) => {
                self.mvcc.validate_assignment(&guard)?;
                self.mvcc
                    .runtime
                    .local_store()
                    .complete_hf_ingestion_postcommit(&job_id, &lease_owner)?;
                Ok(true)
            }
            Err(error) => {
                let delay =
                    250_u64.saturating_mul(1_u64 << record.attempts.saturating_sub(1).min(10));
                self.mvcc
                    .runtime
                    .local_store()
                    .retry_hf_ingestion_postcommit(
                        &job_id,
                        &lease_owner,
                        now.saturating_add(delay),
                        &error.to_string(),
                    )?;
                Err(error)
            }
        }
    }
}

struct HfMutationTransaction {
    id: String,
    principal: String,
    now_unix_ms: u64,
    internal: bool,
    replayed: bool,
}

impl HfMutationTransaction {
    fn write_state(&self) -> i32 {
        if self.internal {
            api::WriteState::Committed as i32
        } else {
            api::WriteState::Staged as i32
        }
    }
}

async fn begin_hf_mutation(
    state: &AppState,
    claims: &auth::Claims,
    options: Option<&api::WriteOptions>,
    default_idempotency_key: &str,
) -> Result<HfMutationTransaction, Status> {
    let principal = crate::object_manager::transaction_principal_from_claims(claims);
    let now_unix_ms = u64::try_from(chrono::Utc::now().timestamp_millis())
        .map_err(|_| Status::internal("HF mutation timestamp predates Unix epoch"))?;
    if let Some(transaction_id) =
        crate::services::transaction_context::write_options_transaction_id(options)?
    {
        state
            .mvcc
            .open_transactions
            .binding(transaction_id, &principal)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        return Ok(HfMutationTransaction {
            id: transaction_id.to_string(),
            principal,
            now_unix_ms,
            internal: false,
            replayed: false,
        });
    }
    let supplied_idempotency_key = options
        .map(|options| options.idempotency_key.trim())
        .filter(|key| !key.is_empty());
    let generated_idempotency_key;
    let idempotency_key = if let Some(key) = supplied_idempotency_key {
        key
    } else {
        generated_idempotency_key =
            format!("{default_idempotency_key}:{}", uuid::Uuid::new_v4());
        &generated_idempotency_key
    };
    let handle = state
        .mvcc
        .open_transactions
        .begin(
            state.mvcc.runtime.as_ref(),
            state.mvcc.cluster_id().to_string(),
            principal.clone(),
            idempotency_key,
            Duration::from_secs(300),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now_unix_ms,
        )
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let status = state
        .mvcc
        .open_transactions
        .status(&handle.transaction_id, &principal, now_unix_ms)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    if status.state == "committing" {
        let outcome = state
            .mvcc
            .open_transactions
            .commit(
                state.mvcc.runtime.as_ref(),
                &handle.transaction_id,
                &principal,
                now_unix_ms,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
            outcome.certification
        {
            return Err(Status::aborted(format!(
                "implicit HF transaction aborted: {reason:?}"
            )));
        }
    } else if status.state == "aborted" {
        return Err(Status::aborted(
            "implicit HF transaction previously aborted",
        ));
    }
    Ok(HfMutationTransaction {
        id: handle.transaction_id,
        principal,
        now_unix_ms,
        internal: true,
        replayed: matches!(status.state, "committed" | "committing"),
    })
}

async fn commit_hf_mutation(
    state: &AppState,
    transaction: &HfMutationTransaction,
) -> Result<(), Status> {
    if !transaction.internal {
        return Ok(());
    }
    let outcome = state
        .mvcc
        .open_transactions
        .commit(
            state.mvcc.runtime.as_ref(),
            &transaction.id,
            &transaction.principal,
            u64::try_from(chrono::Utc::now().timestamp_millis())
                .map_err(|_| Status::internal("HF commit timestamp predates Unix epoch"))?,
        )
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => Err(Status::aborted(
            format!("implicit HF transaction aborted: {reason:?}"),
        )),
    }
}
