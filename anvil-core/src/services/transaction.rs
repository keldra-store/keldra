use crate::anvil_api::transaction_service_server::TransactionService;
use crate::anvil_api::*;
use crate::{AppState, auth, middleware};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl TransactionService for AppState {
    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        let request_id = request_id(&request);
        let principal = transaction_principal(&request)?;
        let req = request.into_inner();
        tracing::info!(
            operation = "request.receive",
            rpc = "transaction.begin",
            request_id = %request_id,
            cluster_id = %req.cluster_id,
            session_id = %req.idempotency_key,
            "received public transaction request"
        );
        validate_local_cluster(self, &req.cluster_id)?;
        let consistency = read_consistency(req.read_consistency)?;
        let handle = self
            .mvcc
            .open_transactions
            .begin(
                self.mvcc.runtime.as_ref(),
                req.cluster_id,
                principal,
                req.idempotency_key,
                std::time::Duration::from_millis(req.ttl_ms),
                durability(req.durability)?,
                consistency,
                now_unix_ms()?,
            )
            .await
            .map_err(mvcc_status)?;

        tracing::info!(
            operation = "response.send",
            rpc = "transaction.begin",
            request_id = %request_id,
            cluster_id = %handle.cluster_id,
            transaction_id = %handle.transaction_id,
            session_id = %handle.transaction_id,
            "sending public transaction response"
        );
        Ok(Response::new(BeginTransactionResponse {
            request_id,
            transaction_id: handle.transaction_id,
            expires_at_unix_ms: handle.expires_at_unix_ms,
            state: "open".to_string(),
            snapshot_version: handle.snapshot_version,
            cluster_id: handle.cluster_id,
            durability: durability_to_proto(handle.durability) as i32,
        }))
    }

    async fn commit_transaction(
        &self,
        request: Request<CommitTransactionRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let request_id = request_id(&request);
        let principal = transaction_principal(&request)?;
        let req = request.into_inner();
        tracing::info!(
            operation = "request.receive",
            rpc = "transaction.commit",
            request_id = %request_id,
            cluster_id = %req.cluster_id,
            transaction_id = %req.transaction_id,
            session_id = %req.transaction_id,
            "received public transaction request"
        );
        validate_local_cluster(self, &req.cluster_id)?;
        let outcome = self
            .mvcc
            .open_transactions
            .commit(
                self.mvcc.runtime.as_ref(),
                &req.transaction_id,
                &principal,
                now_unix_ms()?,
            )
            .await
            .map_err(mvcc_status)?;
        let _commit_version = match outcome.certification {
            crate::mvcc_transaction::CertificationResult::Committed { commit_version } => {
                commit_version
            }
            crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
                tracing::warn!(?reason, transaction_id = %req.transaction_id, "MVCC transaction conflicted");
                return Err(Status::aborted(certification_abort_name(&reason)));
            }
        };

        tracing::info!(
            operation = "response.send",
            rpc = "transaction.commit",
            request_id = %request_id,
            cluster_id = %req.cluster_id,
            transaction_id = %req.transaction_id,
            session_id = %req.transaction_id,
            "sending public transaction response"
        );
        Ok(Response::new(WriteResponse {
            request_id,
            mutation_id: req.transaction_id,
            state: WriteState::Committed as i32,
            root_generation: None,
            transaction_manifest_ref: None,
            idempotency_outcome: if matches!(
                outcome.local_apply,
                Some(crate::mvcc_store::ApplyOutcome::Replayed)
            ) {
                "replayed".to_string()
            } else {
                "accepted".to_string()
            },
            retry_after_hint: None,
            finalisation_error: None,
        }))
    }

    async fn rollback_transaction(
        &self,
        request: Request<RollbackTransactionRequest>,
    ) -> Result<Response<RollbackTransactionResponse>, Status> {
        let request_id = request_id(&request);
        let principal = transaction_principal(&request)?;
        let req = request.into_inner();
        validate_local_cluster(self, &req.cluster_id)?;
        let status = self
            .mvcc
            .open_transactions
            .rollback(&req.transaction_id, &principal, now_unix_ms()?)
            .map_err(mvcc_status)?;
        Ok(Response::new(RollbackTransactionResponse {
            request_id,
            transaction_id: status.transaction_id,
            state: status.state.to_string(),
        }))
    }

    async fn get_transaction(
        &self,
        request: Request<GetTransactionRequest>,
    ) -> Result<Response<TransactionStatus>, Status> {
        let principal = transaction_principal(&request)?;
        let req = request.into_inner();
        validate_local_cluster(self, &req.cluster_id)?;
        let status = self
            .mvcc
            .open_transactions
            .status(&req.transaction_id, &principal, now_unix_ms()?)
            .map_err(mvcc_status)?;
        Ok(Response::new(transaction_status(status)))
    }
}

#[cfg(test)]
mod mvcc_lifecycle_tests {
    use super::*;
    use crate::mvcc_open_transactions::TransactionRegistryStatus;
    use crate::mvcc_transaction::{CertificationAbort, CertificationResult, DurabilityLevel};

    #[test]
    fn lifecycle_defaults_are_safe_and_explicit_values_map_exactly() {
        assert_eq!(
            read_consistency(MvccReadConsistency::Unspecified as i32).unwrap(),
            crate::mvcc_transaction::ReadConsistency::Linearized
        );
        assert_eq!(
            read_consistency(MvccReadConsistency::LocalSnapshot as i32).unwrap(),
            crate::mvcc_transaction::ReadConsistency::LocalSnapshot
        );
        assert_eq!(
            durability(MvccDurability::Unspecified as i32).unwrap(),
            crate::mvcc_transaction::DurabilityLevel::Quorum
        );
        assert_eq!(
            durability(MvccDurability::Erasure as i32).unwrap(),
            crate::mvcc_transaction::DurabilityLevel::Erasure
        );
    }

    #[test]
    fn certification_conflicts_have_stable_public_codes() {
        assert_eq!(
            certification_abort_name(
                &crate::mvcc_transaction::CertificationAbort::PointConflict { key_hash: [0; 32] },
            ),
            "TransactionPointConflict"
        );
        assert_eq!(
            certification_abort_name(
                &crate::mvcc_transaction::CertificationAbort::RangeConflict {
                    range_hash: [0; 32],
                },
            ),
            "TransactionRangeConflict"
        );
    }

    #[test]
    fn assignment_conflict_has_stable_public_code() {
        assert_eq!(
            certification_abort_name(&CertificationAbort::AssignmentConflict { partition_id: 41 }),
            "TransactionAssignmentConflict"
        );
    }

    #[test]
    fn registry_status_preserves_public_lifecycle_fields() {
        let status = transaction_status(TransactionRegistryStatus {
            cluster_id: "cluster-a".to_string(),
            transaction_id: "transaction-a".to_string(),
            snapshot_version: 17,
            expires_at_unix_ms: 42,
            state: "committed",
            result: Some(CertificationResult::Committed { commit_version: 23 }),
            durability: DurabilityLevel::Quorum,
        });

        assert_eq!(status.cluster_id, "cluster-a");
        assert_eq!(status.transaction_id, "transaction-a");
        assert_eq!(status.state, "committed");
        assert_eq!(status.snapshot_version, 17);
        assert_eq!(status.expires_at_unix_ms, 42);
        assert_eq!(status.commit_version, Some(23));
        assert_eq!(status.durability, MvccDurability::Quorum as i32);
        assert!(status.error.is_none());
    }

    #[test]
    fn registry_abort_preserves_public_error_semantics() {
        let status = transaction_status(TransactionRegistryStatus {
            cluster_id: "cluster-a".to_string(),
            transaction_id: "transaction-a".to_string(),
            snapshot_version: 17,
            expires_at_unix_ms: 42,
            state: "aborted",
            result: Some(CertificationResult::Aborted {
                reason: CertificationAbort::RangeConflict {
                    range_hash: [7; 32],
                },
            }),
            durability: DurabilityLevel::Local,
        });

        assert_eq!(status.state, "aborted");
        assert_eq!(status.commit_version, None);
        assert_eq!(status.durability, MvccDurability::Local as i32);
        assert_eq!(
            status.error.as_ref().map(|error| error.code.as_str()),
            Some("TransactionRangeConflict")
        );
    }

    #[test]
    fn registry_errors_map_to_stable_rpc_codes() {
        let missing = mvcc_status(anyhow::anyhow!("unknown transaction transaction-a"));
        assert_eq!(missing.code(), tonic::Code::NotFound);
        assert_eq!(missing.message(), "TransactionNotFound");

        let wrong_principal =
            mvcc_status(anyhow::anyhow!("transaction belongs to another principal"));
        assert_eq!(wrong_principal.code(), tonic::Code::PermissionDenied);
        assert_eq!(wrong_principal.message(), "TransactionPrincipalMismatch");

        let expired = mvcc_status(anyhow::anyhow!("transaction expired"));
        assert_eq!(expired.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn cluster_validation_rejects_empty_and_foreign_clusters() {
        let missing = validate_cluster_id("cluster-a", "").unwrap_err();
        assert_eq!(missing.code(), tonic::Code::InvalidArgument);

        let foreign = validate_cluster_id("cluster-a", "cluster-b").unwrap_err();
        assert_eq!(foreign.code(), tonic::Code::FailedPrecondition);

        validate_cluster_id("cluster-a", "cluster-a").unwrap();
    }
}

fn transaction_principal<T>(request: &Request<T>) -> Result<String, Status> {
    Ok(transaction_principal_from_claims(&transaction_claims(
        request,
    )?))
}

fn transaction_claims<T>(request: &Request<T>) -> Result<auth::Claims, Status> {
    request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))
}

fn transaction_principal_from_claims(claims: &auth::Claims) -> String {
    format!("tenant/{}/principal/{}", claims.tenant_id, claims.sub)
}

fn request_id<T>(request: &Request<T>) -> String {
    request
        .extensions()
        .get::<middleware::AnvilRequestId>()
        .map(|request_id| request_id.0.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string())
}

fn transaction_status(
    transaction: crate::mvcc_open_transactions::TransactionRegistryStatus,
) -> TransactionStatus {
    let commit_version = match &transaction.result {
        Some(crate::mvcc_transaction::CertificationResult::Committed { commit_version }) => {
            Some(*commit_version)
        }
        _ => None,
    };
    let error = match &transaction.result {
        Some(crate::mvcc_transaction::CertificationResult::Aborted { reason }) => {
            Some(AnvilError {
                code: certification_abort_name(reason).to_string(),
                message: format!("{reason:?}"),
            })
        }
        _ => None,
    };
    TransactionStatus {
        transaction_id: transaction.transaction_id,
        state: transaction.state.to_string(),
        error,
        snapshot_version: transaction.snapshot_version,
        expires_at_unix_ms: transaction.expires_at_unix_ms,
        commit_version,
        cluster_id: transaction.cluster_id,
        durability: durability_to_proto(transaction.durability) as i32,
    }
}

fn read_consistency(value: i32) -> Result<crate::mvcc_transaction::ReadConsistency, Status> {
    match MvccReadConsistency::try_from(value)
        .map_err(|_| Status::invalid_argument("invalid MVCC read consistency"))?
    {
        MvccReadConsistency::Unspecified | MvccReadConsistency::Linearized => {
            Ok(crate::mvcc_transaction::ReadConsistency::Linearized)
        }
        MvccReadConsistency::LocalSnapshot => {
            Ok(crate::mvcc_transaction::ReadConsistency::LocalSnapshot)
        }
    }
}

fn durability(value: i32) -> Result<crate::mvcc_transaction::DurabilityLevel, Status> {
    match MvccDurability::try_from(value)
        .map_err(|_| Status::invalid_argument("invalid MVCC durability"))?
    {
        MvccDurability::Unspecified | MvccDurability::Quorum => {
            Ok(crate::mvcc_transaction::DurabilityLevel::Quorum)
        }
        MvccDurability::Local => Ok(crate::mvcc_transaction::DurabilityLevel::Local),
        MvccDurability::Erasure => Ok(crate::mvcc_transaction::DurabilityLevel::Erasure),
    }
}

fn durability_to_proto(value: crate::mvcc_transaction::DurabilityLevel) -> MvccDurability {
    match value {
        crate::mvcc_transaction::DurabilityLevel::Local => MvccDurability::Local,
        crate::mvcc_transaction::DurabilityLevel::Quorum => MvccDurability::Quorum,
        crate::mvcc_transaction::DurabilityLevel::Erasure => MvccDurability::Erasure,
    }
}

fn mvcc_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains("unknown transaction") {
        Status::not_found("TransactionNotFound")
    } else if message.contains("another principal") {
        Status::permission_denied("TransactionPrincipalMismatch")
    } else if message.contains("expired")
        || message.contains("rolled back")
        || message.contains("no longer open")
        || message.contains("can no longer")
    {
        Status::failed_precondition(message)
    } else if message.contains("must be") || message.contains("invalid") {
        Status::invalid_argument(message)
    } else {
        Status::internal(message)
    }
}

fn certification_abort_name(reason: &crate::mvcc_transaction::CertificationAbort) -> &'static str {
    match reason {
        crate::mvcc_transaction::CertificationAbort::InvalidCommand(_) => {
            "TransactionInvalidCommand"
        }
        crate::mvcc_transaction::CertificationAbort::PointConflict { .. } => {
            "TransactionPointConflict"
        }
        crate::mvcc_transaction::CertificationAbort::RangeConflict { .. } => {
            "TransactionRangeConflict"
        }
        crate::mvcc_transaction::CertificationAbort::PredicateConflict { .. } => {
            "TransactionPredicateConflict"
        }
        crate::mvcc_transaction::CertificationAbort::AssignmentConflict { .. } => {
            "TransactionAssignmentConflict"
        }
    }
}

fn validate_local_cluster(state: &AppState, cluster_id: &str) -> Result<(), Status> {
    validate_cluster_id(&state.config.mvcc_cluster_id, cluster_id)
}

fn validate_cluster_id(local: &str, cluster_id: &str) -> Result<(), Status> {
    if cluster_id.trim().is_empty() {
        return Err(Status::invalid_argument("cluster_id is required"));
    }
    if cluster_id != local {
        return Err(Status::failed_precondition(
            "transaction belongs to another cluster",
        ));
    }
    Ok(())
}

fn now_unix_ms() -> Result<u64, Status> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before Unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| Status::internal("system clock exceeds u64 milliseconds"))
}
