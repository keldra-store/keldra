use crate::anvil_api::personal_db_service_server::PersonalDbService;
use crate::anvil_api::*;
use crate::{
    AppState, access_control,
    anvil_personaldb_sqlite_changeset::iterate_changeset,
    auth, authz_journal,
    authz_scope::{DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace},
    error_codes::AnvilErrorCode,
    formats::{Hash32, hash32, personaldb::PersonalDbLogRecord as CorePersonalDbLogRecord},
    permissions::AnvilAction,
    personaldb_catchup::{
        PersonalDbCatchUpRequest as CoreCatchUpRequest,
        PersonalDbCatchUpResponse as CoreCatchUpResponse, PersonalDbSnapshotRestoreReason,
        personaldb_catch_up,
    },
    personaldb_commit_store::{
        prepare_and_stage_personaldb_changeset_payload,
        prepare_and_stage_personaldb_commit_certificate, read_personaldb_commit_certificate_ref,
    },
    personaldb_control::{PersonalDbCommitCertificate, PersonalDbGroupManifest},
    personaldb_coremeta::PersonalDbWritePlan,
    personaldb_envelope::{
        PersonalDbEnvelopeDerivationInput, TableOperation, VerifiedMutationEnvelope,
        derive_verified_mutation_envelope,
    },
    personaldb_heads::{
        PersonalDbCommittedHead, PersonalDbSnapshotsHead,
        prepare_and_stage_personaldb_committed_head, prepare_and_stage_personaldb_group_manifest,
        read_personaldb_group_manifest, read_personaldb_group_manifest_in_transaction,
    },
    personaldb_projection::{
        ProjectionDefinition, WriteBackPolicy, list_projection_definitions_for_database,
        list_projection_definitions_for_source, prepare_and_stage_projection_definition,
        read_projection_definition, read_projection_definition_in_transaction,
    },
    personaldb_projection_builder::{
        ProjectionAuthorizationCheck, ProjectionAuthorizationDecisions, ProjectionBuildInput,
        build_projection_changeset_with_authorization, collect_projection_authorization_checks,
    },
    personaldb_projection_snapshot::{
        MAX_SNAPSHOT_PAGE_BYTES, prepare_projection_snapshot, read_projection_snapshot_range,
    },
    personaldb_projection_writeback::{
        ProjectionWriteBackInput, build_projection_writeback_changeset,
    },
    personaldb_proposal_admission::{
        read_personaldb_committed_head_at_snapshot, read_personaldb_committed_head_mvcc,
        stage_personaldb_committed_head_mvcc, stage_personaldb_committed_head_seed,
    },
    personaldb_row_index::{PersonalDbRowIndexWrite, prepare_and_stage_personaldb_row_index},
    personaldb_schema::{
        prepare_and_stage_personaldb_schema_sql, read_personaldb_schema_sql,
        validate_changeset_tables_registered, validate_schema_sql,
    },
    personaldb_segment::{
        PersonalDbLogSegmentWrite, prepare_and_stage_personaldb_log_segment,
        read_personaldb_log_segment,
    },
    personaldb_snapshot_builder::{
        PersonalDbSnapshotBuildRequest, PersonalDbSnapshotPolicy, maybe_build_personaldb_snapshot,
    },
    personaldb_submit::{
        SubmitPersonalDbChangeset as CoreSubmitChangeset, default_max_changeset_size,
        validate_submit_personaldb_changeset,
    },
    personaldb_watch::{
        PersonalDbGroupWatchEvent, PersonalDbGroupWatchPayload, PersonalDbProjectionWatchEvent,
        PersonalDbProjectionWatchPayload, append_personaldb_projection_watch_record,
        list_personaldb_group_watch_event_page, list_personaldb_projection_watch_event_page,
        stage_personaldb_group_watch_record,
    },
    services::watch_envelope::{self, WatchEnvelopeParts},
};
use prost::Message;
use sha2::{Digest as _, Sha256};
use std::sync::LazyLock;
use tokio::sync::OwnedMutexGuard;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const PERSONALDB_PROJECTION_WRITEBACK_RESULT_NAMESPACE: &str =
    "personaldb.projection-writeback-response.v1";
const PERSONALDB_SNAPSHOT_STREAM_LIMIT: usize = 16;
const PERSONALDB_SNAPSHOT_DESCRIPTOR_COMPONENT_MAX_BYTES: usize = 1024 * 1024;
const PERSONALDB_SNAPSHOT_STREAM_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);
static PERSONALDB_SNAPSHOT_STREAMS: LazyLock<std::sync::Arc<Semaphore>> =
    LazyLock::new(|| std::sync::Arc::new(Semaphore::new(PERSONALDB_SNAPSHOT_STREAM_LIMIT)));

fn projection_writeback_result_key(request: &CoreSubmitChangeset) -> String {
    format!("{}:{}", request.database_id, request.idempotency_key)
}

#[derive(Debug, Clone)]
struct PersonalDbCommitActor {
    tenant_id: i64,
    principal: String,
    bearer_token: Option<String>,
    require_public_commit_authorization: bool,
}

impl PersonalDbCommitActor {
    fn public(tenant_id: i64, principal: String, bearer_token: String) -> Self {
        Self {
            tenant_id,
            principal,
            bearer_token: Some(bearer_token),
            require_public_commit_authorization: true,
        }
    }
}

#[derive(Debug, Clone)]
struct CommittedPersonalDbChangeset {
    log_index: u64,
    log_hash: String,
    changeset_payload_hash: String,
    verified_envelope_hash: String,
    certificate_hash: String,
    certificate: PersonalDbCommitCertificate,
    committed_head: PersonalDbCommittedHead,
    watch_cursor: u128,
    authz_revision: u64,
}

mod postcommit;
mod service;

fn request_claims<T>(request: &Request<T>) -> Result<&auth::Claims, Status> {
    request
        .extensions()
        .get::<auth::Claims>()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))
}

fn request_bearer_token<T>(request: &Request<T>) -> Result<&str, Status> {
    request
        .extensions()
        .get::<auth::AuthenticatedBearerToken>()
        .map(|token| token.0.as_str())
        .ok_or_else(|| Status::unauthenticated("Missing authenticated session token"))
}

fn bind_personaldb_submit_session(
    request: &CoreSubmitChangeset,
    actor: &PersonalDbCommitActor,
    bearer_token: &str,
) -> Result<(), Status> {
    if request.session_token != bearer_token {
        return Err(Status::unauthenticated(
            "PersonalDB session token does not match authenticated bearer",
        ));
    }
    if request.principal != actor.principal {
        return Err(Status::permission_denied(
            "PersonalDB principal does not match authenticated session",
        ));
    }
    Ok(())
}

async fn authorize_personaldb_row_effects(
    storage: &crate::storage::Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    envelope: &VerifiedMutationEnvelope,
    actor: &PersonalDbCommitActor,
) -> Result<(), Status> {
    if !actor.require_public_commit_authorization {
        return Ok(());
    }

    for effect in &envelope.table_effects {
        let binding = &effect.source_resource_binding;
        let resource = personaldb_row_resource_id(actor.tenant_id, &envelope.database_id, binding);
        for permission in &effect.required_permissions {
            let revision = i64::try_from(envelope.authz_revision)
                .map_err(|_| Status::internal("Invalid PersonalDB authz revision"))?;
            let allowed = authz_journal::resolve_permission_at_revision(
                storage,
                mvcc,
                actor.tenant_id,
                &encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, "personaldb_row"),
                &resource,
                permission,
                access_control::APP_SUBJECT_KIND,
                &actor.principal,
                "",
                revision,
            )
            .await
            .map_err(internal_status)?;
            if allowed || insert_effect_creates_owned_row(effect, actor) {
                continue;
            }
            return Err(Status::permission_denied(
                "PersonalDB row/resource mutation is not authorized",
            ));
        }
    }
    Ok(())
}

async fn stage_personaldb_row_owner_grants(
    persistence: &crate::persistence::Persistence,
    envelope: &VerifiedMutationEnvelope,
    actor: &PersonalDbCommitActor,
    transaction_id: &str,
    transaction_principal: &str,
) -> anyhow::Result<()> {
    let mut mutations = Vec::new();
    for row in &envelope.row_metadata_delta.upserts {
        if row.owner_principal.as_deref() != Some(actor.principal.as_str()) {
            continue;
        }
        let resource = format!(
            "tenant-{}/{}/{}/{}",
            actor.tenant_id, envelope.database_id, row.resource_type, row.resource_id
        );
        for relation in [
            "personaldb:insert",
            "personaldb:update",
            "personaldb:delete",
        ] {
            mutations.push(crate::persistence::AuthzTupleBatchMutation {
                namespace: encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, "personaldb_row"),
                object_id: resource.clone(),
                relation: relation.to_string(),
                subject_kind: access_control::APP_SUBJECT_KIND.to_string(),
                subject_id: actor.principal.clone(),
                caveat_hash: String::new(),
                operation: "add".to_string(),
                reason: "PersonalDB row owner grant".to_string(),
            });
        }
    }
    if mutations.is_empty() {
        return Ok(());
    }
    persistence
        .stage_authz_tuple_batch(
            actor.tenant_id,
            mutations,
            &actor.principal,
            transaction_id,
            transaction_principal,
            None,
        )
        .await?;
    Ok(())
}

fn insert_effect_creates_owned_row(
    effect: &crate::personaldb_envelope::TableEffect,
    actor: &PersonalDbCommitActor,
) -> bool {
    effect.operation == TableOperation::Insert
        && effect.source_resource_binding.owner_principal.as_deref()
            == Some(actor.principal.as_str())
}

fn personaldb_row_resource_id(
    tenant_id: i64,
    database_id: &str,
    binding: &crate::personaldb_envelope::ResourceBinding,
) -> String {
    format!(
        "tenant-{}/{}/{}/{}",
        tenant_id, database_id, binding.resource_type, binding.resource_id
    )
}

mod helpers;
use helpers::*;

#[cfg(test)]
mod tests;
