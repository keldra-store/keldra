//! Caller-bound tokens for selective real-time index visibility.

use keldra_authz::ObjectRef;
use keldra_store::{ProgramJournalVisibility, RealtimeVisibilityEvidence};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::authentication::{Caller, JwtManager};

use super::boundary::{IndexVisibilityRequirement, IndexVisibilityTokenCodec};

pub(crate) const INDEX_VISIBILITY_TOKEN_AUDIENCE: &str = "keldra-index-visibility";
pub(crate) const INDEX_VISIBILITY_TOKEN_PURPOSE: &str = "index-visibility";
const INDEX_VISIBILITY_TOKEN_FORMAT: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexVisibilityTokenClaims {
    format: u8,
    pub(crate) aud: String,
    pub(crate) purpose: String,
    sub: String,
    storage_tenant: String,
    subject: ObjectRef,
    source_node_id: u16,
    source_epoch: [u8; 32],
    source_journal_position: u64,
    source_journal_through_position: u64,
    routes: Vec<VisibilityRouteClaims>,
    exact_path: Option<String>,
    version: Option<u64>,
    program_commit_cursor: Option<u64>,
    atomic_unit_hash: Option<[u8; 32]>,
    active_placement_term: u64,
    active_placement_index: u64,
    expires_at_unix_millis: u64,
    exp: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VisibilityRouteClaims {
    tenant_id: u64,
    bucket_id: u64,
}

impl IndexVisibilityTokenClaims {
    fn new(caller: &Caller, evidence: &RealtimeVisibilityEvidence) -> Result<Self, Status> {
        let sub = caller
            .authenticated_app_id()
            .map_err(|_| {
                Status::unauthenticated(
                    "real-time visibility requires an authenticated application",
                )
            })?
            .to_owned();
        Ok(Self {
            format: INDEX_VISIBILITY_TOKEN_FORMAT,
            aud: INDEX_VISIBILITY_TOKEN_AUDIENCE.into(),
            purpose: INDEX_VISIBILITY_TOKEN_PURPOSE.into(),
            sub,
            storage_tenant: caller.storage_tenant().as_str().to_owned(),
            subject: caller.subject().clone(),
            source_node_id: evidence.source_id.node_id,
            source_epoch: evidence.source_id.source_epoch,
            source_journal_position: evidence.source_journal_position,
            source_journal_through_position: evidence.source_journal_through_position,
            routes: vec![VisibilityRouteClaims {
                tenant_id: evidence.tenant_id,
                bucket_id: evidence.bucket_id,
            }],
            exact_path: Some(evidence.exact_path.clone()),
            version: Some(evidence.version.0),
            program_commit_cursor: evidence.program_commit_cursor,
            atomic_unit_hash: None,
            active_placement_term: evidence.active_placement_log_id.term,
            active_placement_index: evidence.active_placement_log_id.index,
            expires_at_unix_millis: evidence.expires_at_unix_millis,
            // Never extend the store's evidence lifetime when converting its
            // millisecond deadline to JWT's registered second granularity.
            exp: evidence.expires_at_unix_millis / 1_000,
        })
    }

    fn new_program(caller: &Caller, evidence: &ProgramJournalVisibility) -> Result<Self, Status> {
        let sub = caller
            .authenticated_app_id()
            .map_err(|_| {
                Status::unauthenticated(
                    "real-time visibility requires an authenticated application",
                )
            })?
            .to_owned();
        Ok(Self {
            format: INDEX_VISIBILITY_TOKEN_FORMAT,
            aud: INDEX_VISIBILITY_TOKEN_AUDIENCE.into(),
            purpose: INDEX_VISIBILITY_TOKEN_PURPOSE.into(),
            sub,
            storage_tenant: caller.storage_tenant().as_str().to_owned(),
            subject: caller.subject().clone(),
            source_node_id: evidence.source_id.node_id,
            source_epoch: evidence.source_id.source_epoch,
            source_journal_position: evidence.atomic_source_journal_position,
            source_journal_through_position: evidence.atomic_source_journal_position,
            routes: evidence
                .routes
                .iter()
                .map(|route| VisibilityRouteClaims {
                    tenant_id: route.tenant_id,
                    bucket_id: route.bucket_id,
                })
                .collect(),
            exact_path: None,
            version: None,
            program_commit_cursor: Some(evidence.program_commit_cursor),
            atomic_unit_hash: Some(evidence.atomic_unit_hash),
            active_placement_term: evidence.active_placement_log_id.term,
            active_placement_index: evidence.active_placement_log_id.index,
            expires_at_unix_millis: evidence.expires_at_unix_millis,
            exp: evidence.expires_at_unix_millis / 1_000,
        })
    }

    pub(crate) fn has_valid_envelope(&self) -> bool {
        self.format == INDEX_VISIBILITY_TOKEN_FORMAT
            && self.aud == INDEX_VISIBILITY_TOKEN_AUDIENCE
            && self.purpose == INDEX_VISIBILITY_TOKEN_PURPOSE
            && !self.sub.is_empty()
            && !self.storage_tenant.is_empty()
            && self.source_node_id != 0
            && self.source_epoch != [0; 32]
            && self.source_journal_position != 0
            && self.source_journal_through_position >= self.source_journal_position
            && !self.routes.is_empty()
            && self.routes.len() <= 1_024
            && self
                .routes
                .iter()
                .all(|route| route.tenant_id != 0 && route.bucket_id != 0)
            && self.routes.windows(2).all(|pair| {
                (pair[0].tenant_id, pair[0].bucket_id) < (pair[1].tenant_id, pair[1].bucket_id)
            })
            && match (
                self.exact_path.as_deref(),
                self.version,
                self.program_commit_cursor,
                self.atomic_unit_hash,
            ) {
                (Some(path), Some(version), None, None) => !path.is_empty() && version != 0,
                (None, None, Some(cursor), Some(hash)) => cursor != 0 && hash != [0; 32],
                _ => false,
            }
            && self.active_placement_term != 0
            && self.active_placement_index != 0
            && self.expires_at_unix_millis != 0
            && self.exp == self.expires_at_unix_millis / 1_000
    }

    fn belongs_to(&self, caller: &Caller) -> bool {
        self.storage_tenant == caller.storage_tenant().as_str()
            && &self.subject == caller.subject()
            && caller
                .authenticated_app_id()
                .is_ok_and(|app_id| app_id == self.sub)
    }

    fn into_requirement(self, tenant_id: u64, bucket_id: u64) -> IndexVisibilityRequirement {
        IndexVisibilityRequirement {
            source_node_id: self.source_node_id,
            source_epoch: self.source_epoch,
            source_journal_position: self.source_journal_position,
            source_journal_through_position: self.source_journal_through_position,
            tenant_id,
            bucket_id,
            exact_path: self.exact_path,
            version: self.version,
            program_commit_cursor: self.program_commit_cursor,
            atomic_unit_hash: self.atomic_unit_hash,
            active_placement_term: self.active_placement_term,
            active_placement_index: self.active_placement_index,
            expires_at_unix_millis: self.expires_at_unix_millis,
        }
    }
}

pub(crate) fn issue_index_visibility_token(
    tokens: &JwtManager,
    caller: &Caller,
    evidence: &RealtimeVisibilityEvidence,
) -> Result<Vec<u8>, Status> {
    let claims = IndexVisibilityTokenClaims::new(caller, evidence)?;
    tokens
        .seal_index_visibility_token(&claims)
        .map_err(|_| Status::internal("could not issue index visibility token"))
}

pub(crate) fn issue_program_index_visibility_token(
    tokens: &JwtManager,
    caller: &Caller,
    evidence: &ProgramJournalVisibility,
) -> Result<Vec<u8>, Status> {
    let claims = IndexVisibilityTokenClaims::new_program(caller, evidence)?;
    tokens
        .seal_index_visibility_token(&claims)
        .map_err(|_| Status::internal("could not issue index visibility token"))
}

impl IndexVisibilityTokenCodec for JwtManager {
    fn decode(
        &self,
        caller: &Caller,
        token: &[u8],
        expected_tenant_id: u64,
        expected_bucket_id: u64,
    ) -> Result<IndexVisibilityRequirement, Status> {
        if expected_tenant_id == 0 || expected_bucket_id == 0 {
            return Err(Status::internal("index visibility binding is invalid"));
        }
        let claims = self
            .open_index_visibility_token(token)
            .map_err(|_| invalid_token())?;
        if !claims.has_valid_envelope()
            || !claims.belongs_to(caller)
            || claims
                .routes
                .binary_search_by_key(&(expected_tenant_id, expected_bucket_id), |route| {
                    (route.tenant_id, route.bucket_id)
                })
                .is_err()
        {
            return Err(invalid_token());
        }
        Ok(claims.into_requirement(expected_tenant_id, expected_bucket_id))
    }
}

fn invalid_token() -> Status {
    Status::invalid_argument("index visibility token is invalid for this query")
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use keldra_store::{
        PlacementLogId, ProgramJournalVisibility, RealtimeVisibilityRoute, SourceId,
        StorageTenantId, VersionId,
    };

    use super::*;

    const KEY: &[u8; 32] = b"0123456789abcdef0123456789abcdef";

    fn caller(tenant: &str, app: &str) -> Caller {
        Caller::from_authenticated_application(StorageTenantId::parse(tenant).unwrap(), app)
            .unwrap()
    }

    fn evidence(expires_at_unix_millis: u64) -> RealtimeVisibilityEvidence {
        RealtimeVisibilityEvidence {
            source_id: SourceId {
                node_id: 3,
                source_epoch: [7; 32],
            },
            source_journal_position: 11,
            source_journal_through_position: 12,
            tenant_id: 13,
            bucket_id: 17,
            exact_path: "objects/one.json".into(),
            version: VersionId(19),
            program_commit_cursor: None,
            active_placement_log_id: PlacementLogId { term: 5, index: 29 },
            expires_at_unix_millis,
        }
    }

    #[test]
    fn token_round_trip_preserves_exact_sparse_evidence() {
        let manager = JwtManager::new(KEY).unwrap();
        let caller = caller("tenant", "app-1");
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let expected = evidence(expiry);
        let token = issue_index_visibility_token(&manager, &caller, &expected).unwrap();
        let decoded = manager.decode(&caller, &token, 13, 17).unwrap();
        assert_eq!(decoded.source_node_id, expected.source_id.node_id);
        assert_eq!(decoded.source_epoch, expected.source_id.source_epoch);
        assert_eq!(
            decoded.source_journal_position,
            expected.source_journal_position
        );
        assert_eq!(
            decoded.source_journal_through_position,
            expected.source_journal_through_position
        );
        assert_eq!(
            decoded.exact_path.as_deref(),
            Some(expected.exact_path.as_str())
        );
        assert_eq!(decoded.version, Some(expected.version.0));
        assert_eq!(decoded.program_commit_cursor, None);
        assert_eq!(decoded.atomic_unit_hash, None);
        assert_eq!(decoded.active_placement_term, 5);
        assert_eq!(decoded.active_placement_index, 29);
        assert_eq!(decoded.expires_at_unix_millis, expiry);
    }

    #[test]
    fn token_rejects_another_caller_or_bucket() {
        let manager = JwtManager::new(KEY).unwrap();
        let owner = caller("tenant", "app-1");
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let token = issue_index_visibility_token(&manager, &owner, &evidence(expiry)).unwrap();
        assert!(
            manager
                .decode(&caller("tenant", "app-2"), &token, 13, 17)
                .is_err()
        );
        assert!(manager.decode(&owner, &token, 13, 18).is_err());
    }

    #[test]
    fn expired_token_is_rejected() {
        let manager = JwtManager::new(KEY).unwrap();
        let caller = caller("tenant", "app-1");
        let expiry = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64)
            .saturating_sub(2_000);
        let token = issue_index_visibility_token(&manager, &caller, &evidence(expiry)).unwrap();
        assert!(manager.decode(&caller, &token, 13, 17).is_err());
    }

    #[test]
    fn forged_token_bytes_are_rejected() {
        let manager = JwtManager::new(KEY).unwrap();
        let caller = caller("tenant", "app-1");
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let mut token = issue_index_visibility_token(&manager, &caller, &evidence(expiry)).unwrap();
        let last = token.last_mut().expect("sealed token is non-empty");
        *last ^= 1;
        assert!(manager.decode(&caller, &token, 13, 17).is_err());
    }

    #[test]
    fn atomic_token_selects_each_bound_bucket_without_losing_unit_identity() {
        let manager = JwtManager::new(KEY).unwrap();
        let caller = caller("tenant", "app-1");
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let evidence = ProgramJournalVisibility {
            source_id: SourceId {
                node_id: 3,
                source_epoch: [7; 32],
            },
            atomic_source_journal_position: 31,
            program_commit_cursor: 37,
            atomic_unit_hash: [41; 32],
            routes: vec![
                RealtimeVisibilityRoute {
                    tenant_id: 13,
                    bucket_id: 17,
                },
                RealtimeVisibilityRoute {
                    tenant_id: 13,
                    bucket_id: 19,
                },
            ],
            active_placement_log_id: PlacementLogId { term: 5, index: 29 },
            expires_at_unix_millis: expiry,
        };
        let token = issue_program_index_visibility_token(&manager, &caller, &evidence).unwrap();
        let first = manager.decode(&caller, &token, 13, 17).unwrap();
        let second = manager.decode(&caller, &token, 13, 19).unwrap();
        assert_eq!(first.source_journal_position, 31);
        assert_eq!(first.source_journal_through_position, 31);
        assert_eq!(first.program_commit_cursor, Some(37));
        assert_eq!(first.atomic_unit_hash, Some([41; 32]));
        assert_eq!(second.atomic_unit_hash, Some([41; 32]));
        assert!(manager.decode(&caller, &token, 13, 23).is_err());
    }
}
