//! Signed, generation-bound public index pagination.

use keldra_authz::ObjectRef;
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::authentication::{Caller, JwtManager};

use super::boundary::{IndexPageCursor, IndexPageTokenBinding, IndexPageTokenCodec};

pub(crate) const INDEX_PAGE_TOKEN_AUDIENCE: &str = "keldra-index-page";
pub(crate) const INDEX_PAGE_TOKEN_PURPOSE: &str = "index-page";
const INDEX_PAGE_TOKEN_FORMAT: u8 = 4;

/// Strongly typed private JWT claims. There is deliberately no expiry: the
/// referenced immutable generation, definition version, and exact Zanzibar
/// revision define useful validity, while normal generation retention bounds
/// how long the continuation can actually be served.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexPageTokenClaims {
    pub(crate) format: u8,
    pub(crate) aud: String,
    pub(crate) purpose: String,
    pub(crate) storage_tenant: String,
    pub(crate) subject: ObjectRef,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) index_id: u64,
    pub(crate) definition_version: u64,
    pub(crate) generation: u64,
    pub(crate) query_hash: [u8; 32],
    pub(crate) authorization_revision: u64,
    pub(crate) last_position: Vec<u8>,
}

impl IndexPageTokenClaims {
    fn new(caller: &Caller, binding: IndexPageTokenBinding, cursor: &IndexPageCursor) -> Self {
        Self {
            format: INDEX_PAGE_TOKEN_FORMAT,
            aud: INDEX_PAGE_TOKEN_AUDIENCE.into(),
            purpose: INDEX_PAGE_TOKEN_PURPOSE.into(),
            storage_tenant: caller.storage_tenant().as_str().to_owned(),
            subject: caller.subject().clone(),
            tenant_id: binding.tenant_id,
            bucket_id: binding.bucket_id,
            index_id: binding.index_id,
            definition_version: binding.definition_version,
            generation: cursor.generation,
            query_hash: binding.query_hash,
            authorization_revision: cursor.authorization_revision,
            last_position: cursor.last_position.clone(),
        }
    }

    pub(crate) fn has_valid_envelope(&self) -> bool {
        self.format == INDEX_PAGE_TOKEN_FORMAT
            && self.aud == INDEX_PAGE_TOKEN_AUDIENCE
            && self.purpose == INDEX_PAGE_TOKEN_PURPOSE
            && !self.storage_tenant.is_empty()
            && self.tenant_id != 0
            && self.bucket_id != 0
            && self.index_id != 0
            && self.definition_version != 0
            && self.generation != 0
            && self.authorization_revision != 0
            && !self.last_position.is_empty()
    }

    fn belongs_to(&self, caller: &Caller) -> bool {
        self.storage_tenant == caller.storage_tenant().as_str() && &self.subject == caller.subject()
    }

    fn matches(&self, expected: IndexPageTokenBinding) -> bool {
        self.tenant_id == expected.tenant_id
            && self.bucket_id == expected.bucket_id
            && self.index_id == expected.index_id
            && self.definition_version == expected.definition_version
            && self.query_hash == expected.query_hash
    }

    fn cursor(self) -> IndexPageCursor {
        IndexPageCursor {
            generation: self.generation,
            last_position: self.last_position,
            authorization_revision: self.authorization_revision,
        }
    }
}

impl IndexPageTokenCodec for JwtManager {
    fn decode(
        &self,
        caller: &Caller,
        token: &[u8],
        expected: IndexPageTokenBinding,
    ) -> Result<IndexPageCursor, Status> {
        require_binding(expected)?;
        let claims = self
            .open_index_page_token(token)
            .map_err(|_| invalid_token())?;
        if !claims.has_valid_envelope() || !claims.belongs_to(caller) || !claims.matches(expected) {
            return Err(invalid_token());
        }
        Ok(claims.cursor())
    }

    fn encode(
        &self,
        caller: &Caller,
        binding: IndexPageTokenBinding,
        cursor: &IndexPageCursor,
    ) -> Result<Vec<u8>, Status> {
        require_binding(binding)?;
        require_cursor(cursor)?;
        self.seal_index_page_token(&IndexPageTokenClaims::new(caller, binding, cursor))
            .map_err(|_| Status::internal("could not issue index page token"))
    }
}

fn require_binding(binding: IndexPageTokenBinding) -> Result<(), Status> {
    if binding.tenant_id == 0
        || binding.bucket_id == 0
        || binding.index_id == 0
        || binding.definition_version == 0
    {
        Err(Status::internal("index page binding is invalid"))
    } else {
        Ok(())
    }
}

fn require_cursor(cursor: &IndexPageCursor) -> Result<(), Status> {
    if cursor.generation == 0
        || cursor.authorization_revision == 0
        || cursor.last_position.is_empty()
    {
        Err(Status::internal("index page cursor is invalid"))
    } else {
        Ok(())
    }
}

fn invalid_token() -> Status {
    Status::invalid_argument("index page token is invalid for this query")
}

#[cfg(test)]
mod tests {
    use keldra_store::StorageTenantId;

    use super::*;
    use crate::authentication::PUT_TOKEN_LIFETIME;

    const KEY: &[u8; 32] = b"0123456789abcdef0123456789abcdef";

    fn caller(tenant: &str, app: &str) -> Caller {
        Caller::from_authenticated_application(StorageTenantId::parse(tenant).unwrap(), app)
            .unwrap()
    }

    fn binding() -> IndexPageTokenBinding {
        IndexPageTokenBinding {
            tenant_id: 11,
            bucket_id: 13,
            index_id: 17,
            definition_version: 23,
            query_hash: [5; 32],
        }
    }

    fn cursor() -> IndexPageCursor {
        IndexPageCursor {
            generation: 31,
            last_position: b"engine-position".to_vec(),
            authorization_revision: 41,
        }
    }

    #[test]
    fn page_token_round_trip_preserves_complete_cursor() {
        let manager = JwtManager::new(KEY).unwrap();
        let caller = caller("tenant-a", "app-a");
        let token = manager.encode(&caller, binding(), &cursor()).unwrap();

        assert_eq!(
            manager.decode(&caller, &token, binding()).unwrap(),
            cursor()
        );
    }

    #[test]
    fn page_token_is_bound_to_caller_scope_definition_and_query() {
        let manager = JwtManager::new(KEY).unwrap();
        let principal = caller("tenant-a", "app-a");
        let token = manager.encode(&principal, binding(), &cursor()).unwrap();

        assert!(
            manager
                .decode(&caller("tenant-a", "app-b"), &token, binding())
                .is_err()
        );
        assert!(
            manager
                .decode(&caller("tenant-b", "app-a"), &token, binding())
                .is_err()
        );
        let mut changed = binding();
        changed.tenant_id += 1;
        assert!(manager.decode(&principal, &token, changed).is_err());
        changed = binding();
        changed.bucket_id += 1;
        assert!(manager.decode(&principal, &token, changed).is_err());
        changed = binding();
        changed.definition_version += 1;
        assert!(manager.decode(&principal, &token, changed).is_err());
        changed = binding();
        changed.query_hash[0] ^= 1;
        assert!(manager.decode(&principal, &token, changed).is_err());
    }

    #[test]
    fn page_token_rejects_tampering_and_another_signing_key() {
        let manager = JwtManager::new(KEY).unwrap();
        let other = JwtManager::new(b"fedcba9876543210fedcba9876543210").unwrap();
        let caller = caller("tenant-a", "app-a");
        let mut token = manager.encode(&caller, binding(), &cursor()).unwrap();

        assert!(other.decode(&caller, &token, binding()).is_err());
        let signature = token.iter().rposition(|byte| *byte == b'.').unwrap() + 1;
        token[signature] = if token[signature] == b'a' { b'b' } else { b'a' };
        assert!(manager.decode(&caller, &token, binding()).is_err());
    }

    #[test]
    fn put_capabilities_are_not_index_page_tokens() {
        let manager = JwtManager::new(KEY).unwrap();
        let caller = caller("tenant-a", "app-a");
        let (put_token, _) = manager
            .mint_put_token(&caller, b"{}", PUT_TOKEN_LIFETIME)
            .unwrap();

        assert!(
            manager
                .decode(&caller, put_token.as_bytes(), binding())
                .is_err()
        );
    }
}
