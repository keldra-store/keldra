//! Authentication establishes caller identity; Zanzibar decides authority.
//!
//! Long-lived application credentials are deliberately outside this module.
//! They must be resolved from durable Anvil state before [`JwtManager::mint`]
//! is called. Protected services use [`JwtManager::authenticate`] as a tonic
//! interceptor and consume the resulting [`Caller`] request extension.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anvil_authz::ObjectRef;
use anvil_store::StorageTenantId;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use tonic::{Request, Status};

const ACCESS_TOKEN_LIFETIME: Duration = Duration::from_secs(60 * 60);

/// Identity established from trusted credentials for one request.
///
/// Fields are private so request payloads cannot modify an authenticated
/// identity after the interceptor has constructed it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Caller {
    storage_tenant: StorageTenantId,
    subject: ObjectRef,
}

impl Caller {
    pub fn storage_tenant(&self) -> &StorageTenantId {
        &self.storage_tenant
    }

    pub fn subject(&self) -> &ObjectRef {
        &self.subject
    }

    pub(crate) fn from_authenticated_application(
        storage_tenant: StorageTenantId,
        client_id: impl Into<String>,
    ) -> Result<Self, AuthenticationError> {
        let subject = ObjectRef::opaque("app", client_id.into())
            .map_err(|error| AuthenticationError::InvalidIdentity(error.to_string()))?;
        if subject.is_public() {
            return Err(AuthenticationError::InvalidIdentity(
                "the reserved public subject cannot authenticate".into(),
            ));
        }
        Ok(Self {
            storage_tenant,
            subject,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthenticationError {
    #[error("the token signing secret must not be empty")]
    EmptySigningSecret,
    #[error("invalid authenticated identity: {0}")]
    InvalidIdentity(String),
    #[error("the system clock predates the Unix epoch")]
    InvalidSystemClock,
    #[error("access token timestamp overflow")]
    TimestampOverflow,
    #[error("access token could not be encoded: {0}")]
    Encode(#[source] jsonwebtoken::errors::Error),
    #[error("access token could not be verified: {0}")]
    Verify(#[source] jsonwebtoken::errors::Error),
}

#[derive(Clone)]
pub struct JwtManager {
    encoding_key: Arc<EncodingKey>,
    decoding_key: Arc<DecodingKey>,
    validation: Arc<Validation>,
}

impl std::fmt::Debug for JwtManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("JwtManager").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    storage_tenant: String,
    exp: u64,
    jti: String,
}

impl JwtManager {
    pub fn new(signing_secret: impl AsRef<[u8]>) -> Result<Self, AuthenticationError> {
        let signing_secret = signing_secret.as_ref();
        if signing_secret.is_empty() {
            return Err(AuthenticationError::EmptySigningSecret);
        }
        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 0;
        validation.set_required_spec_claims(&["exp", "sub"]);
        Ok(Self {
            encoding_key: Arc::new(EncodingKey::from_secret(signing_secret)),
            decoding_key: Arc::new(DecodingKey::from_secret(signing_secret)),
            validation: Arc::new(validation),
        })
    }

    /// Mints a one-hour access token after durable credentials have already
    /// established the application identity.
    pub fn mint(
        &self,
        storage_tenant: StorageTenantId,
        client_id: impl Into<String>,
    ) -> Result<String, AuthenticationError> {
        let client_id = client_id.into();
        let caller = Caller::from_authenticated_application(storage_tenant, client_id.clone())?;
        let now = unix_seconds()?;
        let exp = now
            .checked_add(ACCESS_TOKEN_LIFETIME.as_secs())
            .ok_or(AuthenticationError::TimestampOverflow)?;
        let claims = Claims {
            sub: client_id,
            storage_tenant: caller.storage_tenant.as_str().to_owned(),
            exp,
            jti: uuid::Uuid::new_v4().to_string(),
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            self.encoding_key.as_ref(),
        )
        .map_err(AuthenticationError::Encode)
    }

    pub fn verify(&self, token: &str) -> Result<Caller, AuthenticationError> {
        let token = decode::<Claims>(token, self.decoding_key.as_ref(), self.validation.as_ref())
            .map_err(AuthenticationError::Verify)?;
        let tenant = StorageTenantId::parse(token.claims.storage_tenant)
            .map_err(|error| AuthenticationError::InvalidIdentity(error.to_string()))?;
        Caller::from_authenticated_application(tenant, token.claims.sub)
    }

    /// Verifies exactly one bearer token and installs its immutable caller on
    /// the tonic request. Apply this only to protected services; token exchange
    /// remains a separate unauthenticated service boundary.
    pub fn authenticate<T>(&self, mut request: Request<T>) -> Result<Request<T>, Status> {
        let mut authorization = request.metadata().get_all("authorization").iter();
        let value = authorization
            .next()
            .ok_or_else(|| Status::unauthenticated("a bearer token is required"))?;
        if authorization.next().is_some() {
            return Err(Status::unauthenticated(
                "exactly one bearer token is required",
            ));
        }
        let value = value
            .to_str()
            .map_err(|_| Status::unauthenticated("the bearer token is malformed"))?;
        let token = value
            .strip_prefix("Bearer ")
            .filter(|token| !token.is_empty())
            .ok_or_else(|| Status::unauthenticated("the bearer token is malformed"))?;
        let caller = self
            .verify(token)
            .map_err(|_| Status::unauthenticated("the bearer token is invalid or expired"))?;
        request.extensions_mut().insert(caller);
        Ok(request)
    }
}

fn unix_seconds() -> Result<u64, AuthenticationError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| AuthenticationError::InvalidSystemClock)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anvil_authz::ObjectId;
    use tonic::metadata::MetadataValue;

    fn tenant(value: &str) -> StorageTenantId {
        StorageTenantId::parse(value).unwrap()
    }

    #[test]
    fn verified_token_establishes_tenant_and_canonical_app_subject() {
        let manager = JwtManager::new("correct horse battery staple").unwrap();
        let token = manager.mint(tenant("worka"), "client-7").unwrap();

        let caller = manager.verify(&token).unwrap();

        assert_eq!(caller.storage_tenant().as_str(), "worka");
        assert_eq!(caller.subject().namespace, "app");
        assert_eq!(caller.subject().id, ObjectId::Opaque("client-7".to_owned()));
    }

    #[test]
    fn interceptor_inserts_only_verified_identity() {
        let manager = JwtManager::new("correct horse battery staple").unwrap();
        let token = manager.mint(tenant("worka"), "client-7").unwrap();
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );

        let request = manager.authenticate(request).unwrap();
        let caller = request.extensions().get::<Caller>().unwrap();

        assert_eq!(caller.storage_tenant().as_str(), "worka");
        assert_eq!(caller.subject().id, ObjectId::Opaque("client-7".to_owned()));
    }

    #[test]
    fn wrong_key_missing_token_and_duplicate_headers_fail_closed() {
        let issuer = JwtManager::new("issuer signing secret").unwrap();
        let verifier = JwtManager::new("different verifier secret").unwrap();
        let token = issuer.mint(tenant("worka"), "client-7").unwrap();
        assert!(verifier.verify(&token).is_err());

        let missing = verifier.authenticate(Request::new(())).unwrap_err();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);

        let mut duplicate = Request::new(());
        let value = MetadataValue::try_from(format!("Bearer {token}")).unwrap();
        duplicate
            .metadata_mut()
            .append("authorization", value.clone());
        duplicate.metadata_mut().append("authorization", value);
        let duplicate = issuer.authenticate(duplicate).unwrap_err();
        assert_eq!(duplicate.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn expired_token_is_rejected() {
        let manager = JwtManager::new("correct horse battery staple").unwrap();
        let claims = Claims {
            sub: "client-7".to_owned(),
            storage_tenant: "worka".to_owned(),
            exp: 1,
            jti: uuid::Uuid::new_v4().to_string(),
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            manager.encoding_key.as_ref(),
        )
        .unwrap();

        assert!(manager.verify(&token).is_err());
    }

    #[test]
    fn token_fields_cannot_encode_an_invalid_tenant_or_subject() {
        let manager = JwtManager::new("correct horse battery staple").unwrap();
        assert!(manager.mint(tenant("worka"), "subject\nsmuggling").is_err());
        assert!(
            manager
                .mint(tenant("worka"), anvil_authz::PUBLIC_SUBJECT_ID)
                .is_err()
        );
    }

    #[test]
    fn signing_secret_is_required() {
        assert!(matches!(
            JwtManager::new([]),
            Err(AuthenticationError::EmptySigningSecret)
        ));
    }
}
