//! Authentication establishes caller identity; Zanzibar decides authority.
//!
//! Long-lived application credentials are deliberately outside this module.
//! They must be resolved from durable Anvil state before [`JwtManager::mint`]
//! is called. Protected services use [`JwtManager::authenticate`] as a tonic
//! interceptor and consume the resulting [`Caller`] request extension.

use std::fs::File;
use std::io::Read;
use std::num::{NonZeroU32, NonZeroU64};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anvil_authz::ObjectRef;
use anvil_consensus::JwtSigningKeyFingerprint;
use anvil_store::{CredentialSecretEnvelope, StorageTenantId};
use governor::clock::Clock;
use governor::{DefaultDirectRateLimiter, DefaultKeyedRateLimiter, Quota, RateLimiter};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use tonic::{Request, Status};

use crate::distributed_watch::{
    CHECKPOINT_AUDIENCE, CHECKPOINT_PURPOSE, WatchCheckpointClaims, WatchCheckpointCodec,
};
use crate::index_service::{
    INDEX_PAGE_TOKEN_AUDIENCE, INDEX_PAGE_TOKEN_PURPOSE, IndexPageTokenClaims,
};

pub(crate) const ACCESS_TOKEN_LIFETIME: Duration = Duration::from_secs(60 * 60);
pub const PUT_TOKEN_LIFETIME: Duration = Duration::from_secs(5 * 60);
const MIN_SIGNING_KEY_BYTES: usize = 32;
const MAX_SIGNING_KEY_BYTES: u64 = 4 * 1024;
const ACCESS_TOKEN_AUDIENCE: &str = "anvil-access";
const ACCESS_TOKEN_PURPOSE: &str = "access";
const PUT_TOKEN_AUDIENCE: &str = "anvil-put";
const PUT_TOKEN_PURPOSE: &str = "put";
const JWT_SIGNING_KEY_FINGERPRINT_CONTEXT: &str = "anvil.auth/jwt-signing-key/v1";
const CREDENTIAL_ENVELOPE_KEY_CONTEXT: &str = "anvil.auth/s3-credential-envelope/aes256gcm/v1";
const PERSONALDB_SIGNING_MASTER_CONTEXT: &str = "anvil.personaldb/signing-master/v1";
const CREDENTIAL_ENVELOPE_AAD_VERSION: u8 = 1;

/// Identity established from trusted credentials for one request.
///
/// Fields are private so request payloads cannot modify an authenticated
/// identity after the interceptor has constructed it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Caller {
    storage_tenant: StorageTenantId,
    subject: ObjectRef,
}

/// Explicit ingress marker for an Object-service request that omitted the
/// authorization header. Read RPCs bind this global anonymous principal to the
/// tenant named by the requested object before evaluating Zanzibar. Other
/// services and Object-service mutations continue to require a [`Caller`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AnonymousObjectRequest;

/// Explicit server rate-limit policy. All values are non-zero so a deployed
/// server cannot accidentally construct a disabled or unusable GCRA quota.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateLimitConfig {
    pub global_per_second: NonZeroU32,
    pub global_burst: NonZeroU32,
    pub authenticated_per_second: NonZeroU32,
    pub authenticated_burst: NonZeroU32,
    pub credential_global_per_minute: NonZeroU32,
    pub credential_global_burst: NonZeroU32,
    pub credential_client_per_minute: NonZeroU32,
    pub credential_client_burst: NonZeroU32,
    pub keyed_cleanup_interval: NonZeroU64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            global_per_second: NonZeroU32::new(10_000).expect("constant is non-zero"),
            global_burst: NonZeroU32::new(10_000).expect("constant is non-zero"),
            authenticated_per_second: NonZeroU32::new(1_000).expect("constant is non-zero"),
            authenticated_burst: NonZeroU32::new(1_000).expect("constant is non-zero"),
            credential_global_per_minute: NonZeroU32::new(100).expect("constant is non-zero"),
            credential_global_burst: NonZeroU32::new(20).expect("constant is non-zero"),
            credential_client_per_minute: NonZeroU32::new(10).expect("constant is non-zero"),
            credential_client_burst: NonZeroU32::new(3).expect("constant is non-zero"),
            keyed_cleanup_interval: NonZeroU64::new(1_024).expect("constant is non-zero"),
        }
    }
}

/// One shared, fail-fast limiter set for the single public listener.
///
/// Protected traffic consumes the server-global quota before bearer parsing,
/// then a quota keyed by the authenticated tenant/application identity.
/// Credential exchange consumes that same server-global quota plus stricter
/// exchange-global and client-ID quotas before running the password KDF.
#[derive(Clone)]
pub struct RequestRateLimits {
    global: Arc<DefaultDirectRateLimiter>,
    authenticated: Arc<DefaultKeyedRateLimiter<Caller>>,
    credential_global: Arc<DefaultDirectRateLimiter>,
    credential_clients: Arc<DefaultKeyedRateLimiter<[u8; 32]>>,
    authenticated_checks: Arc<AtomicU64>,
    credential_checks: Arc<AtomicU64>,
    cleanup_interval: NonZeroU64,
}

impl std::fmt::Debug for RequestRateLimits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RequestRateLimits")
            .field("cleanup_interval", &self.cleanup_interval)
            .finish_non_exhaustive()
    }
}

impl RequestRateLimits {
    pub fn new(config: RateLimitConfig) -> Self {
        let global = Quota::per_second(config.global_per_second).allow_burst(config.global_burst);
        let authenticated = Quota::per_second(config.authenticated_per_second)
            .allow_burst(config.authenticated_burst);
        let credential_global = Quota::per_minute(config.credential_global_per_minute)
            .allow_burst(config.credential_global_burst);
        let credential_client = Quota::per_minute(config.credential_client_per_minute)
            .allow_burst(config.credential_client_burst);
        Self {
            global: Arc::new(RateLimiter::direct(global)),
            authenticated: Arc::new(RateLimiter::keyed(authenticated)),
            credential_global: Arc::new(RateLimiter::direct(credential_global)),
            credential_clients: Arc::new(RateLimiter::keyed(credential_client)),
            authenticated_checks: Arc::new(AtomicU64::new(0)),
            credential_checks: Arc::new(AtomicU64::new(0)),
            cleanup_interval: config.keyed_cleanup_interval,
        }
    }

    /// Fail fast globally, authenticate, then enforce one quota for the exact
    /// tenant/application identity installed on the request.
    pub fn authenticate<T>(
        &self,
        tokens: &JwtManager,
        request: Request<T>,
    ) -> Result<Request<T>, Status> {
        check_direct(&self.global, "server")?;
        self.authenticate_after_global_limit(tokens, request)
    }

    pub(crate) fn check_gateway_global(&self) -> Result<(), Status> {
        check_direct(&self.global, "server")
    }

    pub(crate) fn check_gateway_identity(&self, caller: &Caller) -> Result<(), Status> {
        check_keyed(&self.authenticated, caller, "authenticated caller")?;
        self.retain_authenticated_recently();
        Ok(())
    }

    /// Applies the normal authenticated path when a bearer is present. A
    /// genuinely missing header is retained as an explicit anonymous marker;
    /// malformed, duplicate, invalid and expired supplied bearers still fail.
    /// This interceptor is installed only on the Object service.
    pub fn authenticate_object<T>(
        &self,
        tokens: &JwtManager,
        mut request: Request<T>,
    ) -> Result<Request<T>, Status> {
        check_direct(&self.global, "server")?;
        if request
            .metadata()
            .get_all("authorization")
            .iter()
            .next()
            .is_none()
        {
            let anonymous = Caller::from_anonymous(StorageTenantId::system());
            check_keyed(&self.authenticated, &anonymous, "anonymous caller")?;
            self.retain_authenticated_recently();
            request.extensions_mut().insert(AnonymousObjectRequest);
            return Ok(request);
        }
        self.authenticate_after_global_limit(tokens, request)
    }

    fn authenticate_after_global_limit<T>(
        &self,
        tokens: &JwtManager,
        request: Request<T>,
    ) -> Result<Request<T>, Status> {
        let request = tokens.authenticate(request)?;
        let caller = request
            .extensions()
            .get::<Caller>()
            .ok_or_else(|| Status::internal("authenticated caller identity was not installed"))?;
        check_keyed(&self.authenticated, caller, "authenticated caller")?;
        self.retain_authenticated_recently();
        Ok(request)
    }

    /// Applies before credential lookup or password verification. Client IDs
    /// are reduced to fixed-size digests so hostile input is never retained in
    /// the keyed limiter. No socket address or forwarded-IP metadata is used.
    pub fn check_credential_exchange(&self, client_id: &str) -> Result<(), Status> {
        check_direct(&self.global, "server")?;
        check_direct(&self.credential_global, "credential exchange")?;
        let client_key = *blake3::hash(client_id.as_bytes()).as_bytes();
        check_keyed(&self.credential_clients, &client_key, "client credential")?;
        self.retain_credential_clients_recently();
        Ok(())
    }

    fn retain_authenticated_recently(&self) {
        if decision_triggers_cleanup(&self.authenticated_checks, self.cleanup_interval) {
            self.authenticated.retain_recent();
        }
    }

    fn retain_credential_clients_recently(&self) {
        if decision_triggers_cleanup(&self.credential_checks, self.cleanup_interval) {
            self.credential_clients.retain_recent();
        }
    }
}

fn decision_triggers_cleanup(counter: &AtomicU64, interval: NonZeroU64) -> bool {
    counter.fetch_add(1, Ordering::Relaxed).wrapping_add(1) % interval.get() == 0
}

fn check_direct(limiter: &DefaultDirectRateLimiter, scope: &'static str) -> Result<(), Status> {
    limiter.check().map_err(|not_until| {
        rate_limited_status(scope, not_until.wait_time_from(limiter.clock().now()))
    })
}

fn check_keyed<K>(
    limiter: &DefaultKeyedRateLimiter<K>,
    key: &K,
    scope: &'static str,
) -> Result<(), Status>
where
    K: Clone + Eq + std::hash::Hash,
{
    limiter.check_key(key).map_err(|not_until| {
        rate_limited_status(scope, not_until.wait_time_from(limiter.clock().now()))
    })
}

fn rate_limited_status(scope: &'static str, retry_after: Duration) -> Status {
    let retry_after_millis = retry_after.as_millis().max(1);
    Status::resource_exhausted(format!(
        "{scope} rate limit exceeded; retry after {retry_after_millis} ms"
    ))
}

impl Caller {
    pub fn storage_tenant(&self) -> &StorageTenantId {
        &self.storage_tenant
    }

    pub fn subject(&self) -> &ObjectRef {
        &self.subject
    }

    pub(crate) fn authenticated_app_id(&self) -> Result<&str, AuthenticationError> {
        match &self.subject.id {
            anvil_authz::ObjectId::Opaque(app_id)
                if !self.subject.is_public() && !self.subject.is_anonymous() =>
            {
                Ok(app_id)
            }
            _ => Err(AuthenticationError::InvalidIdentity(
                "caller is not an authenticated application".into(),
            )),
        }
    }

    pub(crate) fn from_authenticated_application(
        storage_tenant: StorageTenantId,
        app_id: impl Into<String>,
    ) -> Result<Self, AuthenticationError> {
        let subject = ObjectRef::opaque("app", app_id.into())
            .map_err(|error| AuthenticationError::InvalidIdentity(error.to_string()))?;
        if subject.is_public() || subject.is_anonymous() {
            return Err(AuthenticationError::InvalidIdentity(
                "reserved non-credentialed subjects cannot authenticate".into(),
            ));
        }
        Ok(Self {
            storage_tenant,
            subject,
        })
    }

    pub(crate) fn from_anonymous(storage_tenant: StorageTenantId) -> Self {
        Self {
            storage_tenant,
            subject: ObjectRef::anonymous(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthenticationError {
    #[error("the token signing secret must not be empty")]
    EmptySigningSecret,
    #[error("the token signing secret must contain at least 32 bytes")]
    SigningSecretTooShort,
    #[error("invalid authenticated identity: {0}")]
    InvalidIdentity(String),
    #[error("the system clock predates the Unix epoch")]
    InvalidSystemClock,
    #[error("access token timestamp overflow")]
    TimestampOverflow,
    #[error("put-token lifetime must be exactly five minutes")]
    InvalidPutTokenLifetime,
    #[error("token has the wrong purpose")]
    WrongTokenPurpose,
    #[error("put token contains an invalid canonical header: {0}")]
    InvalidPutHeader(String),
    #[error("access token could not be encoded: {0}")]
    Encode(#[source] jsonwebtoken::errors::Error),
    #[error("access token could not be verified: {0}")]
    Verify(#[source] jsonwebtoken::errors::Error),
    #[error("token signing key file is invalid: {0}")]
    SigningKeyFile(String),
    #[error("credential secret envelope could not be processed")]
    CredentialEnvelope,
}

/// Loads a bounded operator-managed key without accepting a symbolic link.
/// Anvil never persists or logs the returned bytes.
pub fn load_token_signing_key(path: &Path) -> Result<Vec<u8>, AuthenticationError> {
    #[cfg(not(unix))]
    {
        let _ = path;
        return Err(AuthenticationError::SigningKeyFile(
            "mode-0600 signing key files require a Unix host".into(),
        ));
    }
    #[cfg(unix)]
    {
        let before = std::fs::symlink_metadata(path)
            .map_err(|error| AuthenticationError::SigningKeyFile(error.to_string()))?;
        if !before.file_type().is_file() || before.file_type().is_symlink() {
            return Err(AuthenticationError::SigningKeyFile(
                "path must name a regular non-symlink file".into(),
            ));
        }
        if before.mode() & 0o7777 != 0o600 {
            return Err(AuthenticationError::SigningKeyFile(
                "file mode must be exactly 0600".into(),
            ));
        }
        let file = File::open(path)
            .map_err(|error| AuthenticationError::SigningKeyFile(error.to_string()))?;
        let opened = file
            .metadata()
            .map_err(|error| AuthenticationError::SigningKeyFile(error.to_string()))?;
        if !opened.file_type().is_file()
            || opened.dev() != before.dev()
            || opened.ino() != before.ino()
            || opened.mode() & 0o7777 != 0o600
        {
            return Err(AuthenticationError::SigningKeyFile(
                "file changed while it was being opened".into(),
            ));
        }
        let mut key = Vec::new();
        file.take(MAX_SIGNING_KEY_BYTES + 1)
            .read_to_end(&mut key)
            .map_err(|error| AuthenticationError::SigningKeyFile(error.to_string()))?;
        if key.len() < MIN_SIGNING_KEY_BYTES {
            return Err(AuthenticationError::SigningKeyFile(format!(
                "file must contain at least {MIN_SIGNING_KEY_BYTES} bytes"
            )));
        }
        if key.len() as u64 > MAX_SIGNING_KEY_BYTES {
            return Err(AuthenticationError::SigningKeyFile(format!(
                "file exceeds {MAX_SIGNING_KEY_BYTES} bytes"
            )));
        }
        Ok(key)
    }
}

#[derive(Clone)]
pub struct JwtManager {
    encoding_key: Arc<EncodingKey>,
    decoding_key: Arc<DecodingKey>,
    access_validation: Arc<Validation>,
    put_validation: Arc<Validation>,
    watch_checkpoint_validation: Arc<Validation>,
    index_page_validation: Arc<Validation>,
    signing_key_fingerprint: JwtSigningKeyFingerprint,
    credential_envelope_key: Arc<[u8; 32]>,
    personaldb_signing_master: Arc<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PersonalDbSigningPurpose {
    GroupControl,
    ProjectionBuilder,
    Snapshot,
    Witness,
}

impl PersonalDbSigningPurpose {
    const fn context(self) -> &'static str {
        match self {
            Self::GroupControl => "anvil.personaldb/group-control/ed25519/v1",
            Self::ProjectionBuilder => "anvil.personaldb/projection-builder/ed25519/v1",
            Self::Snapshot => "anvil.personaldb/snapshot/ed25519/v1",
            Self::Witness => "anvil.personaldb/witness/ed25519/v1",
        }
    }
}

impl std::fmt::Debug for JwtManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("JwtManager").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AccessTokenClaims {
    sub: String,
    storage_tenant: String,
    exp: u64,
    jti: String,
    aud: String,
    purpose: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PutTokenWireClaims {
    sub: String,
    storage_tenant: String,
    iat: u64,
    exp: u64,
    jti: String,
    aud: String,
    purpose: String,
    header_hex: String,
}

/// Verified, purpose-separated admission capability for exactly one canonical
/// put header. The Object service must also compare this identity with the
/// bearer-authenticated caller on the Put stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedPutToken {
    pub storage_tenant: String,
    pub subject: ObjectRef,
    pub header: Vec<u8>,
    pub expires_at_unix_seconds: u64,
    pub token_id: String,
}

impl VerifiedPutToken {
    pub fn belongs_to(&self, caller: &Caller) -> bool {
        self.storage_tenant == caller.storage_tenant().as_str() && &self.subject == caller.subject()
    }
}

impl JwtManager {
    pub fn new(signing_secret: impl AsRef<[u8]>) -> Result<Self, AuthenticationError> {
        let signing_secret = signing_secret.as_ref();
        if signing_secret.is_empty() {
            return Err(AuthenticationError::EmptySigningSecret);
        }
        if signing_secret.len() < MIN_SIGNING_KEY_BYTES {
            return Err(AuthenticationError::SigningSecretTooShort);
        }
        Ok(Self {
            encoding_key: Arc::new(EncodingKey::from_secret(signing_secret)),
            decoding_key: Arc::new(DecodingKey::from_secret(signing_secret)),
            access_validation: Arc::new(token_validation(ACCESS_TOKEN_AUDIENCE)),
            put_validation: Arc::new(token_validation(PUT_TOKEN_AUDIENCE)),
            watch_checkpoint_validation: Arc::new(watch_checkpoint_validation()),
            index_page_validation: Arc::new(index_page_validation()),
            signing_key_fingerprint: JwtSigningKeyFingerprint(blake3::derive_key(
                JWT_SIGNING_KEY_FINGERPRINT_CONTEXT,
                signing_secret,
            )),
            credential_envelope_key: Arc::new(blake3::derive_key(
                CREDENTIAL_ENVELOPE_KEY_CONTEXT,
                signing_secret,
            )),
            personaldb_signing_master: Arc::new(blake3::derive_key(
                PERSONALDB_SIGNING_MASTER_CONTEXT,
                signing_secret,
            )),
        })
    }

    pub(crate) fn personaldb_signing_seed(&self, purpose: PersonalDbSigningPurpose) -> [u8; 32] {
        blake3::derive_key(purpose.context(), self.personaldb_signing_master.as_ref())
    }

    pub(crate) fn seal_sigv4_secret(
        &self,
        storage_tenant: &StorageTenantId,
        app_id: &str,
        client_id: &str,
        secret: &str,
    ) -> Result<CredentialSecretEnvelope, AuthenticationError> {
        let cipher = Aes256Gcm::new_from_slice(self.credential_envelope_key.as_ref())
            .map_err(|_| AuthenticationError::CredentialEnvelope)?;
        let mut nonce = [0_u8; 12];
        getrandom::fill(&mut nonce).map_err(|_| AuthenticationError::CredentialEnvelope)?;
        let aad = credential_envelope_aad(storage_tenant, app_id, client_id)?;
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: secret.as_bytes(),
                    aad: &aad,
                },
            )
            .map_err(|_| AuthenticationError::CredentialEnvelope)?;
        CredentialSecretEnvelope::new(nonce, ciphertext)
            .map_err(|_| AuthenticationError::CredentialEnvelope)
    }

    pub(crate) fn open_sigv4_secret(
        &self,
        storage_tenant: &StorageTenantId,
        app_id: &str,
        client_id: &str,
        envelope: &CredentialSecretEnvelope,
    ) -> Result<String, AuthenticationError> {
        let cipher = Aes256Gcm::new_from_slice(self.credential_envelope_key.as_ref())
            .map_err(|_| AuthenticationError::CredentialEnvelope)?;
        let aad = credential_envelope_aad(storage_tenant, app_id, client_id)?;
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(envelope.nonce()),
                Payload {
                    msg: envelope.ciphertext(),
                    aad: &aad,
                },
            )
            .map_err(|_| AuthenticationError::CredentialEnvelope)?;
        String::from_utf8(plaintext).map_err(|_| AuthenticationError::CredentialEnvelope)
    }

    /// Bounded, domain-separated identity of the operator-held HS256 key.
    /// The secret itself is never retained in cluster control state.
    pub(crate) fn signing_key_fingerprint(&self) -> JwtSigningKeyFingerprint {
        self.signing_key_fingerprint
    }

    /// Mints a one-hour access token after durable credentials have already
    /// established the application identity.
    pub fn mint(
        &self,
        storage_tenant: StorageTenantId,
        app_id: impl Into<String>,
    ) -> Result<String, AuthenticationError> {
        let app_id = app_id.into();
        let caller = Caller::from_authenticated_application(storage_tenant, app_id.clone())?;
        let now = unix_seconds()?;
        let exp = now
            .checked_add(ACCESS_TOKEN_LIFETIME.as_secs())
            .ok_or(AuthenticationError::TimestampOverflow)?;
        let claims = AccessTokenClaims {
            sub: app_id,
            storage_tenant: caller.storage_tenant.as_str().to_owned(),
            exp,
            jti: uuid::Uuid::new_v4().to_string(),
            aud: ACCESS_TOKEN_AUDIENCE.to_owned(),
            purpose: ACCESS_TOKEN_PURPOSE.to_owned(),
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            self.encoding_key.as_ref(),
        )
        .map_err(AuthenticationError::Encode)
    }

    pub fn verify(&self, token: &str) -> Result<Caller, AuthenticationError> {
        let token = decode::<AccessTokenClaims>(
            token,
            self.decoding_key.as_ref(),
            self.access_validation.as_ref(),
        )
        .map_err(AuthenticationError::Verify)?;
        if token.claims.purpose != ACCESS_TOKEN_PURPOSE {
            return Err(AuthenticationError::WrongTokenPurpose);
        }
        let tenant = StorageTenantId::parse(token.claims.storage_tenant)
            .map_err(|error| AuthenticationError::InvalidIdentity(error.to_string()))?;
        Caller::from_authenticated_application(tenant, token.claims.sub)
    }

    /// Mints a five-minute admission token containing the complete canonical
    /// put header. The header stays opaque to authentication and is decoded by
    /// the Object service after verification.
    pub fn mint_put_token(
        &self,
        caller: &Caller,
        canonical_header: &[u8],
        lifetime: Duration,
    ) -> Result<(String, u64), AuthenticationError> {
        if lifetime != PUT_TOKEN_LIFETIME {
            return Err(AuthenticationError::InvalidPutTokenLifetime);
        }
        let now = unix_seconds()?;
        let exp = now
            .checked_add(lifetime.as_secs())
            .ok_or(AuthenticationError::TimestampOverflow)?;
        let claims = PutTokenWireClaims {
            sub: match &caller.subject.id {
                anvil_authz::ObjectId::Opaque(app_id) => app_id.clone(),
                anvil_authz::ObjectId::ExactPath(_) => {
                    return Err(AuthenticationError::InvalidIdentity(
                        "authenticated application must have an opaque ID".into(),
                    ));
                }
            },
            storage_tenant: caller.storage_tenant.as_str().to_owned(),
            iat: now,
            exp,
            jti: uuid::Uuid::new_v4().to_string(),
            aud: PUT_TOKEN_AUDIENCE.to_owned(),
            purpose: PUT_TOKEN_PURPOSE.to_owned(),
            header_hex: hex::encode(canonical_header),
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            self.encoding_key.as_ref(),
        )
        .map_err(AuthenticationError::Encode)?;
        Ok((token, exp))
    }

    pub fn verify_put_token(&self, token: &str) -> Result<VerifiedPutToken, AuthenticationError> {
        let token = decode::<PutTokenWireClaims>(
            token,
            self.decoding_key.as_ref(),
            self.put_validation.as_ref(),
        )
        .map_err(AuthenticationError::Verify)?;
        if token.claims.purpose != PUT_TOKEN_PURPOSE {
            return Err(AuthenticationError::WrongTokenPurpose);
        }
        if token.claims.exp.checked_sub(token.claims.iat) != Some(PUT_TOKEN_LIFETIME.as_secs())
            || token.claims.iat > unix_seconds()?
        {
            return Err(AuthenticationError::InvalidPutTokenLifetime);
        }
        let tenant = StorageTenantId::parse(token.claims.storage_tenant)
            .map_err(|error| AuthenticationError::InvalidIdentity(error.to_string()))?;
        let caller = Caller::from_authenticated_application(tenant, token.claims.sub)?;
        let header = hex::decode(token.claims.header_hex)
            .map_err(|error| AuthenticationError::InvalidPutHeader(error.to_string()))?;
        Ok(VerifiedPutToken {
            storage_tenant: caller.storage_tenant.as_str().to_owned(),
            subject: caller.subject,
            header,
            expires_at_unix_seconds: token.claims.exp,
            token_id: token.claims.jti,
        })
    }

    /// Sign one purpose-separated, non-expiring index continuation. Its
    /// definition version, immutable generation, and Zanzibar revision bound
    /// useful validity; generation retention eventually makes it unservable.
    pub(crate) fn seal_index_page_token(
        &self,
        claims: &IndexPageTokenClaims,
    ) -> Result<Vec<u8>, String> {
        if !claims.has_valid_envelope() {
            return Err("index page claims are invalid".into());
        }
        encode(
            &Header::new(Algorithm::HS256),
            claims,
            self.encoding_key.as_ref(),
        )
        .map(String::into_bytes)
        .map_err(|error| format!("index page token could not be encoded: {error}"))
    }

    /// Verify signature and the dedicated JWT audience before the index
    /// service compares caller, definition, query, and cursor claims.
    pub(crate) fn open_index_page_token(
        &self,
        token: &[u8],
    ) -> Result<IndexPageTokenClaims, String> {
        let token = std::str::from_utf8(token)
            .map_err(|_| "index page token is not a UTF-8 JWT".to_owned())?;
        let claims = decode::<IndexPageTokenClaims>(
            token,
            self.decoding_key.as_ref(),
            self.index_page_validation.as_ref(),
        )
        .map_err(|error| format!("index page token could not be verified: {error}"))?
        .claims;
        if claims.aud != INDEX_PAGE_TOKEN_AUDIENCE
            || claims.purpose != INDEX_PAGE_TOKEN_PURPOSE
            || !claims.has_valid_envelope()
        {
            return Err("index page token has the wrong audience, purpose, or format".into());
        }
        Ok(claims)
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

impl WatchCheckpointCodec for JwtManager {
    fn seal(&self, claims: &WatchCheckpointClaims) -> Result<Vec<u8>, String> {
        if claims.aud != CHECKPOINT_AUDIENCE || claims.purpose != CHECKPOINT_PURPOSE {
            return Err("watch checkpoint has the wrong audience or purpose".into());
        }
        encode(
            &Header::new(Algorithm::HS256),
            claims,
            self.encoding_key.as_ref(),
        )
        .map(String::into_bytes)
        .map_err(|error| format!("watch checkpoint could not be encoded: {error}"))
    }

    fn open(&self, token: &[u8]) -> Result<WatchCheckpointClaims, String> {
        let token = std::str::from_utf8(token)
            .map_err(|_| "watch checkpoint is not a UTF-8 JWT".to_owned())?;
        let claims = decode::<WatchCheckpointClaims>(
            token,
            self.decoding_key.as_ref(),
            self.watch_checkpoint_validation.as_ref(),
        )
        .map_err(|error| format!("watch checkpoint could not be verified: {error}"))?
        .claims;
        if claims.aud != CHECKPOINT_AUDIENCE || claims.purpose != CHECKPOINT_PURPOSE {
            return Err("watch checkpoint has the wrong audience or purpose".into());
        }
        Ok(claims)
    }
}

fn credential_envelope_aad(
    storage_tenant: &StorageTenantId,
    app_id: &str,
    client_id: &str,
) -> Result<Vec<u8>, AuthenticationError> {
    let mut aad = vec![CREDENTIAL_ENVELOPE_AAD_VERSION];
    for component in [storage_tenant.as_str(), app_id, client_id] {
        let length =
            u32::try_from(component.len()).map_err(|_| AuthenticationError::CredentialEnvelope)?;
        aad.extend_from_slice(&length.to_be_bytes());
        aad.extend_from_slice(component.as_bytes());
    }
    Ok(aad)
}

fn token_validation(audience: &'static str) -> Validation {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 0;
    // jsonwebtoken recognizes only registered JWT claim names here. The
    // strongly typed serde claim structs make every Anvil-specific field
    // mandatory during decoding.
    validation.set_required_spec_claims(&["exp", "sub", "aud"]);
    validation.set_audience(&[audience]);
    validation
}

fn watch_checkpoint_validation() -> Validation {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 0;
    validation.validate_exp = false;
    validation.set_required_spec_claims(&["aud"]);
    validation.set_audience(&[CHECKPOINT_AUDIENCE]);
    validation
}

fn index_page_validation() -> Validation {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 0;
    validation.validate_exp = false;
    validation.set_required_spec_claims(&["aud"]);
    validation.set_audience(&[INDEX_PAGE_TOKEN_AUDIENCE]);
    validation
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
    use anvil_consensus::ClusterId;
    use anvil_store::{PlacementLogId, SourceId, WatchScope};
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use tonic::metadata::MetadataValue;

    use crate::distributed_watch::{DistributedWatchScope, WatchVectorEntry};

    const TEST_SIGNING_SECRET: &[u8; 32] = b"0123456789abcdef0123456789abcdef";

    fn tenant(value: &str) -> StorageTenantId {
        StorageTenantId::parse(value).unwrap()
    }

    fn test_rate_config() -> RateLimitConfig {
        RateLimitConfig {
            global_per_second: NonZeroU32::new(10).unwrap(),
            global_burst: NonZeroU32::new(10).unwrap(),
            authenticated_per_second: NonZeroU32::new(1).unwrap(),
            authenticated_burst: NonZeroU32::new(1).unwrap(),
            credential_global_per_minute: NonZeroU32::new(10).unwrap(),
            credential_global_burst: NonZeroU32::new(10).unwrap(),
            credential_client_per_minute: NonZeroU32::new(1).unwrap(),
            credential_client_burst: NonZeroU32::new(1).unwrap(),
            keyed_cleanup_interval: NonZeroU64::new(1).unwrap(),
        }
    }

    fn bearer_request(manager: &JwtManager, tenant: &str, app_id: &str) -> Request<()> {
        let token = manager
            .mint(StorageTenantId::parse(tenant).unwrap(), app_id)
            .unwrap();
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );
        request
    }

    fn watch_checkpoint_claims() -> WatchCheckpointClaims {
        WatchCheckpointClaims {
            format: 1,
            aud: CHECKPOINT_AUDIENCE.into(),
            purpose: CHECKPOINT_PURPOSE.into(),
            cluster_id: ClusterId([9; 16]),
            scope: DistributedWatchScope::new(
                &WatchScope::new("tenant", "bucket", "docs").unwrap(),
                11,
                22,
            )
            .unwrap(),
            membership_revision: PlacementLogId { term: 3, index: 7 },
            sources: vec![WatchVectorEntry {
                source: SourceId {
                    node_id: 1,
                    source_epoch: [4; 32],
                },
                next_offset: 19,
            }],
        }
    }

    fn sign_checkpoint(
        manager: &JwtManager,
        claims: &WatchCheckpointClaims,
        algorithm: Algorithm,
    ) -> Vec<u8> {
        encode(
            &Header::new(algorithm),
            claims,
            manager.encoding_key.as_ref(),
        )
        .unwrap()
        .into_bytes()
    }

    #[test]
    fn watch_checkpoint_round_trip_has_no_wall_clock_expiry() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let claims = watch_checkpoint_claims();

        let token = manager.seal(&claims).unwrap();

        assert_eq!(manager.open(&token).unwrap(), claims);
        assert_ne!(token, serde_json::to_vec(&claims).unwrap());
    }

    #[test]
    fn watch_checkpoint_rejects_another_signing_key() {
        let issuer = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let verifier = JwtManager::new(b"fedcba9876543210fedcba9876543210").unwrap();
        let token = issuer.seal(&watch_checkpoint_claims()).unwrap();

        assert!(verifier.open(&token).is_err());
    }

    #[test]
    fn watch_checkpoint_rejects_wrong_audience_and_purpose() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let mut wrong_audience = watch_checkpoint_claims();
        wrong_audience.aud = ACCESS_TOKEN_AUDIENCE.into();
        assert!(
            manager
                .open(&sign_checkpoint(
                    &manager,
                    &wrong_audience,
                    Algorithm::HS256
                ))
                .is_err()
        );

        let mut wrong_purpose = watch_checkpoint_claims();
        wrong_purpose.purpose = ACCESS_TOKEN_PURPOSE.into();
        assert!(
            manager
                .open(&sign_checkpoint(&manager, &wrong_purpose, Algorithm::HS256))
                .is_err()
        );
        assert!(manager.seal(&wrong_audience).is_err());
        assert!(manager.seal(&wrong_purpose).is_err());
    }

    #[test]
    fn watch_checkpoint_rejects_non_hs256_and_malformed_tokens() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let claims = watch_checkpoint_claims();
        assert!(
            manager
                .open(&sign_checkpoint(&manager, &claims, Algorithm::HS384))
                .is_err()
        );
        assert!(manager.open(b"not-a-jwt").is_err());
        assert!(manager.open(&[0xff, 0xfe]).is_err());

        let mut extra_claim = serde_json::to_value(&claims).unwrap();
        extra_claim["unexpected"] = serde_json::Value::Bool(true);
        let extra_claim = encode(
            &Header::new(Algorithm::HS256),
            &extra_claim,
            manager.encoding_key.as_ref(),
        )
        .unwrap();
        assert!(manager.open(extra_claim.as_bytes()).is_err());

        let mut tampered = manager.seal(&claims).unwrap();
        let last = tampered.last_mut().unwrap();
        *last = if *last == b'a' { b'b' } else { b'a' };
        assert!(manager.open(&tampered).is_err());
    }

    #[test]
    fn verified_token_establishes_tenant_and_canonical_app_subject() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let token = manager.mint(tenant("acme"), "app-7").unwrap();

        let caller = manager.verify(&token).unwrap();

        assert_eq!(caller.storage_tenant().as_str(), "acme");
        assert_eq!(caller.subject().namespace, "app");
        assert_eq!(caller.subject().id, ObjectId::Opaque("app-7".to_owned()));
    }

    #[test]
    fn interceptor_inserts_only_verified_identity() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let token = manager.mint(tenant("acme"), "app-7").unwrap();
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );

        let request = manager.authenticate(request).unwrap();
        let caller = request.extensions().get::<Caller>().unwrap();

        assert_eq!(caller.storage_tenant().as_str(), "acme");
        assert_eq!(caller.subject().id, ObjectId::Opaque("app-7".to_owned()));
    }

    #[test]
    fn object_interceptor_maps_only_a_missing_header_to_anonymous() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();

        let anonymous = RequestRateLimits::new(test_rate_config())
            .authenticate_object(&manager, Request::new(()))
            .unwrap();
        assert!(
            anonymous
                .extensions()
                .get::<AnonymousObjectRequest>()
                .is_some()
        );
        assert!(anonymous.extensions().get::<Caller>().is_none());

        let authenticated = RequestRateLimits::new(test_rate_config())
            .authenticate_object(&manager, bearer_request(&manager, "acme", "app-7"))
            .unwrap();
        assert!(
            authenticated
                .extensions()
                .get::<AnonymousObjectRequest>()
                .is_none()
        );
        assert_eq!(
            authenticated
                .extensions()
                .get::<Caller>()
                .unwrap()
                .subject()
                .id,
            ObjectId::Opaque("app-7".to_owned())
        );
    }

    #[test]
    fn object_interceptor_rejects_every_supplied_invalid_bearer() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        for value in ["not-bearer", "Bearer ", "Bearer not-a-jwt"] {
            let mut request = Request::new(());
            request
                .metadata_mut()
                .insert("authorization", value.parse().unwrap());
            let error = RequestRateLimits::new(test_rate_config())
                .authenticate_object(&manager, request)
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unauthenticated, "{value}");
        }

        let token = manager.mint(tenant("acme"), "app-7").unwrap();
        let value = MetadataValue::try_from(format!("Bearer {token}")).unwrap();
        let mut duplicate = Request::new(());
        duplicate
            .metadata_mut()
            .append("authorization", value.clone());
        duplicate.metadata_mut().append("authorization", value);
        assert_eq!(
            RequestRateLimits::new(test_rate_config())
                .authenticate_object(&manager, duplicate)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn anonymous_caller_is_the_uncredentialed_reserved_application() {
        let caller = Caller::from_anonymous(tenant("acme"));
        assert_eq!(caller.storage_tenant(), &tenant("acme"));
        assert_eq!(caller.subject(), &ObjectRef::anonymous());
        assert!(caller.subject().is_anonymous());
    }

    #[test]
    fn wrong_key_missing_token_and_duplicate_headers_fail_closed() {
        let issuer = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let verifier = JwtManager::new(b"fedcba9876543210fedcba9876543210").unwrap();
        let token = issuer.mint(tenant("acme"), "app-7").unwrap();
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
    fn protected_limits_are_global_then_exact_authenticated_identity() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let limits = RequestRateLimits::new(test_rate_config());

        let alice = limits
            .authenticate(&manager, bearer_request(&manager, "acme", "alice"))
            .unwrap();
        assert_eq!(
            alice.extensions().get::<Caller>().unwrap().storage_tenant(),
            &tenant("acme")
        );

        let repeated = limits
            .authenticate(&manager, bearer_request(&manager, "acme", "alice"))
            .unwrap_err();
        assert_eq!(repeated.code(), tonic::Code::ResourceExhausted);
        assert!(repeated.message().contains("retry after"));

        // A distinct authenticated application has independent keyed state.
        limits
            .authenticate(&manager, bearer_request(&manager, "acme", "bob"))
            .unwrap();
        // The same application ID in a different tenant is also a distinct
        // immutable caller identity.
        limits
            .authenticate(&manager, bearer_request(&manager, "other", "alice"))
            .unwrap();
    }

    #[test]
    fn credential_limits_are_stricter_globally_and_per_client_id() {
        let limits = RequestRateLimits::new(test_rate_config());

        limits.check_credential_exchange("client-a").unwrap();
        let repeated = limits.check_credential_exchange("client-a").unwrap_err();
        assert_eq!(repeated.code(), tonic::Code::ResourceExhausted);
        assert!(repeated.message().contains("client credential"));
        limits.check_credential_exchange("client-b").unwrap();
    }

    #[test]
    fn sigv4_secret_envelope_is_randomized_and_identity_authenticated() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let tenant = tenant("acme");
        let first = manager
            .seal_sigv4_secret(
                &tenant,
                "app",
                "client",
                "a-secret-with-at-least-32-bytes!!",
            )
            .unwrap();
        let second = manager
            .seal_sigv4_secret(
                &tenant,
                "app",
                "client",
                "a-secret-with-at-least-32-bytes!!",
            )
            .unwrap();
        assert_ne!(first.nonce(), second.nonce());
        assert_eq!(
            manager
                .open_sigv4_secret(&tenant, "app", "client", &first)
                .unwrap(),
            "a-secret-with-at-least-32-bytes!!"
        );
        assert!(
            manager
                .open_sigv4_secret(&tenant, "another-app", "client", &first)
                .is_err()
        );
    }

    #[test]
    fn global_limit_fails_before_bearer_validation() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let mut config = test_rate_config();
        config.global_per_second = NonZeroU32::new(1).unwrap();
        config.global_burst = NonZeroU32::new(1).unwrap();
        let limits = RequestRateLimits::new(config);

        let missing = limits.authenticate(&manager, Request::new(())).unwrap_err();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);
        let rejected = limits
            .authenticate(&manager, bearer_request(&manager, "acme", "alice"))
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::ResourceExhausted);
        assert!(rejected.message().contains("server"));
    }

    #[test]
    fn expired_token_is_rejected() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let claims = AccessTokenClaims {
            sub: "client-7".to_owned(),
            storage_tenant: "acme".to_owned(),
            exp: 1,
            jti: uuid::Uuid::new_v4().to_string(),
            aud: ACCESS_TOKEN_AUDIENCE.to_owned(),
            purpose: ACCESS_TOKEN_PURPOSE.to_owned(),
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
    fn access_and_put_tokens_have_strictly_separate_purposes() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let caller = Caller::from_authenticated_application(tenant("acme"), "app-7").unwrap();
        let header = br#"{"address":{"tenant":"acme","bucket":"objects","path":"a"}}"#;
        let (put_token, expires_at) = manager
            .mint_put_token(&caller, header, PUT_TOKEN_LIFETIME)
            .unwrap();
        let verified = manager.verify_put_token(&put_token).unwrap();

        assert!(verified.belongs_to(&caller));
        assert_eq!(verified.storage_tenant, "acme");
        assert_eq!(verified.subject, caller.subject().clone());
        assert_eq!(verified.header, header);
        assert_eq!(verified.expires_at_unix_seconds, expires_at);
        assert!(!verified.token_id.is_empty());

        let other = Caller::from_authenticated_application(tenant("acme"), "app-8").unwrap();
        assert!(!verified.belongs_to(&other));

        let access_token = manager.mint(tenant("acme"), "app-7").unwrap();
        assert!(manager.verify(&put_token).is_err());
        assert!(manager.verify_put_token(&access_token).is_err());
    }

    #[test]
    fn put_token_admission_lifetime_is_exactly_five_minutes() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let caller = Caller::from_authenticated_application(tenant("acme"), "app-7").unwrap();

        assert!(matches!(
            manager.mint_put_token(&caller, b"{}", Duration::from_secs(60)),
            Err(AuthenticationError::InvalidPutTokenLifetime)
        ));
    }

    #[test]
    fn token_fields_cannot_encode_an_invalid_tenant_or_subject() {
        let manager = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        assert!(manager.mint(tenant("acme"), "subject\nsmuggling").is_err());
        assert!(
            manager
                .mint(tenant("acme"), anvil_authz::PUBLIC_SUBJECT_ID)
                .is_err()
        );
        assert!(
            manager
                .mint(tenant("acme"), anvil_authz::ANONYMOUS_SUBJECT_ID)
                .is_err()
        );
    }

    #[test]
    fn signing_secret_is_required() {
        assert!(matches!(
            JwtManager::new([]),
            Err(AuthenticationError::EmptySigningSecret)
        ));
        assert!(matches!(
            JwtManager::new([7_u8; 31]),
            Err(AuthenticationError::SigningSecretTooShort)
        ));
    }

    #[test]
    fn signing_key_fingerprint_is_domain_separated_and_stable() {
        let first = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let same = JwtManager::new(TEST_SIGNING_SECRET).unwrap();
        let other = JwtManager::new(b"fedcba9876543210fedcba9876543210").unwrap();

        assert_eq!(
            first.signing_key_fingerprint(),
            same.signing_key_fingerprint()
        );
        assert_ne!(
            first.signing_key_fingerprint(),
            other.signing_key_fingerprint()
        );
        assert_eq!(
            first.signing_key_fingerprint(),
            JwtSigningKeyFingerprint(blake3::derive_key(
                "anvil.auth/jwt-signing-key/v1",
                TEST_SIGNING_SECRET,
            ))
        );
        assert_ne!(first.signing_key_fingerprint().0, [0; 32]);
    }

    #[cfg(unix)]
    #[test]
    fn signing_key_loader_requires_regular_mode_0600_bounded_file() {
        let directory = tempfile::tempdir().unwrap();
        let key_path = directory.path().join("signing.key");
        std::fs::write(&key_path, [7_u8; 32]).unwrap();
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(load_token_signing_key(&key_path).unwrap(), vec![7_u8; 32]);

        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(load_token_signing_key(&key_path).is_err());
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();

        let link_path = directory.path().join("signing-link.key");
        symlink(&key_path, &link_path).unwrap();
        assert!(load_token_signing_key(&link_path).is_err());

        let short_path = directory.path().join("short.key");
        std::fs::write(&short_path, [1_u8; 31]).unwrap();
        std::fs::set_permissions(&short_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(load_token_signing_key(&short_path).is_err());

        let large_path = directory.path().join("large.key");
        std::fs::write(&large_path, vec![1_u8; MAX_SIGNING_KEY_BYTES as usize + 1]).unwrap();
        std::fs::set_permissions(&large_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(load_token_signing_key(&large_path).is_err());
    }
}
