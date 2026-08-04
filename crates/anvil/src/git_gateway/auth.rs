use anvil_api::v1::ExchangeClientCredentialsRequest;
use axum::http::{self, HeaderMap};
use base64::Engine as _;

use super::{GitError, GitGatewayState};
use crate::v05::GatewayIdentity;

pub(super) async fn authenticate(
    state: &GitGatewayState,
    headers: &HeaderMap,
) -> Result<GatewayIdentity, GitError> {
    state
        .serving
        .require(tonic::Request::new(()))
        .map_err(GitError::from_status)?;
    state
        .rate_limits
        .check_gateway_global()
        .map_err(GitError::from_status)?;
    let mut values = headers.get_all(http::header::AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        return Ok(GatewayIdentity::Anonymous);
    };
    if values.next().is_some() {
        return Err(GitError::unauthorized(
            "multiple Authorization headers are not accepted",
        ));
    }
    let value = value
        .to_str()
        .map_err(|_| GitError::unauthorized("Authorization is malformed"))?;
    let (caller, bearer) = if let Some(token) = value.strip_prefix("Bearer ") {
        if token.is_empty() {
            return Err(GitError::unauthorized("Bearer token is empty"));
        }
        let caller = state
            .tokens
            .verify(token)
            .map_err(|_| GitError::unauthorized("Bearer token is invalid or expired"))?;
        (caller, token.to_owned())
    } else if let Some(encoded) = value.strip_prefix("Basic ") {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| GitError::unauthorized("Basic credentials are malformed"))?;
        let decoded = String::from_utf8(decoded)
            .map_err(|_| GitError::unauthorized("Basic credentials are malformed"))?;
        let (client_id, client_secret) = decoded
            .split_once(':')
            .filter(|(client_id, secret)| !client_id.is_empty() && !secret.is_empty())
            .ok_or_else(|| GitError::unauthorized("Basic credentials are malformed"))?;
        let cache_key =
            blake3::derive_key("anvil.git/basic-credential-cache/v1", decoded.as_bytes());
        let cached = state
            .basic_tokens
            .lock()
            .map_err(|_| GitError::internal("Git credential cache is unavailable"))?
            .get(&cache_key)
            .cloned();
        let access_token = if let Some(token) = cached
            && state.tokens.verify(&token).is_ok()
        {
            token
        } else {
            state
                .rate_limits
                .check_credential_exchange(client_id)
                .map_err(GitError::from_status)?;
            let access = state
                .control
                .exchange_client_credentials(ExchangeClientCredentialsRequest {
                    client_id: client_id.to_owned(),
                    client_secret: client_secret.to_owned(),
                })
                .await
                .map_err(|_| GitError::unauthorized("client credentials are invalid"))?;
            let mut cache = state
                .basic_tokens
                .lock()
                .map_err(|_| GitError::internal("Git credential cache is unavailable"))?;
            if cache.len() >= 1_024 {
                cache.clear();
            }
            cache.insert(cache_key, access.access_token.clone());
            access.access_token
        };
        let caller = state
            .tokens
            .verify(&access_token)
            .map_err(|_| GitError::internal("credential exchange returned an invalid token"))?;
        (caller, access_token)
    } else {
        return Err(GitError::unauthorized(
            "Authorization must use Basic or Bearer",
        ));
    };
    state
        .rate_limits
        .check_gateway_identity(&caller)
        .map_err(GitError::from_status)?;
    Ok(GatewayIdentity::Authenticated { caller, bearer })
}
