//! Unauthenticated exchange of durable application credentials for short-lived tokens.

use std::sync::Arc;

use keldra_api::v1 as api;
use keldra_api::v1::credential_service_server::CredentialService;
use keldra_store::{CredentialRepositoryError, Store};
use tonic::{Request, Response, Status};

use crate::authentication::{ACCESS_TOKEN_LIFETIME, JwtManager, RequestRateLimits};
use crate::distributed_control_plane::DistributedControlPlane;

#[derive(Clone)]
pub(crate) struct CredentialServiceImpl {
    store: Store,
    tokens: JwtManager,
    rate_limits: RequestRateLimits,
    distributed: Option<Arc<DistributedControlPlane>>,
}

impl CredentialServiceImpl {
    pub(crate) fn new(store: Store, tokens: JwtManager, rate_limits: RequestRateLimits) -> Self {
        Self {
            store,
            tokens,
            rate_limits,
            distributed: None,
        }
    }

    pub(crate) fn with_distributed(mut self, distributed: Arc<DistributedControlPlane>) -> Self {
        self.distributed = Some(distributed);
        self
    }
}

#[tonic::async_trait]
impl CredentialService for CredentialServiceImpl {
    async fn exchange_client_credentials(
        &self,
        request: Request<api::ExchangeClientCredentialsRequest>,
    ) -> Result<Response<api::AccessToken>, Status> {
        let request = request.into_inner();
        self.rate_limits
            .check_credential_exchange(&request.client_id)?;
        if let Some(distributed) = self.distributed.as_ref() {
            return distributed
                .exchange_client_credentials(request)
                .await
                .map(Response::new);
        }
        let store = self.store.clone();
        let verified = tokio::task::spawn_blocking(move || {
            store.verify_credential(&request.client_id, &request.client_secret)
        })
        .await
        .map_err(|_| Status::internal("credential verification task failed"))?;
        let credential = match verified {
            Ok(Some(credential)) => credential,
            Ok(None) | Err(CredentialRepositoryError::InvalidInput(_)) => {
                return Err(Status::unauthenticated(
                    "the client credentials are invalid",
                ));
            }
            Err(_) => return Err(Status::internal("credential state could not be read")),
        };
        let access_token = self
            .tokens
            .mint(credential.storage_tenant, credential.app_id)
            .map_err(|_| Status::internal("access token could not be minted"))?;
        Ok(Response::new(api::AccessToken {
            access_token,
            token_type: "Bearer".into(),
            expires_in_seconds: ACCESS_TOKEN_LIFETIME.as_secs(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use keldra_api::v1::credential_service_server::CredentialService;
    use keldra_store::{AuthzRevision, StorageTenantId, StoreOptions, SystemBootstrapRequest};

    use super::*;

    const SECRET: &str = "secret-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    async fn service() -> (tempfile::TempDir, Store, JwtManager, CredentialServiceImpl) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        store
            .bootstrap_system(SystemBootstrapRequest {
                app_id: "bootstrap-app".into(),
                client_id: "bootstrap-client".into(),
                client_secret: SECRET.into(),
            })
            .unwrap();
        let tokens = JwtManager::new("0123456789abcdef0123456789abcdef").unwrap();
        let service = CredentialServiceImpl::new(
            store.clone(),
            tokens.clone(),
            RequestRateLimits::new(crate::authentication::RateLimitConfig::default()),
        );
        (directory, store, tokens, service)
    }

    #[tokio::test]
    async fn verified_client_credential_mints_stable_app_identity() {
        let (_directory, _store, tokens, service) = service().await;

        let response = service
            .exchange_client_credentials(Request::new(api::ExchangeClientCredentialsRequest {
                client_id: "bootstrap-client".into(),
                client_secret: SECRET.into(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.token_type, "Bearer");
        assert_eq!(response.expires_in_seconds, 3_600);
        let caller = tokens.verify(&response.access_token).unwrap();
        assert_eq!(caller.storage_tenant(), &StorageTenantId::system());
        assert_eq!(caller.subject().namespace, "app");
        assert_eq!(
            caller.subject().id,
            keldra_authz::ObjectId::Opaque("bootstrap-app".into())
        );
    }

    #[tokio::test]
    async fn wrong_unknown_and_disabled_credentials_are_indistinguishable() {
        let (_directory, store, _tokens, service) = service().await;
        for request in [
            api::ExchangeClientCredentialsRequest {
                client_id: "bootstrap-client".into(),
                client_secret: "wrong-0123456789abcdef0123456789abcdef".into(),
            },
            api::ExchangeClientCredentialsRequest {
                client_id: "missing-client".into(),
                client_secret: SECRET.into(),
            },
            api::ExchangeClientCredentialsRequest {
                client_id: "malformed/client".into(),
                client_secret: SECRET.into(),
            },
        ] {
            let error = service
                .exchange_client_credentials(Request::new(request))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unauthenticated);
            assert_eq!(error.message(), "the client credentials are invalid");
        }

        store
            .disable_application_credential(
                StorageTenantId::system(),
                "bootstrap-app".into(),
                "bootstrap-client".into(),
                AuthzRevision(3),
            )
            .unwrap();
        let error = service
            .exchange_client_credentials(Request::new(api::ExchangeClientCredentialsRequest {
                client_id: "bootstrap-client".into(),
                client_secret: SECRET.into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(error.message(), "the client credentials are invalid");
    }
}
