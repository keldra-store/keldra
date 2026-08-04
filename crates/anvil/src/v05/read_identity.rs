//! Request identity for Object-service read RPCs.
//!
//! Authentication remains strict everywhere else. An anonymous read is bound
//! to the tenant carried by the validated object address before Zanzibar sees
//! the reserved, non-credentialed application principal.

use anvil_store::StorageTenantId;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

use crate::authentication::{AnonymousObjectRequest, Caller};
use crate::distributed_list::OriginalBearer;

#[derive(Clone, Debug)]
pub(super) enum ObjectReadIdentity {
    Authenticated(Caller),
    Anonymous,
}

impl ObjectReadIdentity {
    pub(super) fn from_request<T>(request: &Request<T>) -> Result<Self, Status> {
        match (
            request.extensions().get::<Caller>(),
            request.extensions().get::<AnonymousObjectRequest>(),
        ) {
            (Some(caller), None) => Ok(Self::Authenticated(caller.clone())),
            (None, Some(_)) => Ok(Self::Anonymous),
            (Some(_), Some(_)) => Err(Status::internal(
                "object request contains contradictory caller identities",
            )),
            (None, None) => Err(Status::unauthenticated("request identity is missing")),
        }
    }

    pub(super) fn caller_for_tenant(&self, tenant: &str) -> Result<Caller, Status> {
        match self {
            Self::Authenticated(caller) => Ok(caller.clone()),
            Self::Anonymous => StorageTenantId::parse(tenant)
                .map(Caller::from_anonymous)
                .map_err(|error| Status::invalid_argument(error.to_string())),
        }
    }

    pub(super) fn original_bearer(&self, metadata: &MetadataMap) -> Result<OriginalBearer, Status> {
        match self {
            Self::Authenticated(_) => OriginalBearer::from_metadata(metadata),
            Self::Anonymous if metadata.get("authorization").is_none() => {
                Ok(OriginalBearer::anonymous())
            }
            Self::Anonymous => Err(Status::unauthenticated(
                "anonymous object request unexpectedly supplied authorization",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anvil_store::StorageTenantId;

    #[test]
    fn missing_header_identity_binds_to_the_requested_tenant() {
        let mut request = Request::new(());
        request.extensions_mut().insert(AnonymousObjectRequest);

        let identity = ObjectReadIdentity::from_request(&request).unwrap();
        let caller = identity.caller_for_tenant("acme").unwrap();

        assert_eq!(caller.storage_tenant().as_str(), "acme");
        assert_eq!(caller.subject(), &anvil_authz::ObjectRef::anonymous());
        assert!(
            identity
                .original_bearer(request.metadata())
                .unwrap()
                .is_anonymous()
        );
    }

    #[test]
    fn authenticated_identity_and_signed_bearer_are_preserved() {
        let expected = Caller::from_authenticated_application(
            StorageTenantId::parse("acme").unwrap(),
            "reader",
        )
        .unwrap();
        let mut request = Request::new(());
        request.extensions_mut().insert(expected.clone());
        request
            .metadata_mut()
            .insert("authorization", "Bearer signed.jwt".parse().unwrap());

        let identity = ObjectReadIdentity::from_request(&request).unwrap();
        assert_eq!(identity.caller_for_tenant("acme").unwrap(), expected);
        assert_eq!(
            identity
                .original_bearer(request.metadata())
                .unwrap()
                .signed_token(),
            "signed.jwt"
        );
    }

    #[test]
    fn absent_or_contradictory_identity_fails_closed() {
        assert_eq!(
            ObjectReadIdentity::from_request(&Request::new(()))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        let mut contradictory = Request::new(());
        contradictory
            .extensions_mut()
            .insert(AnonymousObjectRequest);
        contradictory.extensions_mut().insert(
            Caller::from_authenticated_application(
                StorageTenantId::parse("acme").unwrap(),
                "reader",
            )
            .unwrap(),
        );
        assert_eq!(
            ObjectReadIdentity::from_request(&contradictory)
                .unwrap_err()
                .code(),
            tonic::Code::Internal
        );
    }
}
