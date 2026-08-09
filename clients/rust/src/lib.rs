//! Authenticated Rust client for Anvil 0.6 object storage.
//!
//! The crate provides ready-to-use object, authorization, administration, and
//! PersonalDB clients, upload helpers, and the complete generated protocol surface.

use anvil_api::v1::administration_service_client::AdministrationServiceClient;
use anvil_api::v1::authz_service_client::AuthzServiceClient;
use anvil_api::v1::credential_service_client::CredentialServiceClient;
use anvil_api::v1::object_service_client::ObjectServiceClient;
use anvil_api::v1::personal_db_service_client::PersonalDbServiceClient;
use anvil_api::v1::{
    AccessToken, ExchangeClientCredentialsRequest, MutationReceipt, PutHeader, PutRequest,
};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

const MAX_MESSAGE_BYTES: usize = 72 * 1024 * 1024;

pub use anvil_api::v1;

#[derive(Clone)]
pub struct BearerToken {
    value: MetadataValue<Ascii>,
}

impl BearerToken {
    pub fn new(token: &str) -> Result<Self, tonic::metadata::errors::InvalidMetadataValue> {
        format!("Bearer {token}")
            .parse()
            .map(|value| Self { value })
    }
}

impl Interceptor for BearerToken {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.value.clone());
        Ok(request)
    }
}

pub type RawClient =
    ObjectServiceClient<tonic::service::interceptor::InterceptedService<Channel, BearerToken>>;
pub type RawAdministrationClient = AdministrationServiceClient<
    tonic::service::interceptor::InterceptedService<Channel, BearerToken>,
>;
pub type RawAuthzClient =
    AuthzServiceClient<tonic::service::interceptor::InterceptedService<Channel, BearerToken>>;
pub type RawPersonalDbClient =
    PersonalDbServiceClient<tonic::service::interceptor::InterceptedService<Channel, BearerToken>>;

pub fn object_client(
    channel: Channel,
    token: &str,
) -> Result<RawClient, tonic::metadata::errors::InvalidMetadataValue> {
    Ok(
        ObjectServiceClient::with_interceptor(channel, BearerToken::new(token)?)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
            .max_decoding_message_size(MAX_MESSAGE_BYTES),
    )
}

pub fn administration_client(
    channel: Channel,
    token: &str,
) -> Result<RawAdministrationClient, tonic::metadata::errors::InvalidMetadataValue> {
    Ok(
        AdministrationServiceClient::with_interceptor(channel, BearerToken::new(token)?)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
            .max_decoding_message_size(MAX_MESSAGE_BYTES),
    )
}

pub fn authz_client(
    channel: Channel,
    token: &str,
) -> Result<RawAuthzClient, tonic::metadata::errors::InvalidMetadataValue> {
    Ok(
        AuthzServiceClient::with_interceptor(channel, BearerToken::new(token)?)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
            .max_decoding_message_size(MAX_MESSAGE_BYTES),
    )
}

pub fn personaldb_client(
    channel: Channel,
    token: &str,
) -> Result<RawPersonalDbClient, tonic::metadata::errors::InvalidMetadataValue> {
    Ok(
        PersonalDbServiceClient::with_interceptor(channel, BearerToken::new(token)?)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
            .max_decoding_message_size(MAX_MESSAGE_BYTES),
    )
}

pub async fn connect(
    endpoint: impl AsRef<str>,
    token: &str,
) -> Result<RawClient, Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel(endpoint).await?;
    Ok(object_client(channel, token)?)
}

/// Opens the shared transport used by object, PersonalDB, authorization and
/// administration clients. Keeping credential exchange separate makes the one
/// unauthenticated RPC explicit at call sites.
pub async fn connect_channel(
    endpoint: impl AsRef<str>,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    Ok(Endpoint::from_shared(endpoint.as_ref().to_owned())?
        .connect()
        .await?)
}

/// Exchanges one durable application credential for a short-lived bearer
/// token. Production endpoints must be protected by TLS; the client secret is
/// sent only in this request and is never retained by this transport.
pub async fn exchange_client_credentials(
    channel: Channel,
    client_id: impl Into<String>,
    client_secret: impl Into<String>,
) -> Result<AccessToken, tonic::Status> {
    CredentialServiceClient::new(channel)
        .exchange_client_credentials(ExchangeClientCredentialsRequest {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        })
        .await
        .map(tonic::Response::into_inner)
}

/// Convenience for callers that want an object client directly from durable
/// credentials. Long-lived applications should repeat exchange when their
/// short-lived token expires, not persist the returned bearer token forever.
pub async fn connect_with_credentials(
    endpoint: impl AsRef<str>,
    client_id: impl Into<String>,
    client_secret: impl Into<String>,
) -> Result<RawClient, Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel(endpoint).await?;
    let token = exchange_client_credentials(channel.clone(), client_id, client_secret).await?;
    Ok(object_client(channel, &token.access_token)?)
}

/// Executes the public StartPut + Put + PutEnd flow for owned chunks.
/// Empty input sends the required single empty chunk and therefore creates a
/// valid zero-byte object. This helper is for already-owned, infallible input;
/// a fallible file or network producer must cancel the in-flight RPC on source
/// failure so a short read cannot become a clean end-of-stream publication.
pub async fn put_chunks<I>(
    client: &mut RawClient,
    header: PutHeader,
    chunks: I,
) -> Result<MutationReceipt, tonic::Status>
where
    I: IntoIterator<Item = Vec<u8>>,
    I::IntoIter: Send + 'static,
{
    let token = client.start_put(header).await?.into_inner();
    let mut chunks = chunks.into_iter().peekable();
    if chunks.peek().is_none() {
        let ready = client
            .put(tokio_stream::iter([PutRequest {
                token: Some(token),
                chunk: Vec::new(),
            }]))
            .await
            .map(tonic::Response::into_inner)?;
        return client.put_end(ready).await.map(tonic::Response::into_inner);
    }
    let requests = chunks.map(move |chunk| PutRequest {
        token: Some(token.clone()),
        chunk,
    });
    let ready = client
        .put(tokio_stream::iter(requests))
        .await
        .map(tonic::Response::into_inner)?;
    client.put_end(ready).await.map(tonic::Response::into_inner)
}

#[cfg(test)]
mod tests {
    use tonic::transport::Endpoint;

    use super::v1::{
        CreateBucketRequest, DeleteIfVersionRequest, DeleteVersionRequest, DeleteVersionResponse,
        Durability, ObjectVersioning, PutHeader,
    };
    use super::{
        MAX_MESSAGE_BYTES, RawAdministrationClient, RawAuthzClient, RawClient, RawPersonalDbClient,
        administration_client, authz_client, object_client, personaldb_client,
    };

    #[test]
    fn versioning_defaults_to_unversioned_and_delete_operations_stay_distinct() {
        assert_eq!(
            CreateBucketRequest::default().versioning,
            ObjectVersioning::Unversioned as i32
        );
        let conditional_current_delete = DeleteIfVersionRequest {
            expected_version: 9,
            ..Default::default()
        };
        let retained_delete = DeleteVersionRequest {
            version: 8,
            ..Default::default()
        };
        assert_eq!(conditional_current_delete.expected_version, 9);
        assert_eq!(retained_delete.version, 8);
        assert_eq!(
            DeleteVersionResponse {
                deleted: true,
                replacement_tombstone_version: Some(10),
            }
            .replacement_tombstone_version,
            Some(10)
        );
    }

    #[test]
    fn replicated_durability_is_preserved_for_the_server_to_reject() {
        let header = PutHeader {
            durability: Durability::Replicated as i32,
            ..Default::default()
        };
        assert_eq!(header.durability, Durability::Replicated as i32);
        assert_ne!(header.durability, Durability::Local as i32);
    }

    #[tokio::test]
    async fn every_authenticated_service_constructor_uses_the_shared_transport_surface() {
        assert_eq!(MAX_MESSAGE_BYTES, 72 * 1024 * 1024);
        let channel = Endpoint::from_static("http://127.0.0.1:50051").connect_lazy();
        let _: RawClient = object_client(channel.clone(), "token").unwrap();
        let _: RawAuthzClient = authz_client(channel.clone(), "token").unwrap();
        let _: RawAdministrationClient = administration_client(channel.clone(), "token").unwrap();
        let _: RawPersonalDbClient = personaldb_client(channel, "token").unwrap();
    }
}
