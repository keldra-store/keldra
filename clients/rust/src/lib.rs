//! Thin Rust transport for Anvil 0.5.
//!
//! Domain-specific retry loops deliberately do not live here. Callers send
//! one-path CAS operations, independent bulk operations, or invoke a
//! pinned atomic program.

use anvil_api::v1::object_service_client::ObjectServiceClient;
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

pub async fn connect(
    endpoint: impl AsRef<str>,
    token: &str,
) -> Result<RawClient, Box<dyn std::error::Error + Send + Sync>> {
    let channel = Endpoint::from_shared(endpoint.as_ref().to_owned())?
        .connect()
        .await?;
    Ok(
        ObjectServiceClient::with_interceptor(channel, BearerToken::new(token)?)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
            .max_decoding_message_size(MAX_MESSAGE_BYTES),
    )
}
