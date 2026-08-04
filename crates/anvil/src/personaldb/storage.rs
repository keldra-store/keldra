use anvil_api::v1::bulk_operation::Operation;
use anvil_api::v1::bulk_outcome::Outcome;
use anvil_api::v1::object_chunk::Value as ChunkValue;
use anvil_api::v1::object_head::State as HeadState;
use anvil_api::v1::object_service_server::ObjectService;
use anvil_api::v1::{
    BulkOperation, BulkPutIfVersionRequest, BulkPutRequest, BulkWriteRequest, Durability,
    GetObjectRequest, ObjectAddress,
};
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::object_path_access;
use crate::v05::ObjectServiceImpl;

use super::model::GroupScope;

#[derive(Clone, Debug)]
pub(super) struct VersionedBytes {
    pub(super) version: Option<u64>,
    pub(super) bytes: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConditionalWrite {
    Applied,
    ConditionFailed,
}

#[derive(Clone)]
pub(crate) struct PersonalDbObjects {
    service: ObjectServiceImpl,
}

impl PersonalDbObjects {
    pub(crate) fn new(service: ObjectServiceImpl) -> Self {
        Self { service }
    }

    pub(super) async fn read(
        &self,
        scope: &GroupScope,
        suffix: &str,
    ) -> Result<VersionedBytes, Status> {
        let key = scope.storage_key(suffix)?;
        let mut stream = ObjectService::get_object(
            &self.service,
            authenticated(
                scope,
                GetObjectRequest {
                    address: Some(ObjectAddress {
                        tenant: key.tenant().to_owned(),
                        bucket: key.bucket().to_owned(),
                        path: key.path().to_owned(),
                    }),
                    version: None,
                },
            )?,
        )
        .await?
        .into_inner();
        let first = stream
            .next()
            .await
            .transpose()?
            .ok_or_else(|| Status::data_loss("PersonalDB object stream is empty"))?;
        let (version, present) = match first.value {
            Some(ChunkValue::Head(head)) => match head.state {
                Some(HeadState::Present(head)) => (Some(head.version), true),
                Some(HeadState::Deleted(head)) => (Some(head.version), false),
                Some(HeadState::NeverExisted(_)) => (None, false),
                None => return Err(Status::data_loss("PersonalDB object head is empty")),
            },
            _ => {
                return Err(Status::data_loss(
                    "PersonalDB object stream does not begin with a head",
                ));
            }
        };
        if !present {
            return Ok(VersionedBytes {
                version,
                bytes: None,
            });
        }
        let mut bytes = Vec::new();
        while let Some(frame) = stream.next().await {
            match frame?.value {
                Some(ChunkValue::Bytes(chunk)) => bytes.extend_from_slice(&chunk),
                _ => {
                    return Err(Status::data_loss(
                        "PersonalDB object stream contains a non-byte frame",
                    ));
                }
            }
        }
        Ok(VersionedBytes {
            version,
            bytes: Some(bytes),
        })
    }

    pub(super) async fn put_if_absent(
        &self,
        scope: &GroupScope,
        suffix: &str,
        bytes: Vec<u8>,
        command_id: String,
    ) -> Result<ConditionalWrite, Status> {
        let address = address(scope, suffix)?;
        let durability = personaldb_write_durability(self.service.is_single_node()?);
        self.write(
            scope,
            Operation::PutIfAbsent(BulkPutRequest {
                address: Some(address),
                bytes,
                content_type: "application/octet-stream".into(),
                command_id,
                durability: durability as i32,
            }),
        )
        .await
    }

    pub(super) async fn put_if_version(
        &self,
        scope: &GroupScope,
        suffix: &str,
        bytes: Vec<u8>,
        expected_version: u64,
        command_id: String,
    ) -> Result<ConditionalWrite, Status> {
        let address = address(scope, suffix)?;
        let durability = personaldb_write_durability(self.service.is_single_node()?);
        self.write(
            scope,
            Operation::PutIfVersion(BulkPutIfVersionRequest {
                address: Some(address),
                bytes,
                content_type: "application/octet-stream".into(),
                command_id,
                durability: durability as i32,
                expected_version,
            }),
        )
        .await
    }

    async fn write(
        &self,
        scope: &GroupScope,
        operation: Operation,
    ) -> Result<ConditionalWrite, Status> {
        let response = ObjectService::bulk_write(
            &self.service,
            authenticated(
                scope,
                BulkWriteRequest {
                    operations: vec![BulkOperation {
                        operation: Some(operation),
                    }],
                },
            )?,
        )
        .await?
        .into_inner();
        match response
            .outcomes
            .into_iter()
            .next()
            .and_then(|outcome| outcome.outcome)
            .ok_or_else(|| Status::data_loss("PersonalDB write returned no outcome"))?
        {
            Outcome::Receipt(_) => Ok(ConditionalWrite::Applied),
            Outcome::Failure(failure)
                if failure.code == anvil_api::v1::MutationFailureCode::ConditionFailed as i32 =>
            {
                Ok(ConditionalWrite::ConditionFailed)
            }
            Outcome::Failure(failure) => Err(Status::failed_precondition(failure.message)),
        }
    }
}

fn personaldb_write_durability(single_node: bool) -> Durability {
    if single_node {
        Durability::Local
    } else {
        Durability::Replicated
    }
}

fn address(scope: &GroupScope, suffix: &str) -> Result<ObjectAddress, Status> {
    let key = scope.storage_key(suffix)?;
    Ok(ObjectAddress {
        tenant: key.tenant().to_owned(),
        bucket: key.bucket().to_owned(),
        path: key.path().to_owned(),
    })
}

fn authenticated<T>(scope: &GroupScope, value: T) -> Result<Request<T>, Status> {
    let authorization = format!("Bearer {}", scope.bearer.signed_token())
        .parse::<MetadataValue<_>>()
        .map_err(|_| Status::unauthenticated("PersonalDB bearer token is malformed"))?;
    let mut request = Request::new(value);
    request
        .metadata_mut()
        .insert("authorization", authorization);
    request.extensions_mut().insert(scope.caller.clone());
    object_path_access::mark_personaldb(&mut request);
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::{Durability, personaldb_write_durability};

    #[test]
    fn personaldb_internal_writes_use_only_satisfiable_topology_durability() {
        assert_eq!(personaldb_write_durability(true), Durability::Local);
        assert_eq!(personaldb_write_durability(false), Durability::Replicated);
    }
}
