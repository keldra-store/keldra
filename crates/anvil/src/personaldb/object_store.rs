use anvil_api::v1::bulk_operation::Operation;
use anvil_api::v1::bulk_outcome::Outcome;
use anvil_api::v1::object_chunk::Value as ChunkValue;
use anvil_api::v1::object_head::State as HeadState;
use anvil_api::v1::object_service_server::ObjectService;
use anvil_api::v1::{
    BulkOperation, BulkPutIfVersionRequest, BulkPutRequest, BulkWriteRequest, Durability,
    GetObjectRequest, HeadObjectRequest, MutationFailureCode, ObjectAddress,
};
use personaldb_server_core::{ObjectStore, ObjectStoreError};
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::distributed_list::DistributedObjectLister;
use crate::object_path_access;
use crate::v05::ObjectServiceImpl;

use super::scope::{PersonalDbScopes, PersonalDbStorageScope};

const STORAGE_PREFIX: &str = "_anvil/personaldb/v0/";
const LIST_PAGE_SIZE: usize = anvil_store::MAX_LIST_OBJECTS;

pub(super) struct VersionedObject {
    pub(super) version: Option<u64>,
    pub(super) bytes: Option<Vec<u8>>,
}

/// PersonalDB's canonical object-store trait backed only by ordinary Anvil
/// objects. Payload size determines inline versus erasure-coded storage in the
/// normal object pipeline; this adapter has no persistence of its own.
#[derive(Clone)]
pub(crate) struct AnvilPersonalDbObjectStore {
    objects: ObjectServiceImpl,
    lister: DistributedObjectLister,
    scopes: PersonalDbScopes,
}

impl AnvilPersonalDbObjectStore {
    pub(crate) fn new(
        objects: ObjectServiceImpl,
        lister: DistributedObjectLister,
        scopes: PersonalDbScopes,
    ) -> Self {
        Self {
            objects,
            lister,
            scopes,
        }
    }

    fn address(
        scope: &PersonalDbStorageScope,
        logical_key: &str,
    ) -> Result<ObjectAddress, ObjectStoreError> {
        let path = storage_path(logical_key)?;
        anvil_store::ObjectKey::new(&scope.tenant, &scope.bucket, &path)
            .map_err(|error| unavailable(error.to_string()))?;
        Ok(ObjectAddress {
            tenant: scope.tenant.clone(),
            bucket: scope.bucket.clone(),
            path,
        })
    }

    fn authenticated<T>(
        scope: &PersonalDbStorageScope,
        value: T,
    ) -> Result<Request<T>, ObjectStoreError> {
        let authorization = format!("Bearer {}", scope.bearer.signed_token())
            .parse::<MetadataValue<_>>()
            .map_err(|_| unavailable("PersonalDB bearer token is malformed"))?;
        let mut request = Request::new(value);
        request
            .metadata_mut()
            .insert("authorization", authorization);
        request.extensions_mut().insert(scope.caller.clone());
        object_path_access::mark_personaldb(&mut request);
        Ok(request)
    }

    async fn current_head(
        &self,
        scope: &PersonalDbStorageScope,
        address: ObjectAddress,
    ) -> Result<HeadState, ObjectStoreError> {
        let request = Self::authenticated(
            scope,
            HeadObjectRequest {
                address: Some(address),
            },
        )?;
        ObjectService::head_object(&self.objects, request)
            .await
            .map_err(status_error)?
            .into_inner()
            .state
            .ok_or_else(|| unavailable("Anvil returned an empty object head"))
    }

    pub(super) async fn read_versioned(
        &self,
        logical_key: &str,
    ) -> Result<VersionedObject, ObjectStoreError> {
        let (_, scope) = self.scopes.for_key(logical_key)?;
        let address = Self::address(&scope, logical_key)?;
        let request = Self::authenticated(
            &scope,
            GetObjectRequest {
                address: Some(address),
                version: None,
            },
        )?;
        let mut stream = ObjectService::get_object(&self.objects, request)
            .await
            .map_err(status_error)?
            .into_inner();
        let first = stream
            .next()
            .await
            .transpose()
            .map_err(status_error)?
            .ok_or_else(|| unavailable("Anvil returned an empty object stream"))?;
        let (version, present) = match first.value {
            Some(ChunkValue::Head(head)) => match head.state {
                Some(HeadState::Present(head)) => (Some(head.version), true),
                Some(HeadState::Deleted(head)) => (Some(head.version), false),
                Some(HeadState::NeverExisted(_)) => (None, false),
                None => return Err(unavailable("Anvil returned an empty object head")),
            },
            _ => return Err(unavailable("Anvil object stream did not begin with a head")),
        };
        if !present {
            return Ok(VersionedObject {
                version,
                bytes: None,
            });
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            match chunk.map_err(status_error)?.value {
                Some(ChunkValue::Bytes(chunk)) => bytes.extend_from_slice(&chunk),
                _ => {
                    return Err(unavailable(
                        "Anvil object stream contained an invalid frame",
                    ));
                }
            }
        }
        Ok(VersionedObject {
            version,
            bytes: Some(bytes),
        })
    }

    pub(super) async fn put_at_version(
        &self,
        logical_key: &str,
        value: Vec<u8>,
        expected_version: Option<u64>,
    ) -> Result<bool, ObjectStoreError> {
        let (_, scope) = self.scopes.for_key(logical_key)?;
        let address = Self::address(&scope, logical_key)?;
        let operation = match expected_version {
            Some(version) => Operation::PutIfVersion(put_if_version_request(
                address,
                value.clone(),
                version,
                "pending",
            )),
            None => Operation::PutIfAbsent(put_request(address, value.clone(), "pending")),
        };
        let operation = with_command_id(
            operation,
            command_id(
                logical_key,
                &value,
                expected_version,
                expected_version.is_none(),
            ),
        );
        let request = Self::authenticated(
            &scope,
            BulkWriteRequest {
                operations: vec![BulkOperation {
                    operation: Some(operation),
                }],
            },
        )?;
        let response = ObjectService::bulk_write(&self.objects, request)
            .await
            .map_err(status_error)?
            .into_inner();
        let outcome = response
            .outcomes
            .into_iter()
            .next()
            .and_then(|outcome| outcome.outcome)
            .ok_or_else(|| unavailable("Anvil returned no PersonalDB write outcome"))?;
        match outcome {
            Outcome::Receipt(_) => Ok(true),
            Outcome::Failure(failure)
                if failure.code == MutationFailureCode::ConditionFailed as i32 =>
            {
                Ok(false)
            }
            Outcome::Failure(failure) => Err(unavailable(failure.message)),
        }
    }

    async fn write(
        &self,
        logical_key: &str,
        value: Vec<u8>,
        absent_only: bool,
    ) -> Result<(), ObjectStoreError> {
        let (_, scope) = self.scopes.for_key(logical_key)?;
        let address = Self::address(&scope, logical_key)?;
        let (operation, command_version) = if absent_only {
            (
                Operation::PutIfAbsent(put_request(address, value.clone(), "pending")),
                None,
            )
        } else {
            match self.current_head(&scope, address.clone()).await? {
                HeadState::NeverExisted(_) => (
                    Operation::PutIfAbsent(put_request(address, value.clone(), "pending")),
                    None,
                ),
                HeadState::Present(head) => (
                    Operation::PutIfVersion(put_if_version_request(
                        address,
                        value.clone(),
                        head.version,
                        "pending",
                    )),
                    Some(head.version),
                ),
                HeadState::Deleted(head) => (
                    Operation::PutIfVersion(put_if_version_request(
                        address,
                        value.clone(),
                        head.version,
                        "pending",
                    )),
                    Some(head.version),
                ),
            }
        };
        let command_id = command_id(logical_key, &value, command_version, absent_only);
        let operation = with_command_id(operation, command_id);
        let request = Self::authenticated(
            &scope,
            BulkWriteRequest {
                operations: vec![BulkOperation {
                    operation: Some(operation),
                }],
            },
        )?;
        let response = ObjectService::bulk_write(&self.objects, request)
            .await
            .map_err(status_error)?
            .into_inner();
        let outcome = response
            .outcomes
            .into_iter()
            .next()
            .and_then(|outcome| outcome.outcome)
            .ok_or_else(|| unavailable("Anvil returned no PersonalDB write outcome"))?;
        match outcome {
            Outcome::Receipt(_) => Ok(()),
            Outcome::Failure(failure)
                if absent_only && failure.code == MutationFailureCode::ConditionFailed as i32 =>
            {
                Err(ObjectStoreError::AlreadyExists(logical_key.to_owned()))
            }
            Outcome::Failure(failure) => Err(unavailable(failure.message)),
        }
    }
}

#[tonic::async_trait]
impl ObjectStore for AnvilPersonalDbObjectStore {
    async fn put_if_absent(&self, key: &str, value: Vec<u8>) -> Result<(), ObjectStoreError> {
        self.write(key, value, true).await
    }

    async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), ObjectStoreError> {
        if super::monotonic_head::is_committed_head_key(key) {
            return self.put_committed_head(key, value).await;
        }
        self.write(key, value, false).await
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError> {
        self.read_versioned(key)
            .await?
            .bytes
            .ok_or_else(|| ObjectStoreError::NotFound(key.to_owned()))
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let (_, scope) = self.scopes.for_key(prefix)?;
        let storage_prefix = storage_path(prefix)?;
        let mut cursor = None;
        let mut keys = Vec::new();
        loop {
            let page = self
                .lister
                .list_personaldb_objects(
                    scope.bearer.clone(),
                    &scope.tenant,
                    &scope.bucket,
                    scope.tenant_id,
                    scope.bucket_id,
                    &storage_prefix,
                    cursor.as_deref(),
                    LIST_PAGE_SIZE,
                )
                .await
                .map_err(status_error)?;
            for path in &page.paths {
                keys.push(logical_key(path)?);
            }
            if !page.has_more {
                return Ok(keys);
            }
            cursor = page.paths.last().cloned();
            if cursor.is_none() {
                return Err(unavailable("Anvil returned an empty continuation page"));
            }
        }
    }
}

fn put_request(address: ObjectAddress, bytes: Vec<u8>, command_id: &str) -> BulkPutRequest {
    BulkPutRequest {
        address: Some(address),
        bytes,
        content_type: String::new(),
        command_id: command_id.to_owned(),
        durability: Durability::Replicated as i32,
    }
}

fn put_if_version_request(
    address: ObjectAddress,
    bytes: Vec<u8>,
    expected_version: u64,
    command_id: &str,
) -> BulkPutIfVersionRequest {
    BulkPutIfVersionRequest {
        address: Some(address),
        bytes,
        content_type: String::new(),
        command_id: command_id.to_owned(),
        durability: Durability::Replicated as i32,
        expected_version,
    }
}

fn with_command_id(mut operation: Operation, command_id: String) -> Operation {
    match &mut operation {
        Operation::Put(request)
        | Operation::PutIfAbsent(request)
        | Operation::PutImmutable(request) => request.command_id = command_id,
        Operation::PutIfVersion(request) => request.command_id = command_id,
        Operation::Delete(_) | Operation::DeleteIfVersion(_) => unreachable!("put operation"),
    }
    operation
}

fn command_id(key: &str, value: &[u8], version: Option<u64>, absent_only: bool) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(key.as_bytes());
    hash.update(value);
    hash.update(&version.unwrap_or_default().to_be_bytes());
    hash.update(&[u8::from(absent_only)]);
    format!("personaldb-{}", hash.finalize().to_hex())
}

fn storage_path(logical_key: &str) -> Result<String, ObjectStoreError> {
    if logical_key.is_empty() {
        return Err(unavailable("PersonalDB object key is empty"));
    }
    Ok(format!("{STORAGE_PREFIX}{}", hex::encode(logical_key)))
}

fn logical_key(storage_path: &str) -> Result<String, ObjectStoreError> {
    let encoded = storage_path
        .strip_prefix(STORAGE_PREFIX)
        .ok_or_else(|| unavailable("PersonalDB listing returned another reserved namespace"))?;
    let bytes = hex::decode(encoded)
        .map_err(|_| unavailable("PersonalDB listing returned a malformed object key"))?;
    String::from_utf8(bytes)
        .map_err(|_| unavailable("PersonalDB listing returned a non-UTF-8 object key"))
}

fn status_error(error: Status) -> ObjectStoreError {
    match error.code() {
        tonic::Code::NotFound => ObjectStoreError::NotFound(error.message().to_owned()),
        tonic::Code::AlreadyExists => ObjectStoreError::AlreadyExists(error.message().to_owned()),
        _ => unavailable(error.message()),
    }
}

fn unavailable(message: impl Into<String>) -> ObjectStoreError {
    ObjectStoreError::Unavailable(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_paths_preserve_prefixes_and_round_trip() {
        let prefix = storage_path("groups/db/log/").unwrap();
        let key = storage_path("groups/db/log/entries/0001.json").unwrap();
        assert!(key.starts_with(&prefix));
        assert_eq!(
            logical_key(&key).unwrap(),
            "groups/db/log/entries/0001.json"
        );
    }

    #[test]
    fn command_identity_includes_the_expected_head() {
        let first = command_id("key", b"value", Some(1), false);
        let second = command_id("key", b"value", Some(2), false);
        assert_ne!(first, second);
    }
}
