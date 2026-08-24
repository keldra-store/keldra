use std::path::Path;

use axum::body::Body;
use keldra_api::v1::object_chunk::Value as ChunkValue;
use keldra_api::v1::object_head::State as HeadState;
use keldra_api::v1::{Durability, PresentObject};
use keldra_store::ObjectKey;
use tokio::io::AsyncReadExt as _;
use tokio_stream::StreamExt as _;

use super::model::{self, GitCheckpoint, GitCurrent, GitPushBatch, RepositoryName};
use super::{GitError, GitGatewayState, Target};
use crate::v05::{GatewayIdentity, GatewayPutMode};

const NAME_CONTEXT: &str = "keldra.git/repository-name/v2";

#[derive(Clone, Debug)]
pub(super) struct RepositoryLocation {
    pub(super) repository_id: String,
    tenant: String,
    bucket: String,
}

#[derive(Clone, Debug)]
pub(super) struct StoredCurrent {
    pub(super) object_version: u64,
    pub(super) value: GitCurrent,
}

#[derive(Clone)]
pub(super) struct GitStorage {
    state: GitGatewayState,
    identity: GatewayIdentity,
    location: RepositoryLocation,
}

impl RepositoryLocation {
    pub(super) async fn resolve(
        state: &GitGatewayState,
        identity: &GatewayIdentity,
        target: &Target,
        create: bool,
    ) -> Result<Option<Self>, GitError> {
        let name_key = name_key(target)?;
        if let Some(bytes) = read_optional_bytes(state, identity, &name_key).await? {
            let name = RepositoryName::decode(&bytes)?;
            return Ok(Some(Self {
                repository_id: name.repository_id,
                tenant: target.tenant.clone(),
                bucket: target.bucket.clone(),
            }));
        }
        if !create {
            return Ok(None);
        }

        let candidate = RepositoryName {
            format_version: model::FORMAT_VERSION,
            repository_id: uuid::Uuid::new_v4().to_string(),
        };
        let encoded = candidate.encode()?;
        let durability = artifact_durability(state)?;
        match state
            .objects
            .git_put(
                identity,
                &name_key,
                "application/vnd.keldra.git-repository-name+json",
                command_id("git-name"),
                GatewayPutMode::IfAbsent,
                durability,
                Body::from(encoded),
            )
            .await
        {
            Ok(_) => Ok(Some(Self {
                repository_id: candidate.repository_id,
                tenant: target.tenant.clone(),
                bucket: target.bucket.clone(),
            })),
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::AlreadyExists
                        | tonic::Code::Aborted
                        | tonic::Code::FailedPrecondition
                ) =>
            {
                let bytes = read_required_bytes(state, identity, &name_key).await?;
                let winner = RepositoryName::decode(&bytes)?;
                Ok(Some(Self {
                    repository_id: winner.repository_id,
                    tenant: target.tenant.clone(),
                    bucket: target.bucket.clone(),
                }))
            }
            Err(error) => Err(GitError::from_status(error)),
        }
    }

    fn key(&self, suffix: &str) -> Result<ObjectKey, GitError> {
        ObjectKey::new(
            self.tenant.clone(),
            self.bucket.clone(),
            format!("_keldra/git/v2/repos/{}/{suffix}", self.repository_id),
        )
        .map_err(|error| GitError::bad_request(error.to_string()))
    }

    pub(super) fn cache_key(&self) -> &str {
        &self.repository_id
    }
}

impl GitStorage {
    pub(super) fn new(
        state: &GitGatewayState,
        identity: &GatewayIdentity,
        location: RepositoryLocation,
    ) -> Self {
        Self {
            state: state.clone(),
            identity: identity.clone(),
            location,
        }
    }

    pub(super) fn location(&self) -> &RepositoryLocation {
        &self.location
    }

    pub(super) fn authenticated_principal(&self) -> Result<String, GitError> {
        self.identity
            .caller()
            .ok_or_else(|| GitError::unauthorized("Git push requires credentials"))?
            .authenticated_app_id()
            .map(str::to_owned)
            .map_err(|_| GitError::internal("Git caller is not an application"))
    }

    pub(super) async fn current(&self) -> Result<Option<StoredCurrent>, GitError> {
        let key = self.location.key("current")?;
        let Some((head, bytes)) = read_optional_object(&self.state, &self.identity, &key).await?
        else {
            return Ok(None);
        };
        let value = model::decode_current(&bytes, &self.location.repository_id)?;
        Ok(Some(StoredCurrent {
            object_version: head.version,
            value,
        }))
    }

    pub(super) async fn checkpoint(&self, id: &str) -> Result<GitCheckpoint, GitError> {
        let key = self.location.key(&format!("checkpoints/{id}"))?;
        let bytes = read_required_bytes(&self.state, &self.identity, &key).await?;
        model::decode_checkpoint(id, &bytes, &self.location.repository_id)
    }

    pub(super) async fn batch(&self, id: &str) -> Result<GitPushBatch, GitError> {
        let key = self.location.key(&format!("batches/{id}"))?;
        let bytes = read_required_bytes(&self.state, &self.identity, &key).await?;
        model::decode_batch(id, &bytes, &self.location.repository_id)
    }

    pub(super) async fn put_checkpoint(&self, value: &GitCheckpoint) -> Result<String, GitError> {
        let (id, bytes) = value.encode()?;
        self.put_identified_bytes(
            &format!("checkpoints/{id}"),
            &id,
            "application/vnd.keldra.git-checkpoint+json",
            bytes,
        )
        .await?;
        Ok(id)
    }

    pub(super) async fn put_batch(&self, value: &GitPushBatch) -> Result<String, GitError> {
        let (id, bytes) = value.encode()?;
        self.put_identified_bytes(
            &format!("batches/{id}"),
            &id,
            "application/vnd.keldra.git-push-batch+json",
            bytes,
        )
        .await?;
        Ok(id)
    }

    pub(super) async fn put_pack(&self, path: &Path) -> Result<String, GitError> {
        let id = hash_file(path).await?;
        let key = self.location.key(&format!("packs/{id}"))?;
        let body = file_body(path.to_owned()).await?;
        match self
            .state
            .objects
            .git_put(
                &self.identity,
                &key,
                "application/x-git-packed-objects",
                command_id("git-pack"),
                GatewayPutMode::IfAbsent,
                artifact_durability(&self.state)?,
                body,
            )
            .await
        {
            Ok(_) => Ok(id),
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::AlreadyExists
                        | tonic::Code::Aborted
                        | tonic::Code::FailedPrecondition
                ) =>
            {
                require_existing_identity(&self.state, &self.identity, &key, &id).await?;
                Ok(id)
            }
            Err(error) => Err(GitError::from_status(error)),
        }
    }

    pub(super) async fn stream_pack(&self, id: &str, destination: &Path) -> Result<(), GitError> {
        let key = self.location.key(&format!("packs/{id}"))?;
        stream_object_to_file(&self.state, &self.identity, &key, destination, id).await
    }

    pub(super) async fn publish_current(
        &self,
        expected_version: Option<u64>,
        value: &GitCurrent,
    ) -> Result<u64, GitError> {
        let key = self.location.key("current")?;
        let mode = expected_version
            .map(GatewayPutMode::IfVersion)
            .unwrap_or(GatewayPutMode::IfAbsent);
        let result = self
            .state
            .objects
            .git_put(
                &self.identity,
                &key,
                "application/vnd.keldra.git-current+json",
                command_id("git-current"),
                mode,
                artifact_durability(&self.state)?,
                Body::from(model::encode_current(value)?),
            )
            .await
            .map_err(GitError::from_status)?;
        Ok(result.receipt.version)
    }

    async fn put_identified_bytes(
        &self,
        suffix: &str,
        identity: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> Result<(), GitError> {
        let key = self.location.key(suffix)?;
        match self
            .state
            .objects
            .git_put(
                &self.identity,
                &key,
                content_type,
                command_id("git-artifact"),
                GatewayPutMode::IfAbsent,
                artifact_durability(&self.state)?,
                Body::from(bytes),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::AlreadyExists
                        | tonic::Code::Aborted
                        | tonic::Code::FailedPrecondition
                ) =>
            {
                require_existing_identity(&self.state, &self.identity, &key, identity).await
            }
            Err(error) => Err(GitError::from_status(error)),
        }
    }
}

pub(super) fn name_key(target: &Target) -> Result<ObjectKey, GitError> {
    let mut hasher = blake3::Hasher::new_derive_key(NAME_CONTEXT);
    hasher.update(&(target.repository.len() as u64).to_be_bytes());
    hasher.update(target.repository.as_bytes());
    ObjectKey::new(
        target.tenant.clone(),
        target.bucket.clone(),
        format!("_keldra/git/v2/names/{}", hasher.finalize().to_hex()),
    )
    .map_err(|error| GitError::bad_request(error.to_string()))
}

fn artifact_durability(state: &GitGatewayState) -> Result<Durability, GitError> {
    state
        .control
        .active_node_count()
        .map(|count| {
            if count > 1 {
                Durability::Replicated
            } else {
                Durability::Local
            }
        })
        .map_err(GitError::from_status)
}

fn command_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

async fn read_optional_bytes(
    state: &GitGatewayState,
    identity: &GatewayIdentity,
    key: &ObjectKey,
) -> Result<Option<Vec<u8>>, GitError> {
    read_optional_object(state, identity, key)
        .await
        .map(|value| value.map(|(_, bytes)| bytes))
}

async fn read_required_bytes(
    state: &GitGatewayState,
    identity: &GatewayIdentity,
    key: &ObjectKey,
) -> Result<Vec<u8>, GitError> {
    read_optional_bytes(state, identity, key)
        .await?
        .ok_or_else(|| GitError::internal("required Git artifact is missing"))
}

async fn read_optional_object(
    state: &GitGatewayState,
    identity: &GatewayIdentity,
    key: &ObjectKey,
) -> Result<Option<(PresentObject, Vec<u8>)>, GitError> {
    let head = state
        .objects
        .git_head(identity, key)
        .await
        .map_err(GitError::from_status)?;
    match head.state {
        Some(HeadState::Present(_)) => {}
        Some(HeadState::Deleted(_)) | Some(HeadState::NeverExisted(_)) | None => return Ok(None),
    }
    let mut stream = state
        .objects
        .git_get(identity, key)
        .await
        .map_err(GitError::from_status)?;
    let mut present = None;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk.map_err(GitError::from_status)?.value {
            Some(ChunkValue::Head(head)) if present.is_none() => match head.state {
                Some(HeadState::Present(value)) => present = Some(value),
                _ => return Ok(None),
            },
            Some(ChunkValue::Bytes(chunk)) if present.is_some() => {
                bytes
                    .try_reserve(chunk.len())
                    .map_err(|_| GitError::internal("Git control object cannot fit in memory"))?;
                bytes.extend_from_slice(&chunk);
            }
            _ => return Err(GitError::internal("Git object stream is malformed")),
        }
    }
    Ok(Some((
        present.ok_or_else(|| GitError::internal("Git object stream has no head"))?,
        bytes,
    )))
}

async fn require_existing_identity(
    state: &GitGatewayState,
    identity: &GatewayIdentity,
    key: &ObjectKey,
    expected: &str,
) -> Result<(), GitError> {
    let head = state
        .objects
        .git_head(identity, key)
        .await
        .map_err(GitError::from_status)?;
    let Some(HeadState::Present(present)) = head.state else {
        return Err(GitError::conflict(
            "Git immutable artifact disappeared during replay",
        ));
    };
    if hex::encode(present.content_hash) != expected {
        return Err(GitError::conflict(
            "Git immutable artifact identity is already bound to different bytes",
        ));
    }
    Ok(())
}

async fn hash_file(path: &Path) -> Result<String, GitError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| GitError::internal(format!("open Git pack: {error}")))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|error| GitError::internal(format!("hash Git pack: {error}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

async fn file_body(path: std::path::PathBuf) -> Result<Body, GitError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| GitError::internal(format!("open Git pack: {error}")))?;
    let stream = async_stream::try_stream! {
        Ok::<(), std::io::Error>(())?;
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            yield bytes::Bytes::copy_from_slice(&buffer[..read]);
        }
    };
    Ok(Body::from_stream(stream))
}

async fn stream_object_to_file(
    state: &GitGatewayState,
    identity: &GatewayIdentity,
    key: &ObjectKey,
    destination: &Path,
    expected_identity: &str,
) -> Result<(), GitError> {
    let mut stream = state
        .objects
        .git_get(identity, key)
        .await
        .map_err(GitError::from_status)?;
    let temporary = destination.with_extension("pack.tmp");
    let mut file = tokio::fs::File::create(&temporary)
        .await
        .map_err(|error| GitError::internal(format!("create Git pack cache: {error}")))?;
    let mut hasher = blake3::Hasher::new();
    let mut saw_head = false;
    while let Some(chunk) = stream.next().await {
        match chunk.map_err(GitError::from_status)?.value {
            Some(ChunkValue::Head(_)) if !saw_head => saw_head = true,
            Some(ChunkValue::Bytes(bytes)) if saw_head => {
                hasher.update(&bytes);
                tokio::io::AsyncWriteExt::write_all(&mut file, &bytes)
                    .await
                    .map_err(|error| {
                        GitError::internal(format!("write Git pack cache: {error}"))
                    })?;
            }
            _ => return Err(GitError::internal("Git pack object stream is malformed")),
        }
    }
    tokio::io::AsyncWriteExt::flush(&mut file)
        .await
        .map_err(|error| GitError::internal(format!("flush Git pack cache: {error}")))?;
    if !saw_head || hasher.finalize().to_hex().as_str() != expected_identity {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(GitError::internal("Git pack content identity is invalid"));
    }
    tokio::fs::rename(&temporary, destination)
        .await
        .map_err(|error| GitError::internal(format!("publish Git pack cache: {error}")))
}
