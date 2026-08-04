use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anvil_api::v1::object_chunk::Value as ChunkValue;
use anvil_api::v1::object_head::State as HeadState;
use anvil_store::ObjectKey;
use axum::body::Body;
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;
use tokio_stream::StreamExt as _;

use super::{GitError, GitGatewayState, Target};
use crate::v05::{GatewayIdentity, GatewayPutMode};

pub(super) struct MaterializedRepository {
    pub(super) repository: PathBuf,
    expected_version: Option<u64>,
    bundle_path: PathBuf,
}

pub(super) async fn materialize(
    state: &GitGatewayState,
    identity: &GatewayIdentity,
    target: &Target,
    key: &ObjectKey,
) -> Result<MaterializedRepository, GitError> {
    let cache_id = blake3::hash(
        format!(
            "{}\0{}\0{}",
            target.tenant, target.bucket, target.repository
        )
        .as_bytes(),
    )
    .to_hex();
    let directory = state.cache_root.join(cache_id.as_str());
    remove_disposable(&directory).await?;
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(|error| GitError::internal(format!("create Git cache: {error}")))?;
    let repository = directory.join("repo.git");
    let bundle_path = directory.join("source.bundle");
    let head = state
        .objects
        .git_head(identity, key)
        .await
        .map_err(GitError::from_status)?;
    let expected_version = match head.state {
        Some(HeadState::Present(present)) => {
            stream_bundle(state, identity, key, &bundle_path).await?;
            git(
                Command::new("git")
                    .arg("clone")
                    .arg("--bare")
                    .arg("--no-hardlinks")
                    .arg(&bundle_path)
                    .arg(&repository),
                "materialize Git bundle",
            )
            .await?;
            Some(present.version)
        }
        Some(HeadState::Deleted(_)) | Some(HeadState::NeverExisted(_)) | None => {
            git(
                Command::new("git")
                    .arg("init")
                    .arg("--bare")
                    .arg(&repository),
                "initialize Git repository",
            )
            .await?;
            None
        }
    };
    git(
        Command::new("git")
            .arg("-C")
            .arg(&repository)
            .args(["config", "http.receivepack", "true"]),
        "enable authenticated Git receive-pack",
    )
    .await?;
    Ok(MaterializedRepository {
        repository,
        expected_version,
        bundle_path,
    })
}

pub(super) async fn refs(repository: &Path) -> Result<String, GitError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["for-each-ref", "--format=%(refname)%00%(objectname)"])
        .output()
        .await
        .map_err(|error| GitError::internal(format!("run git for-each-ref: {error}")))?;
    if !output.status.success() {
        return Err(GitError::internal(format!(
            "git for-each-ref failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| GitError::internal("git for-each-ref returned non-UTF-8 output"))
}

pub(super) async fn publish(
    state: &GitGatewayState,
    identity: &GatewayIdentity,
    target: &Target,
    key: &ObjectKey,
    materialized: &MaterializedRepository,
) -> Result<(), GitError> {
    let output = materialized.bundle_path.with_file_name("published.bundle");
    let _ = tokio::fs::remove_file(&output).await;
    git(
        Command::new("git")
            .arg("-C")
            .arg(&materialized.repository)
            .args(["bundle", "create"])
            .arg(&output)
            .arg("--all"),
        "create authoritative Git bundle",
    )
    .await?;
    let bytes = tokio::fs::read(&output)
        .await
        .map_err(|error| GitError::internal(format!("read Git bundle: {error}")))?;
    let mode = materialized
        .expected_version
        .map_or(GatewayPutMode::IfAbsent, GatewayPutMode::IfVersion);
    state
        .objects
        .git_put(
            identity,
            key,
            format!("git-push-{}-{}", target.repository, uuid::Uuid::new_v4()),
            mode,
            Body::from(bytes),
        )
        .await
        .map_err(|error| match error.code() {
            tonic::Code::Aborted | tonic::Code::FailedPrecondition | tonic::Code::AlreadyExists => {
                GitError::conflict(
                    "repository changed during push; fetch the current repository and retry",
                )
            }
            _ => GitError::from_status(error),
        })?;
    Ok(())
}

async fn stream_bundle(
    state: &GitGatewayState,
    identity: &GatewayIdentity,
    key: &ObjectKey,
    destination: &Path,
) -> Result<(), GitError> {
    let mut stream = state
        .objects
        .git_get(identity, key)
        .await
        .map_err(GitError::from_status)?;
    let mut file = tokio::fs::File::create(destination)
        .await
        .map_err(|error| GitError::internal(format!("create cached Git bundle: {error}")))?;
    let mut saw_head = false;
    while let Some(chunk) = stream.next().await {
        match chunk.map_err(GitError::from_status)?.value {
            Some(ChunkValue::Head(_)) if !saw_head => saw_head = true,
            Some(ChunkValue::Bytes(bytes)) if saw_head => file
                .write_all(&bytes)
                .await
                .map_err(|error| GitError::internal(format!("write cached Git bundle: {error}")))?,
            _ => return Err(GitError::internal("Git bundle object stream is malformed")),
        }
    }
    if !saw_head {
        return Err(GitError::internal("Git bundle object stream has no head"));
    }
    file.flush()
        .await
        .map_err(|error| GitError::internal(format!("flush cached Git bundle: {error}")))
}

async fn remove_disposable(path: &Path) -> Result<(), GitError> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(GitError::internal(format!(
            "reset disposable Git cache: {error}"
        ))),
    }
}

async fn git(command: &mut Command, action: &str) -> Result<(), GitError> {
    let output = command
        .output()
        .await
        .map_err(|error| GitError::internal(format!("{action}: {error}")))?;
    if output.status.success() {
        return Ok(());
    }
    Err(GitError::internal(format!(
        "{action}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}
