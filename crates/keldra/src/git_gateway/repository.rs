use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use super::model::{self, GitCheckpoint, GitCurrent, GitPush, GitPushBatch, GitReferenceCommand};
use super::storage::{GitStorage, StoredCurrent};
use super::{GitError, GitGatewayState};

const MARKER_FILE: &str = "keldra-generation.json";
const REPOSITORY_DIRECTORY: &str = "repo.git";
const COMPACTION_TAIL_BATCHES: u64 = 64;

pub(super) enum MaterializationGuard {
    Read {
        _guard: tokio::sync::OwnedRwLockReadGuard<()>,
    },
    Write {
        _guard: tokio::sync::OwnedRwLockWriteGuard<()>,
    },
}

pub(super) struct MaterializedRepository {
    pub(super) repository: PathBuf,
    pub(super) current: Option<StoredCurrent>,
    pub(super) before_refs: BTreeMap<String, String>,
    pub(super) before_packs: BTreeSet<PathBuf>,
    directory: PathBuf,
    _guard: MaterializationGuard,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MaterializationMarker {
    format_version: u16,
    repository_id: String,
    current_object_version: Option<u64>,
    generation: u64,
    checkpoint_id: Option<String>,
    tail_batch_id: Option<String>,
}

#[tracing::instrument(
    name = "keldra.git.materialize",
    skip(state, storage),
    fields(repository_id = %storage.location().repository_id, writable)
)]
pub(super) async fn materialize(
    state: &GitGatewayState,
    storage: &GitStorage,
    writable: bool,
) -> Result<MaterializedRepository, GitError> {
    let directory = state.cache_root.join(storage.location().cache_key());
    let repository = directory.join(REPOSITORY_DIRECTORY);
    let lock = state.repository_locks.get(storage.location().cache_key());

    let guard = if writable {
        let guard = lock.write_owned().await;
        ensure_current(storage, &directory, &repository).await?;
        MaterializationGuard::Write { _guard: guard }
    } else {
        let mut read = lock.clone().read_owned().await;
        let current = storage.current().await?;
        if !marker_matches(&directory, storage.location().cache_key(), current.as_ref()).await? {
            drop(read);
            let write = lock.clone().write_owned().await;
            ensure_current(storage, &directory, &repository).await?;
            drop(write);
            read = lock.read_owned().await;
        }
        MaterializationGuard::Read { _guard: read }
    };

    let current = storage.current().await?;
    if !marker_matches(&directory, storage.location().cache_key(), current.as_ref()).await? {
        return Err(GitError::conflict(
            "Git repository advanced while its materialization was acquired",
        ));
    }
    let before_refs = refs(&repository).await?;
    let before_packs = pack_paths(&repository).await?;
    Ok(MaterializedRepository {
        repository,
        current,
        before_refs,
        before_packs,
        directory,
        _guard: guard,
    })
}

#[tracing::instrument(
    name = "keldra.git.publish",
    skip(storage, materialized),
    fields(repository_id = %storage.location().repository_id)
)]
pub(super) async fn publish(
    storage: &GitStorage,
    materialized: &MaterializedRepository,
) -> Result<bool, GitError> {
    let after_refs = refs(&materialized.repository).await?;
    let commands = reference_commands(&materialized.before_refs, &after_refs);
    if commands.is_empty() {
        return Ok(false);
    }

    let after_packs = pack_paths(&materialized.repository).await?;
    let mut pack_ids = Vec::new();
    for path in after_packs.difference(&materialized.before_packs) {
        pack_ids.push(storage.put_pack(path).await?);
    }
    pack_ids.sort_unstable();
    pack_ids.dedup();

    let (checkpoint_id, parent_batch_id, tail_depth, generation, expected_version) =
        match &materialized.current {
            Some(current) => (
                current.value.checkpoint_id.clone(),
                current.value.tail_batch_id.clone(),
                current.value.tail_depth.saturating_add(1),
                current.value.generation.saturating_add(1),
                Some(current.object_version),
            ),
            None => {
                let refs = Vec::new();
                let checkpoint = GitCheckpoint {
                    format_version: model::FORMAT_VERSION,
                    repository_id: storage.location().repository_id.clone(),
                    source_generation: 0,
                    ref_state_hash: model::refs_hash(&refs),
                    refs,
                    pack_ids: Vec::new(),
                };
                (storage.put_checkpoint(&checkpoint).await?, None, 1, 1, None)
            }
        };

    let ref_state_hash = model::refs_hash(&model::references(&after_refs));
    let batch = GitPushBatch {
        format_version: model::FORMAT_VERSION,
        repository_id: storage.location().repository_id.clone(),
        parent_batch_id,
        base_checkpoint_id: checkpoint_id.clone(),
        first_generation: generation,
        pushes: vec![GitPush {
            operation_id: uuid::Uuid::new_v4().to_string(),
            authenticated_principal: storage.authenticated_principal()?,
            accepted_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| GitError::internal("system clock is before the Unix epoch"))?
                .as_millis()
                .try_into()
                .map_err(|_| GitError::internal("Git push timestamp exceeds u64"))?,
            pack_ids,
            reference_commands: commands,
        }],
        resulting_ref_state_hash: ref_state_hash.clone(),
    };
    let batch_id = storage.put_batch(&batch).await?;
    let current = GitCurrent {
        format_version: model::FORMAT_VERSION,
        repository_id: storage.location().repository_id.clone(),
        generation,
        checkpoint_id: checkpoint_id.clone(),
        tail_batch_id: Some(batch_id.clone()),
        tail_depth,
        ref_state_hash,
    };

    let current_object_version = match storage.publish_current(expected_version, &current).await {
        Ok(version) => version,
        Err(error) => {
            remove_marker(&materialized.directory).await;
            return Err(error);
        }
    };
    write_marker(
        &materialized.directory,
        &MaterializationMarker {
            format_version: model::FORMAT_VERSION,
            repository_id: storage.location().repository_id.clone(),
            current_object_version: Some(current_object_version),
            generation,
            checkpoint_id: Some(checkpoint_id),
            tail_batch_id: Some(batch_id),
        },
    )
    .await?;

    tracing::info!(
        monotonic_counter.keldra_git_pushes_published_total = 1_u64,
        monotonic_counter.keldra_git_pack_objects_published_total =
            batch.pushes[0].pack_ids.len() as u64,
        monotonic_counter.keldra_git_ref_commands_published_total =
            batch.pushes[0].reference_commands.len() as u64,
        histogram.keldra_git_tail_depth = tail_depth,
        "Git push generation published"
    );

    Ok(tail_depth >= COMPACTION_TAIL_BATCHES)
}

pub(super) async fn begin_push(materialized: &MaterializedRepository) {
    // receive-pack mutates the disposable bare repository before Keldra publishes
    // the corresponding immutable batch and current-pointer CAS. Removing the
    // marker first ensures every unsuccessful or conflicting push is rebuilt
    // from authoritative objects before this cache is used again.
    remove_marker(&materialized.directory).await;
}

pub(super) fn spawn_compaction(state: GitGatewayState, storage: GitStorage) {
    tokio::spawn(async move {
        if let Err(error) = compact_current(&state, &storage).await {
            tracing::warn!(
                error = %error.message,
                "Git repository compaction will be retried after a later push"
            );
        }
    });
}

async fn compact_current(state: &GitGatewayState, storage: &GitStorage) -> Result<(), GitError> {
    let materialized = materialize(state, storage, true).await?;
    let Some(current) = materialized.current.as_ref() else {
        return Ok(());
    };
    if current.value.tail_depth < COMPACTION_TAIL_BATCHES {
        return Ok(());
    }
    let result = compact(
        storage,
        &materialized,
        current.object_version,
        &current.value,
    )
    .await;
    tracing::info!(
        monotonic_counter.keldra_git_compaction_runs_total = 1_u64,
        monotonic_counter.keldra_git_compaction_failures_total = u64::from(result.is_err()),
        "Git repository compaction completed"
    );
    result
}

async fn ensure_current(
    storage: &GitStorage,
    directory: &Path,
    repository: &Path,
) -> Result<(), GitError> {
    let current = storage.current().await?;
    if marker_matches(directory, storage.location().cache_key(), current.as_ref()).await? {
        return Ok(());
    }
    let marker = read_marker(directory).await?;
    let caught_up = match (&marker, &current) {
        (Some(marker), Some(current))
            if marker.repository_id == storage.location().repository_id
                && marker.checkpoint_id.as_deref()
                    == Some(current.value.checkpoint_id.as_str()) =>
        {
            catch_up(storage, repository, marker, current).await?
        }
        _ => false,
    };
    if !caught_up {
        rebuild(storage, directory, repository, current.as_ref()).await?;
    }
    let marker = match current {
        Some(current) => MaterializationMarker {
            format_version: model::FORMAT_VERSION,
            repository_id: storage.location().repository_id.clone(),
            current_object_version: Some(current.object_version),
            generation: current.value.generation,
            checkpoint_id: Some(current.value.checkpoint_id),
            tail_batch_id: current.value.tail_batch_id,
        },
        None => MaterializationMarker {
            format_version: model::FORMAT_VERSION,
            repository_id: storage.location().repository_id.clone(),
            current_object_version: None,
            generation: 0,
            checkpoint_id: None,
            tail_batch_id: None,
        },
    };
    write_marker(directory, &marker).await
}

async fn catch_up(
    storage: &GitStorage,
    repository: &Path,
    marker: &MaterializationMarker,
    current: &StoredCurrent,
) -> Result<bool, GitError> {
    let mut id = current.value.tail_batch_id.clone();
    let mut reverse = Vec::new();
    let mut found = marker.tail_batch_id.is_none();
    for _ in 0..=current.value.tail_depth {
        let Some(batch_id) = id else {
            break;
        };
        if marker.tail_batch_id.as_deref() == Some(batch_id.as_str()) {
            found = true;
            break;
        }
        let batch = storage.batch(&batch_id).await?;
        if batch.base_checkpoint_id != current.value.checkpoint_id {
            return Ok(false);
        }
        id = batch.parent_batch_id.clone();
        reverse.push(batch);
    }
    if !found {
        return Ok(false);
    }
    for batch in reverse.into_iter().rev() {
        apply_batch(storage, repository, &batch).await?;
    }
    Ok(refs_hash(repository).await? == current.value.ref_state_hash)
}

#[tracing::instrument(
    name = "keldra.git.rebuild_materialization",
    skip(storage, directory, repository, current),
    fields(repository_id = %storage.location().repository_id)
)]
async fn rebuild(
    storage: &GitStorage,
    directory: &Path,
    repository: &Path,
    current: Option<&StoredCurrent>,
) -> Result<(), GitError> {
    remove_disposable(directory).await?;
    tokio::fs::create_dir_all(directory)
        .await
        .map_err(|error| GitError::internal(format!("create Git cache: {error}")))?;
    init(repository).await?;
    let Some(current) = current else {
        return Ok(());
    };
    let checkpoint = storage.checkpoint(&current.value.checkpoint_id).await?;
    for pack_id in &checkpoint.pack_ids {
        install_pack(storage, repository, pack_id).await?;
    }
    set_refs(repository, &checkpoint.refs).await?;

    let empty_marker = MaterializationMarker {
        format_version: model::FORMAT_VERSION,
        repository_id: storage.location().repository_id.clone(),
        current_object_version: None,
        generation: checkpoint.source_generation,
        checkpoint_id: Some(current.value.checkpoint_id.clone()),
        tail_batch_id: None,
    };
    if !catch_up(storage, repository, &empty_marker, current).await? {
        return Err(GitError::internal(
            "Git checkpoint tail does not lead to current",
        ));
    }
    if refs_hash(repository).await? != current.value.ref_state_hash {
        return Err(GitError::internal(
            "materialized Git refs do not match current",
        ));
    }
    Ok(())
}

async fn apply_batch(
    storage: &GitStorage,
    repository: &Path,
    batch: &GitPushBatch,
) -> Result<(), GitError> {
    for push in &batch.pushes {
        for pack_id in &push.pack_ids {
            install_pack(storage, repository, pack_id).await?;
        }
        apply_commands(repository, &push.reference_commands).await?;
    }
    if refs_hash(repository).await? != batch.resulting_ref_state_hash {
        return Err(GitError::internal(
            "Git push batch produced an unexpected ref state",
        ));
    }
    Ok(())
}

async fn compact(
    storage: &GitStorage,
    materialized: &MaterializedRepository,
    expected_version: u64,
    current: &GitCurrent,
) -> Result<(), GitError> {
    git(
        Command::new("git")
            .arg("-C")
            .arg(&materialized.repository)
            .args(["repack", "-ad"]),
        "compact Git packs",
    )
    .await?;
    let refs = refs(&materialized.repository).await?;
    let mut pack_ids = Vec::new();
    for path in pack_paths(&materialized.repository).await? {
        pack_ids.push(storage.put_pack(&path).await?);
    }
    pack_ids.sort_unstable();
    pack_ids.dedup();
    let references = model::references(&refs);
    let checkpoint = GitCheckpoint {
        format_version: model::FORMAT_VERSION,
        repository_id: storage.location().repository_id.clone(),
        source_generation: current.generation,
        ref_state_hash: model::refs_hash(&references),
        refs: references,
        pack_ids,
    };
    let checkpoint_id = storage.put_checkpoint(&checkpoint).await?;
    let compacted = GitCurrent {
        format_version: model::FORMAT_VERSION,
        repository_id: current.repository_id.clone(),
        generation: current.generation,
        checkpoint_id: checkpoint_id.clone(),
        tail_batch_id: None,
        tail_depth: 0,
        ref_state_hash: current.ref_state_hash.clone(),
    };
    match storage
        .publish_current(Some(expected_version), &compacted)
        .await
    {
        Ok(version) => {
            write_marker(
                &materialized.directory,
                &MaterializationMarker {
                    format_version: model::FORMAT_VERSION,
                    repository_id: current.repository_id.clone(),
                    current_object_version: Some(version),
                    generation: current.generation,
                    checkpoint_id: Some(checkpoint_id),
                    tail_batch_id: None,
                },
            )
            .await
        }
        Err(error) if error.status == axum::http::StatusCode::CONFLICT => {
            remove_marker(&materialized.directory).await;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn refs(repository: &Path) -> Result<BTreeMap<String, String>, GitError> {
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
    let text = String::from_utf8(output.stdout)
        .map_err(|_| GitError::internal("git for-each-ref returned non-UTF-8 output"))?;
    let mut refs = BTreeMap::new();
    for line in text.lines() {
        let (name, object_id) = line
            .split_once('\0')
            .ok_or_else(|| GitError::internal("git for-each-ref returned malformed output"))?;
        if refs.insert(name.to_owned(), object_id.to_owned()).is_some() {
            return Err(GitError::internal(
                "git for-each-ref returned a duplicate ref",
            ));
        }
    }
    Ok(refs)
}

fn reference_commands(
    before: &BTreeMap<String, String>,
    after: &BTreeMap<String, String>,
) -> Vec<GitReferenceCommand> {
    before
        .keys()
        .chain(after.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|name| {
            let old = before.get(name).cloned();
            let new = after.get(name).cloned();
            (old != new).then(|| GitReferenceCommand {
                ref_name: name.clone(),
                expected_old_object_id: old,
                new_object_id: new,
            })
        })
        .collect()
}

async fn refs_hash(repository: &Path) -> Result<String, GitError> {
    Ok(model::refs_hash(&model::references(
        &refs(repository).await?,
    )))
}

async fn init(repository: &Path) -> Result<(), GitError> {
    git(
        Command::new("git")
            .arg("init")
            .arg("--bare")
            .arg(repository),
        "initialize Git repository",
    )
    .await?;
    git(
        Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(["config", "http.receivepack", "true"]),
        "enable authenticated Git receive-pack",
    )
    .await?;
    git(
        Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(["config", "receive.unpackLimit", "0"]),
        "retain received Git packs",
    )
    .await
}

async fn install_pack(
    storage: &GitStorage,
    repository: &Path,
    pack_id: &str,
) -> Result<(), GitError> {
    let scratch = repository
        .parent()
        .ok_or_else(|| GitError::internal("Git repository has no cache parent"))?
        .join(format!("incoming-{pack_id}.pack"));
    storage.stream_pack(pack_id, &scratch).await?;
    let input = std::fs::File::open(&scratch)
        .map_err(|error| GitError::internal(format!("open downloaded Git pack: {error}")))?;
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["index-pack", "--stdin"])
        .stdin(Stdio::from(input))
        .output()
        .await
        .map_err(|error| GitError::internal(format!("install Git pack: {error}")))?;
    let _ = tokio::fs::remove_file(&scratch).await;
    if output.status.success() {
        return Ok(());
    }
    Err(GitError::internal(format!(
        "git index-pack failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

async fn set_refs(repository: &Path, refs: &[model::GitReference]) -> Result<(), GitError> {
    let commands = refs
        .iter()
        .map(|reference| GitReferenceCommand {
            ref_name: reference.ref_name.clone(),
            expected_old_object_id: None,
            new_object_id: Some(reference.object_id.clone()),
        })
        .collect::<Vec<_>>();
    apply_commands(repository, &commands).await
}

async fn apply_commands(
    repository: &Path,
    commands: &[GitReferenceCommand],
) -> Result<(), GitError> {
    if commands.is_empty() {
        return Ok(());
    }
    let mut child = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["update-ref", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| GitError::internal(format!("start git update-ref: {error}")))?;
    let mut input = String::from("start\n");
    for command in commands {
        match (&command.expected_old_object_id, &command.new_object_id) {
            (Some(old), Some(new)) => {
                input.push_str(&format!("update {} {new} {old}\n", command.ref_name));
            }
            (None, Some(new)) => {
                input.push_str(&format!("create {} {new}\n", command.ref_name));
            }
            (Some(old), None) => {
                input.push_str(&format!("delete {} {old}\n", command.ref_name));
            }
            (None, None) => continue,
        }
    }
    input.push_str("prepare\ncommit\n");
    child
        .stdin
        .take()
        .ok_or_else(|| GitError::internal("git update-ref stdin is unavailable"))?
        .write_all(input.as_bytes())
        .await
        .map_err(|error| GitError::internal(format!("write git update-ref input: {error}")))?;
    let output = child
        .wait_with_output()
        .await
        .map_err(|error| GitError::internal(format!("wait for git update-ref: {error}")))?;
    if output.status.success() {
        return Ok(());
    }
    Err(GitError::internal(format!(
        "git update-ref failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

async fn pack_paths(repository: &Path) -> Result<BTreeSet<PathBuf>, GitError> {
    let directory = repository.join("objects/pack");
    let mut paths = BTreeSet::new();
    let mut entries = match tokio::fs::read_dir(&directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(paths),
        Err(error) => {
            return Err(GitError::internal(format!(
                "read Git pack directory: {error}"
            )));
        }
    };
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| GitError::internal(format!("read Git pack entry: {error}")))?
    {
        let path = entry.path();
        if path.extension().is_some_and(|value| value == "pack") {
            paths.insert(path);
        }
    }
    Ok(paths)
}

async fn marker_matches(
    directory: &Path,
    repository_id: &str,
    current: Option<&StoredCurrent>,
) -> Result<bool, GitError> {
    let Some(marker) = read_marker(directory).await? else {
        return Ok(false);
    };
    Ok(marker.format_version == model::FORMAT_VERSION
        && marker.repository_id == repository_id
        && marker.current_object_version == current.map(|value| value.object_version))
}

async fn read_marker(directory: &Path) -> Result<Option<MaterializationMarker>, GitError> {
    let bytes = match tokio::fs::read(directory.join(MARKER_FILE)).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(GitError::internal(format!(
                "read Git materialization marker: {error}"
            )));
        }
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| GitError::internal(format!("decode Git cache marker: {error}")))
}

async fn write_marker(directory: &Path, marker: &MaterializationMarker) -> Result<(), GitError> {
    tokio::fs::create_dir_all(directory)
        .await
        .map_err(|error| GitError::internal(format!("create Git cache directory: {error}")))?;
    let bytes = serde_json::to_vec(marker)
        .map_err(|error| GitError::internal(format!("encode Git cache marker: {error}")))?;
    let temporary = directory.join(format!("{MARKER_FILE}.{}.tmp", uuid::Uuid::new_v4()));
    let destination = directory.join(MARKER_FILE);
    let mut file = tokio::fs::File::create(&temporary)
        .await
        .map_err(|error| GitError::internal(format!("create Git cache marker: {error}")))?;
    file.write_all(&bytes)
        .await
        .map_err(|error| GitError::internal(format!("write Git cache marker: {error}")))?;
    file.sync_all()
        .await
        .map_err(|error| GitError::internal(format!("sync Git cache marker: {error}")))?;
    tokio::fs::rename(&temporary, &destination)
        .await
        .map_err(|error| GitError::internal(format!("publish Git cache marker: {error}")))
}

async fn remove_marker(directory: &Path) {
    let _ = tokio::fs::remove_file(directory.join(MARKER_FILE)).await;
}

async fn remove_disposable(path: &Path) -> Result<(), GitError> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
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
