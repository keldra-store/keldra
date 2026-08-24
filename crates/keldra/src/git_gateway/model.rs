use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::GitError;

pub(super) const FORMAT_VERSION: u16 = 2;
const REF_STATE_CONTEXT: &str = "keldra.git/ref-state/v2";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GitCurrent {
    pub(super) format_version: u16,
    pub(super) repository_id: String,
    pub(super) generation: u64,
    pub(super) checkpoint_id: String,
    pub(super) tail_batch_id: Option<String>,
    pub(super) tail_depth: u64,
    pub(super) ref_state_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GitPushBatch {
    pub(super) format_version: u16,
    pub(super) repository_id: String,
    pub(super) parent_batch_id: Option<String>,
    pub(super) base_checkpoint_id: String,
    pub(super) first_generation: u64,
    pub(super) pushes: Vec<GitPush>,
    pub(super) resulting_ref_state_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GitPush {
    pub(super) operation_id: String,
    pub(super) authenticated_principal: String,
    pub(super) accepted_at_unix_ms: u64,
    pub(super) pack_ids: Vec<String>,
    pub(super) reference_commands: Vec<GitReferenceCommand>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GitReferenceCommand {
    pub(super) ref_name: String,
    pub(super) expected_old_object_id: Option<String>,
    pub(super) new_object_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GitCheckpoint {
    pub(super) format_version: u16,
    pub(super) repository_id: String,
    pub(super) source_generation: u64,
    pub(super) refs: Vec<GitReference>,
    pub(super) pack_ids: Vec<String>,
    pub(super) ref_state_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GitReference {
    pub(super) ref_name: String,
    pub(super) object_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepositoryName {
    pub(super) format_version: u16,
    pub(super) repository_id: String,
}

impl GitCurrent {
    pub(super) fn validate(&self, expected_repository_id: &str) -> Result<(), GitError> {
        require_header(
            self.format_version,
            &self.repository_id,
            expected_repository_id,
        )?;
        require_identity(&self.checkpoint_id, "checkpoint ID")?;
        if let Some(id) = &self.tail_batch_id {
            require_identity(id, "tail batch ID")?;
        }
        require_identity(&self.ref_state_hash, "ref state hash")?;
        if self.tail_depth == 0 && self.tail_batch_id.is_some() {
            return Err(GitError::internal(
                "Git current has a tail identity with zero depth",
            ));
        }
        if self.tail_depth > 0 && self.tail_batch_id.is_none() {
            return Err(GitError::internal(
                "Git current has tail depth without a tail identity",
            ));
        }
        Ok(())
    }
}

impl GitPushBatch {
    pub(super) fn encode(&self) -> Result<(String, Vec<u8>), GitError> {
        self.validate(&self.repository_id)?;
        encode_identified(self)
    }

    pub(super) fn validate(&self, expected_repository_id: &str) -> Result<(), GitError> {
        require_header(
            self.format_version,
            &self.repository_id,
            expected_repository_id,
        )?;
        require_identity(&self.base_checkpoint_id, "base checkpoint ID")?;
        if let Some(id) = &self.parent_batch_id {
            require_identity(id, "parent batch ID")?;
        }
        require_identity(&self.resulting_ref_state_hash, "ref state hash")?;
        if self.first_generation == 0 || self.pushes.is_empty() {
            return Err(GitError::internal("Git push batch is empty or unnumbered"));
        }
        for push in &self.pushes {
            if push.operation_id.is_empty()
                || push.authenticated_principal.is_empty()
                || push.accepted_at_unix_ms == 0
                || push.reference_commands.is_empty()
            {
                return Err(GitError::internal(
                    "Git push entry has no operation identity or ref commands",
                ));
            }
            let mut previous = None;
            for pack_id in &push.pack_ids {
                require_identity(pack_id, "pack ID")?;
                if previous.is_some_and(|value: &String| value >= pack_id) {
                    return Err(GitError::internal(
                        "Git push pack identities are not strictly ordered",
                    ));
                }
                previous = Some(pack_id);
            }
            let mut previous_ref = None;
            for command in &push.reference_commands {
                require_ref_name(&command.ref_name)?;
                require_git_object_id(command.expected_old_object_id.as_deref())?;
                require_git_object_id(command.new_object_id.as_deref())?;
                if command.expected_old_object_id == command.new_object_id {
                    return Err(GitError::internal(
                        "Git ref command does not change the ref",
                    ));
                }
                if previous_ref.is_some_and(|value: &String| value >= &command.ref_name) {
                    return Err(GitError::internal(
                        "Git ref commands are not strictly ordered",
                    ));
                }
                previous_ref = Some(&command.ref_name);
            }
        }
        Ok(())
    }
}

impl GitCheckpoint {
    pub(super) fn encode(&self) -> Result<(String, Vec<u8>), GitError> {
        self.validate(&self.repository_id)?;
        encode_identified(self)
    }

    pub(super) fn validate(&self, expected_repository_id: &str) -> Result<(), GitError> {
        require_header(
            self.format_version,
            &self.repository_id,
            expected_repository_id,
        )?;
        require_identity(&self.ref_state_hash, "ref state hash")?;
        let mut previous_ref = None;
        for reference in &self.refs {
            require_ref_name(&reference.ref_name)?;
            require_git_object_id(Some(&reference.object_id))?;
            if previous_ref.is_some_and(|value: &String| value >= &reference.ref_name) {
                return Err(GitError::internal(
                    "Git checkpoint refs are not strictly ordered",
                ));
            }
            previous_ref = Some(&reference.ref_name);
        }
        let mut previous_pack = None;
        for pack_id in &self.pack_ids {
            require_identity(pack_id, "pack ID")?;
            if previous_pack.is_some_and(|value: &String| value >= pack_id) {
                return Err(GitError::internal(
                    "Git checkpoint pack identities are not strictly ordered",
                ));
            }
            previous_pack = Some(pack_id);
        }
        if refs_hash(&self.refs) != self.ref_state_hash {
            return Err(GitError::internal(
                "Git checkpoint ref-state identity is invalid",
            ));
        }
        Ok(())
    }
}

impl RepositoryName {
    pub(super) fn encode(&self) -> Result<Vec<u8>, GitError> {
        if self.format_version != FORMAT_VERSION {
            return Err(GitError::internal("Git repository name format is invalid"));
        }
        require_repository_id(&self.repository_id)?;
        serde_json::to_vec(self)
            .map_err(|error| GitError::internal(format!("encode Git repository name: {error}")))
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, GitError> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|error| GitError::internal(format!("decode Git repository name: {error}")))?;
        if value.format_version != FORMAT_VERSION {
            return Err(GitError::internal("Git repository name format is invalid"));
        }
        require_repository_id(&value.repository_id)?;
        Ok(value)
    }
}

pub(super) fn decode_current(bytes: &[u8], repository_id: &str) -> Result<GitCurrent, GitError> {
    let value: GitCurrent = serde_json::from_slice(bytes)
        .map_err(|error| GitError::internal(format!("decode Git current: {error}")))?;
    value.validate(repository_id)?;
    Ok(value)
}

pub(super) fn encode_current(value: &GitCurrent) -> Result<Vec<u8>, GitError> {
    value.validate(&value.repository_id)?;
    serde_json::to_vec(value)
        .map_err(|error| GitError::internal(format!("encode Git current: {error}")))
}

pub(super) fn decode_batch(
    expected_id: &str,
    bytes: &[u8],
    repository_id: &str,
) -> Result<GitPushBatch, GitError> {
    require_content_identity(expected_id, bytes, "Git push batch")?;
    let value: GitPushBatch = serde_json::from_slice(bytes)
        .map_err(|error| GitError::internal(format!("decode Git push batch: {error}")))?;
    value.validate(repository_id)?;
    Ok(value)
}

pub(super) fn decode_checkpoint(
    expected_id: &str,
    bytes: &[u8],
    repository_id: &str,
) -> Result<GitCheckpoint, GitError> {
    require_content_identity(expected_id, bytes, "Git checkpoint")?;
    let value: GitCheckpoint = serde_json::from_slice(bytes)
        .map_err(|error| GitError::internal(format!("decode Git checkpoint: {error}")))?;
    value.validate(repository_id)?;
    Ok(value)
}

pub(super) fn references(values: &BTreeMap<String, String>) -> Vec<GitReference> {
    values
        .iter()
        .map(|(ref_name, object_id)| GitReference {
            ref_name: ref_name.clone(),
            object_id: object_id.clone(),
        })
        .collect()
}

pub(super) fn refs_hash(refs: &[GitReference]) -> String {
    let mut hasher = blake3::Hasher::new_derive_key(REF_STATE_CONTEXT);
    for reference in refs {
        hasher.update(&(reference.ref_name.len() as u64).to_be_bytes());
        hasher.update(reference.ref_name.as_bytes());
        hasher.update(&(reference.object_id.len() as u64).to_be_bytes());
        hasher.update(reference.object_id.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

pub(super) fn content_identity(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn encode_identified<T: Serialize>(value: &T) -> Result<(String, Vec<u8>), GitError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| GitError::internal(format!("encode Git artifact: {error}")))?;
    Ok((content_identity(&bytes), bytes))
}

fn require_content_identity(
    expected: &str,
    bytes: &[u8],
    description: &str,
) -> Result<(), GitError> {
    require_identity(expected, description)?;
    if content_identity(bytes) != expected {
        return Err(GitError::internal(format!(
            "{description} content identity is invalid"
        )));
    }
    Ok(())
}

fn require_header(
    format_version: u16,
    repository_id: &str,
    expected_repository_id: &str,
) -> Result<(), GitError> {
    if format_version != FORMAT_VERSION {
        return Err(GitError::internal("Git artifact format is unsupported"));
    }
    require_repository_id(repository_id)?;
    if repository_id != expected_repository_id {
        return Err(GitError::internal(
            "Git artifact belongs to a different repository",
        ));
    }
    Ok(())
}

fn require_repository_id(value: &str) -> Result<(), GitError> {
    if uuid::Uuid::parse_str(value).is_err() {
        return Err(GitError::internal("Git repository identity is invalid"));
    }
    Ok(())
}

fn require_identity(value: &str, description: &str) -> Result<(), GitError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(GitError::internal(format!("{description} is invalid")));
    }
    Ok(())
}

fn require_ref_name(value: &str) -> Result<(), GitError> {
    if !value.starts_with("refs/") || value.as_bytes().contains(&0) {
        return Err(GitError::internal("Git ref name is invalid"));
    }
    Ok(())
}

fn require_git_object_id(value: Option<&str>) -> Result<(), GitError> {
    let Some(value) = value else {
        return Ok(());
    };
    if !matches!(value.len(), 40 | 64) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(GitError::internal("Git object identity is invalid"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repository_id() -> String {
        "018f7a31-6f7c-7d80-8000-000000000001".into()
    }

    #[test]
    fn checkpoint_identity_and_ref_hash_round_trip() {
        let refs = vec![GitReference {
            ref_name: "refs/heads/main".into(),
            object_id: "0123456789012345678901234567890123456789".into(),
        }];
        let checkpoint = GitCheckpoint {
            format_version: FORMAT_VERSION,
            repository_id: repository_id(),
            source_generation: 1,
            ref_state_hash: refs_hash(&refs),
            refs,
            pack_ids: Vec::new(),
        };
        let (identity, bytes) = checkpoint.encode().unwrap();
        assert_eq!(
            decode_checkpoint(&identity, &bytes, &repository_id()).unwrap(),
            checkpoint
        );
    }

    #[test]
    fn batch_rejects_unsorted_ref_commands() {
        let hash = "1".repeat(64);
        let batch = GitPushBatch {
            format_version: FORMAT_VERSION,
            repository_id: repository_id(),
            parent_batch_id: None,
            base_checkpoint_id: hash.clone(),
            first_generation: 1,
            pushes: vec![GitPush {
                operation_id: "push-1".into(),
                authenticated_principal: "test-app".into(),
                accepted_at_unix_ms: 1,
                pack_ids: Vec::new(),
                reference_commands: vec![
                    GitReferenceCommand {
                        ref_name: "refs/heads/z".into(),
                        expected_old_object_id: None,
                        new_object_id: Some("1".repeat(40)),
                    },
                    GitReferenceCommand {
                        ref_name: "refs/heads/a".into(),
                        expected_old_object_id: None,
                        new_object_id: Some("2".repeat(40)),
                    },
                ],
            }],
            resulting_ref_state_hash: hash,
        };
        assert!(batch.validate(&repository_id()).is_err());
    }
}
