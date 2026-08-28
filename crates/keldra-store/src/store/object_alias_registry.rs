use super::*;
use crate::{
    OBJECT_ALIAS_REGISTRY_FORMAT, OBJECT_ALIAS_REGISTRY_TRANSITION_FORMAT, ObjectAliasRegistry,
    ObjectAliasRegistryTransition, ObjectMutationContext,
};

const APPLIED_PREFIX: &[u8] = b"applied/v1/";

impl Store {
    pub fn object_alias_registry(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        canonical_path: &str,
    ) -> Result<Option<ObjectAliasRegistry>, MutationError> {
        let identity = alias_identity(tenant_id, bucket_id)?;
        validate_alias_canonical_path(canonical_path)?;
        self.alias_registry_locked(identity, canonical_path)
    }

    pub(crate) fn alias_registry_locked(
        &self,
        identity: BucketIdentity,
        canonical_path: &str,
    ) -> Result<Option<ObjectAliasRegistry>, MutationError> {
        let Some(encoded) = self
            .db
            .get_cf(
                self.cf(CF_OBJECT_ALIAS_REGISTRIES)?,
                identity.head_key(canonical_path),
            )
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let registry = decode_registry(&encoded)?;
        registry.validate(canonical_path)?;
        Ok(Some(registry))
    }

    pub(crate) fn stage_alias_registry_transition_locked(
        &self,
        batch: &mut WriteBatch,
        identity: BucketIdentity,
        canonical_path: &str,
        expected: Option<&ObjectAliasRegistry>,
        replacement_aliases: &[String],
        commit_cursor: u64,
    ) -> Result<(bool, Option<ObjectAliasRegistry>), MutationError> {
        validate_alias_canonical_path(canonical_path)?;
        if commit_cursor == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "alias registry commit cursor must be non-zero".into(),
            ));
        }
        if let Some(expected) = expected {
            expected.validate(canonical_path)?;
        }
        let replacement =
            derive_replacement(canonical_path, expected, replacement_aliases, commit_cursor)?;
        let expected_hash = expected
            .map(ObjectAliasRegistry::canonical_hash)
            .transpose()?;
        let replacement_hash = replacement
            .as_ref()
            .map(ObjectAliasRegistry::canonical_hash)
            .transpose()?;
        let applied = ObjectAliasRegistryTransition {
            format: OBJECT_ALIAS_REGISTRY_TRANSITION_FORMAT,
            commit_cursor,
            expected_hash,
            replacement_hash,
        };
        let applied_key = applied_key(identity, canonical_path);
        if let Some(existing) = self
            .read_json::<ObjectAliasRegistryTransition>(CF_OBJECT_ALIAS_REGISTRIES, &applied_key)?
        {
            existing.validate()?;
            if existing == applied {
                if self
                    .alias_registry_locked(identity, canonical_path)?
                    .as_ref()
                    != replacement.as_ref()
                {
                    return Err(MutationError::Storage(
                        "applied alias registry transition disagrees with its sidecar".into(),
                    ));
                }
                return Ok((false, replacement));
            }
            if existing.commit_cursor >= commit_cursor {
                return Err(MutationError::ObjectMutationConflict);
            }
        }
        let current = self.alias_registry_locked(identity, canonical_path)?;
        if current.as_ref() != expected {
            return Err(MutationError::ObjectMutationConflict);
        }
        let sidecar_key = identity.head_key(canonical_path);
        match replacement.as_ref() {
            Some(replacement) => batch.put_cf(
                self.cf(CF_OBJECT_ALIAS_REGISTRIES)?,
                sidecar_key,
                replacement.canonical_bytes()?,
            ),
            None => batch.delete_cf(self.cf(CF_OBJECT_ALIAS_REGISTRIES)?, sidecar_key),
        }
        batch.put_cf(
            self.cf(CF_OBJECT_ALIAS_REGISTRIES)?,
            applied_key,
            serde_json::to_vec(&applied).map_err(storage_error)?,
        );
        Ok((true, replacement))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn apply_alias_registry_transition(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        target: &ObjectKey,
        expected: Option<&ObjectAliasRegistry>,
        replacement_aliases: &[String],
        begin_cursor: u64,
        commit_cursor: u64,
        context: ObjectMutationContext,
    ) -> Result<Option<ObjectAliasRegistry>, MutationError> {
        let identity = alias_identity(tenant_id, bucket_id)?;
        validate_alias_canonical_path(target.path())?;
        let _path_guard = self.ordinary_locks.acquire(&[object_path(target)]).await;
        let _commit_guard = self.lock_commit("alias_registry_transition").await;
        self.require_committed_program_reservation_locked(
            identity,
            target.path(),
            begin_cursor,
            commit_cursor,
            context.serving_fence_term,
            context.active_placement_log_id,
        )?;
        if !replacement_aliases.is_empty() {
            self.require_live_unprotected_alias_target_locked(identity, target)?;
        }
        let mut batch = WriteBatch::default();
        let (changed, replacement) = self.stage_alias_registry_transition_locked(
            &mut batch,
            identity,
            target.path(),
            expected,
            replacement_aliases,
            commit_cursor,
        )?;
        if changed {
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
        }
        Ok(replacement)
    }

    fn require_live_unprotected_alias_target_locked(
        &self,
        identity: BucketIdentity,
        target: &ObjectKey,
    ) -> Result<(), MutationError> {
        let head = self
            .head_by_storage_key(&identity.head_key(target.path()))?
            .ok_or_else(|| {
                MutationError::InvalidObjectMutation(
                    "nonempty alias registry requires an existing canonical target".into(),
                )
            })?;
        let version = self
            .version_metadata_by_identity(identity, target, head.version)?
            .ok_or_else(|| {
                MutationError::Storage(
                    "canonical target head references a missing version descriptor".into(),
                )
            })?;
        if head.deleted
            || version.deleted
            || version.id != head.version
            || version.protected_link_descriptor
        {
            return Err(MutationError::InvalidObjectMutation(
                "nonempty alias registry requires a live ordinary canonical target".into(),
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn apply_alias_registry_replica_transition(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        target: &ObjectKey,
        expected: Option<&ObjectAliasRegistry>,
        replacement: Option<&ObjectAliasRegistry>,
        begin_cursor: u64,
        commit_cursor: u64,
        context: ObjectMutationContext,
    ) -> Result<bool, MutationError> {
        let aliases = replacement.map_or(&[][..], |value| value.aliases.as_slice());
        if derive_replacement(target.path(), expected, aliases, commit_cursor)?.as_ref()
            != replacement
        {
            return Err(MutationError::InvalidObjectMutation(
                "replicated alias registry replacement is not canonical".into(),
            ));
        }
        let before = self.object_alias_registry(tenant_id, bucket_id, target.path())?;
        let derived = self
            .apply_alias_registry_transition(
                tenant_id,
                bucket_id,
                target,
                expected,
                aliases,
                begin_cursor,
                commit_cursor,
                context,
            )
            .await?;
        debug_assert_eq!(derived.as_ref(), replacement);
        Ok(before.as_ref() != replacement)
    }
}

fn derive_replacement(
    canonical_path: &str,
    expected: Option<&ObjectAliasRegistry>,
    replacement_aliases: &[String],
    commit_cursor: u64,
) -> Result<Option<ObjectAliasRegistry>, MutationError> {
    if replacement_aliases.is_empty() {
        return Ok(None);
    }
    let registry = ObjectAliasRegistry {
        format: OBJECT_ALIAS_REGISTRY_FORMAT,
        revision: expected.map_or(Ok(1), |value| {
            value.revision.checked_add(1).ok_or_else(|| {
                MutationError::Storage("alias registry revision is exhausted".into())
            })
        })?,
        aliases: replacement_aliases.to_vec(),
        program_commit_cursor: Some(commit_cursor),
    };
    registry.validate(canonical_path)?;
    Ok(Some(registry))
}

fn alias_identity(tenant_id: u64, bucket_id: u64) -> Result<BucketIdentity, MutationError> {
    if tenant_id == 0 || bucket_id == 0 {
        return Err(MutationError::InvalidObjectMutation(
            "alias registry stable bucket identity must be non-zero".into(),
        ));
    }
    Ok(BucketIdentity {
        tenant_id: TenantId(tenant_id),
        bucket_id: BucketId(bucket_id),
    })
}

fn validate_alias_canonical_path(path: &str) -> Result<(), MutationError> {
    ObjectKey::new("validation", "validation", path).map_err(|error| {
        MutationError::InvalidObjectMutation(format!("alias registry target path: {error}"))
    })?;
    if contains_reserved_keldra_segment(path) {
        return Err(MutationError::InvalidObjectMutation(
            "alias registry target is in Keldra's reserved namespace".into(),
        ));
    }
    Ok(())
}

pub(super) fn applied_key(identity: BucketIdentity, canonical_path: &str) -> Vec<u8> {
    let identity = identity.encode();
    let mut key = Vec::with_capacity(APPLIED_PREFIX.len() + identity.len() + canonical_path.len());
    key.extend_from_slice(APPLIED_PREFIX);
    key.extend_from_slice(&identity);
    key.extend_from_slice(canonical_path.as_bytes());
    key
}

pub(super) fn decode_registry(mut encoded: &[u8]) -> Result<ObjectAliasRegistry, MutationError> {
    let format = take_u16(&mut encoded)?;
    let revision = take_u64(&mut encoded)?;
    let program_commit_cursor = Some(take_u64(&mut encoded)?);
    let count = take_u32(&mut encoded)? as usize;
    if count == 0
        || count > crate::MAX_INBOUND_OBJECT_LINKS
        || count.saturating_add(1) > crate::MAX_ATOMIC_BATCH_MUTATIONS
    {
        return Err(MutationError::Storage(
            "persisted alias registry count is invalid".into(),
        ));
    }
    let mut aliases = Vec::with_capacity(count);
    for _ in 0..count {
        let length = take_u32(&mut encoded)? as usize;
        let (alias, rest) = encoded.split_at_checked(length).ok_or_else(|| {
            MutationError::Storage("persisted alias registry is truncated".into())
        })?;
        encoded = rest;
        aliases.push(
            std::str::from_utf8(alias)
                .map_err(|_| MutationError::Storage("persisted alias path is not UTF-8".into()))?
                .to_owned(),
        );
    }
    if !encoded.is_empty() || format != OBJECT_ALIAS_REGISTRY_FORMAT {
        return Err(MutationError::Storage(
            "persisted alias registry encoding is non-canonical".into(),
        ));
    }
    Ok(ObjectAliasRegistry {
        format,
        revision,
        aliases,
        program_commit_cursor,
    })
}

fn take_u16(input: &mut &[u8]) -> Result<u16, MutationError> {
    let (value, rest) = input
        .split_at_checked(2)
        .ok_or_else(|| MutationError::Storage("persisted alias registry is truncated".into()))?;
    *input = rest;
    Ok(u16::from_be_bytes(value.try_into().expect("two bytes")))
}

fn take_u32(input: &mut &[u8]) -> Result<u32, MutationError> {
    let (value, rest) = input
        .split_at_checked(4)
        .ok_or_else(|| MutationError::Storage("persisted alias registry is truncated".into()))?;
    *input = rest;
    Ok(u32::from_be_bytes(value.try_into().expect("four bytes")))
}

fn take_u64(input: &mut &[u8]) -> Result<u64, MutationError> {
    let (value, rest) = input
        .split_at_checked(8)
        .ok_or_else(|| MutationError::Storage("persisted alias registry is truncated".into()))?;
    *input = rest;
    Ok(u64::from_be_bytes(value.try_into().expect("eight bytes")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_registry_codec_and_hash_are_stable() {
        let registry = ObjectAliasRegistry {
            format: OBJECT_ALIAS_REGISTRY_FORMAT,
            revision: 7,
            aliases: vec!["alias/a".into(), "alias/b".into()],
            program_commit_cursor: Some(41),
        };
        registry.validate("target").unwrap();
        let encoded = registry.canonical_bytes().unwrap();
        assert_eq!(decode_registry(&encoded).unwrap(), registry);
        assert_eq!(
            registry.canonical_hash().unwrap(),
            *blake3::hash(&encoded).as_bytes()
        );
    }

    #[test]
    fn replacement_revision_is_derived_only_after_commit() {
        let first = derive_replacement("target", None, &["alias".into()], 11)
            .unwrap()
            .unwrap();
        assert_eq!(first.revision, 1);
        assert_eq!(first.program_commit_cursor, Some(11));
        let second = derive_replacement(
            "target",
            Some(&first),
            &["alias".into(), "other".into()],
            19,
        )
        .unwrap()
        .unwrap();
        assert_eq!(second.revision, 2);
        assert_eq!(second.program_commit_cursor, Some(19));
        assert!(
            derive_replacement("target", Some(&second), &[], 23)
                .unwrap()
                .is_none()
        );
    }
}
