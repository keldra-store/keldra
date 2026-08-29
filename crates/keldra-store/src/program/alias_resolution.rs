use std::collections::BTreeSet;

use keldra_atomic_program::VersionedWrite;

use super::*;

impl Store {
    pub async fn resolve_program_alias_bindings(
        &self,
        paths: &[ExpandedProgramPath],
    ) -> Result<Vec<ProgramAliasBinding>, ProgramStoreError> {
        let mut bindings = Vec::with_capacity(paths.len());
        for expanded in paths {
            let requested_key = object_key(&expanded.path)?;
            let requested = self
                .open_object(&requested_key, None)
                .await
                .map_err(program_mutation_error)?;
            let mut alias_binding = None;
            let requested_version = requested.as_ref().map(|object| object.version.clone());
            if let Some(object) = requested
                && object.version.protected_link_descriptor
                && object.version.content_type.as_deref() == Some(crate::OBJECT_LINK_CONTENT_TYPE)
            {
                if let Some(blob) = object.version.blob.as_ref() {
                    let descriptor_bytes = self
                        .read_blob_bytes(blob)
                        .await
                        .map_err(program_mutation_error)?;
                    if let Ok(descriptor) = crate::ObjectLinkDescriptor::decode(&descriptor_bytes)
                        && let Ok(canonical_path) = ObjectPath::new(
                            &expanded.path.tenant,
                            &expanded.path.bucket,
                            descriptor.target_path(),
                        )
                    {
                        let identity = self
                            .resolve_bucket_identity(&canonical_path.tenant, &canonical_path.bucket)
                            .map_err(program_mutation_error)?;
                        let registry = self
                            .object_alias_registry(
                                identity.tenant_id.0,
                                identity.bucket_id.0,
                                &canonical_path.path,
                            )
                            .map_err(program_mutation_error)?;
                        if registry.as_ref().is_some_and(|registry| {
                            registry.aliases.binary_search(&expanded.path.path).is_ok()
                        }) {
                            let canonical = self
                                .open_object(&object_key(&canonical_path)?, None)
                                .await
                                .map_err(program_mutation_error)?
                                .ok_or_else(|| {
                                    ProgramStoreError::InvalidBundle(
                                        "proven alias target does not exist".into(),
                                    )
                                })?;
                            if canonical.version.deleted {
                                return Err(ProgramStoreError::InvalidBundle(
                                    "proven alias target is deleted".into(),
                                ));
                            }
                            if canonical.version.protected_link_descriptor {
                                return Err(ProgramStoreError::InvalidBundle(
                                    "proven alias target is itself a protected alias descriptor"
                                        .into(),
                                ));
                            }
                            alias_binding = Some(ProgramAliasBinding {
                                requested_path: expanded.path.clone(),
                                canonical_path,
                                descriptor_version: Some(object.version),
                                descriptor_bytes: Some(descriptor_bytes),
                                canonical_version: Some(canonical.version),
                                alias_registry: registry,
                            });
                        }
                    }
                }
            }
            if alias_binding.is_none()
                && requested_version
                    .as_ref()
                    .is_some_and(|version| version.protected_link_descriptor)
            {
                return Err(ProgramStoreError::InvalidBundle(
                    "protected alias descriptor has no exact target-sidecar provenance".into(),
                ));
            }
            let binding = if let Some(binding) = alias_binding {
                binding
            } else {
                let identity = self
                    .resolve_bucket_identity(&expanded.path.tenant, &expanded.path.bucket)
                    .map_err(program_mutation_error)?;
                ProgramAliasBinding {
                    requested_path: expanded.path.clone(),
                    canonical_path: expanded.path.clone(),
                    descriptor_version: None,
                    descriptor_bytes: None,
                    canonical_version: requested_version,
                    alias_registry: self
                        .object_alias_registry(
                            identity.tenant_id.0,
                            identity.bucket_id.0,
                            &expanded.path.path,
                        )
                        .map_err(program_mutation_error)?,
                }
            };
            binding.validate()?;
            bindings.push(binding);
        }
        bindings.sort_by(|left, right| left.requested_path.cmp(&right.requested_path));
        let mut canonical = BTreeSet::new();
        if bindings
            .iter()
            .any(|binding| !canonical.insert(binding.canonical_path.clone()))
        {
            return Err(ProgramStoreError::InvalidBundle(
                "one physical object is bound more than once".into(),
            ));
        }
        Ok(bindings)
    }
}

pub(super) fn stored_alias_delete_binding<'a>(
    write: &VersionedWrite,
    alias_bindings: &'a [ProgramAliasBinding],
) -> Option<&'a ProgramAliasBinding> {
    write.value.is_none().then(|| {
        alias_bindings.iter().find(|binding| {
            binding.requested_path != binding.canonical_path && binding.canonical_path == write.path
        })
    })?
}

pub(super) fn stored_alias_registry_transitions(
    source: &AtomicWriteBundle,
    alias_bindings: &[ProgramAliasBinding],
) -> Result<Vec<StoredProgramAliasRegistryTransition>, ProgramStoreError> {
    let mut transitions = Vec::new();
    for write in &source.writes {
        let Some(binding) = stored_alias_delete_binding(write, alias_bindings) else {
            continue;
        };
        let expected = binding.alias_registry.clone().ok_or_else(|| {
            ProgramStoreError::InvalidBundle(
                "alias delete is not present in its canonical target registry".into(),
            )
        })?;
        if !expected.aliases.contains(&binding.requested_path.path) {
            return Err(ProgramStoreError::InvalidBundle(
                "alias delete is not present in its canonical target registry".into(),
            ));
        }
        let replacement_aliases = expected
            .aliases
            .iter()
            .filter(|alias| *alias != &binding.requested_path.path)
            .cloned()
            .collect();
        let transition = StoredProgramAliasRegistryTransition {
            target: binding.canonical_path.clone(),
            expected,
            replacement_aliases,
        };
        transition
            .validate()
            .map_err(ProgramStoreError::InvalidBundle)?;
        transitions.push(transition);
    }
    transitions.sort_by(|left, right| left.target.cmp(&right.target));
    if transitions
        .windows(2)
        .any(|pair| pair[0].target >= pair[1].target)
    {
        return Err(ProgramStoreError::InvalidBundle(
            "stored-program alias transitions are not unique".into(),
        ));
    }
    Ok(transitions)
}
