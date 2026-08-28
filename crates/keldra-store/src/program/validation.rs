use super::*;

pub(super) fn validate_protected_link_descriptor(
    version: &Version,
    bytes: &[u8],
) -> Result<crate::ObjectLinkDescriptor, ProgramStoreError> {
    if !version.protected_link_descriptor
        || version.deleted
        || version.content_type.as_deref() != Some(crate::OBJECT_LINK_CONTENT_TYPE)
        || version.blob.as_ref().is_none_or(|blob| {
            blob.hash != *blake3::hash(bytes).as_bytes() || blob.length != bytes.len() as u64
        })
    {
        return Err(ProgramStoreError::InvalidBundle(
            "protected alias descriptor provenance is not exact".into(),
        ));
    }
    crate::ObjectLinkDescriptor::decode(bytes)
        .map_err(|error| ProgramStoreError::InvalidBundle(error.to_string()))
}

pub(super) fn validate_observed_head(head: &ObservedHead) -> Result<(), ProgramStoreError> {
    if let ObservedHead::Version { version } = head {
        version.parse::<u64>().map_err(|_| {
            ProgramStoreError::InvalidBundle(format!("invalid store version `{version}`"))
        })?;
    }
    Ok(())
}

pub(super) fn live_version_length(version: &Version) -> Option<u64> {
    (!version.deleted)
        .then(|| version.blob.as_ref().map(|blob| blob.length))
        .flatten()
}

pub(super) fn validate_builtin_plan(
    plan: &BuiltInObjectTransactionPlan,
) -> Result<(), ProgramStoreError> {
    plan.participant_manifest
        .validate()
        .map_err(ProgramStoreError::InvalidBundle)?;
    if plan
        .writes
        .windows(2)
        .any(|pair| pair[0].path >= pair[1].path)
        || plan
            .head_preconditions
            .windows(2)
            .any(|pair| pair[0].path >= pair[1].path)
        || plan
            .read_proofs
            .windows(2)
            .any(|pair| pair[0].participant_index >= pair[1].participant_index)
        || plan
            .alias_observations
            .windows(2)
            .any(|pair| pair[0].requested_path >= pair[1].requested_path)
        || plan.alias_observations.len() > crate::MAX_ATOMIC_BATCH_MUTATIONS
        || plan.alias_registries.windows(2).any(|pair| {
            alias_registry_target_index(&pair[0]) >= alias_registry_target_index(&pair[1])
        })
    {
        return invalid_builtin("plan ordering is not canonical");
    }
    for access in &plan.alias_registries {
        validate_alias_registry_access(plan, access)?;
    }
    validate_builtin_receipt(&plan.receipt)?;
    match (plan.authority_kind, plan.contract_version) {
        (1, 1) => validate_clone_plan(plan),
        (2, 1) => validate_link_plan(plan),
        (3, 1) => validate_unlink_plan(plan),
        (4, 1) => validate_link_put_plan(plan),
        (5, 1) => validate_link_put_immutable_plan(plan),
        _ => invalid_builtin("unknown built-in object transaction contract"),
    }
}

fn alias_registry_target_index(access: &BuiltInAliasRegistryAccess) -> u32 {
    match access {
        BuiltInAliasRegistryAccess::Read {
            target_participant_index,
            ..
        }
        | BuiltInAliasRegistryAccess::Write {
            target_participant_index,
            ..
        } => *target_participant_index,
    }
}

fn validate_alias_registry_access(
    plan: &BuiltInObjectTransactionPlan,
    access: &BuiltInAliasRegistryAccess,
) -> Result<(), ProgramStoreError> {
    let target_index = alias_registry_target_index(access);
    let target = plan
        .participant_manifest
        .objects
        .get(target_index as usize)
        .ok_or_else(|| invalid_builtin_error("alias sidecar target participant is absent"))?;
    let (expected, replacement) = match access {
        BuiltInAliasRegistryAccess::Read { expected, .. } => (expected, None),
        BuiltInAliasRegistryAccess::Write {
            expected,
            replacement_aliases,
            ..
        } => (expected, Some(replacement_aliases.as_slice())),
    };
    let expected_condition = Some(match expected {
        Some(expected) => ProgramAliasRegistryCondition::Exact(expected.clone()),
        None => ProgramAliasRegistryCondition::Absent,
    });
    if target.condition.observed_head().is_none()
        || target.alias_registry != expected_condition
        || expected
            .as_ref()
            .is_some_and(|value| value.validate(&target.path.path).is_err())
        || replacement.is_some_and(|aliases| {
            !aliases.is_empty()
                && crate::ObjectAliasRegistry {
                    format: crate::OBJECT_ALIAS_REGISTRY_FORMAT,
                    revision: 1,
                    aliases: aliases.to_vec(),
                    program_commit_cursor: Some(1),
                }
                .validate(&target.path.path)
                .is_err()
        })
    {
        return invalid_builtin("alias sidecar access is malformed");
    }
    Ok(())
}

pub(super) fn validate_builtin_record(
    record: &StoredPreparedBundle,
) -> Result<(), ProgramStoreError> {
    let ProgramBundleAuthority::BuiltInObjectTransaction {
        kind,
        contract_version,
    } = record.authority
    else {
        if record.builtin_plan.is_some() {
            return invalid_builtin("non-built-in record carries a built-in plan");
        }
        validate_stored_alias_bindings(record)?;
        return Ok(());
    };
    let plan = record
        .builtin_plan
        .as_ref()
        .ok_or_else(|| invalid_builtin_error("built-in record omits its sealed plan"))?;
    let plan_bytes = serde_json::to_vec(plan).map_err(program_storage_error)?;
    if plan.authority_kind != kind
        || plan.contract_version != contract_version
        || record.participant_manifest != plan.participant_manifest
        || record.preconditions != plan.head_preconditions
        || record.receipt != plan.receipt
        || record.source_bundle_hash
            != PreparedBundleHash(tagged_hash(
                b"keldra.builtin-object-transaction.v1",
                &plan_bytes,
            ))
        || record.writes.len() != plan.writes.len()
    {
        return invalid_builtin("prepared record differs from its sealed plan");
    }
    validate_builtin_plan(plan)?;
    if !record.alias_bindings.is_empty() || !record.alias_registry_transitions.is_empty() {
        return invalid_builtin("built-in record carries stored-program alias bindings");
    }
    for (prepared, planned) in record.writes.iter().zip(&plan.writes) {
        if prepared.path != planned.path
            || prepared.expected != planned.expected
            || prepared.previous_version != planned.previous_version
        {
            return invalid_builtin("prepared write differs from its sealed plan");
        }
        let (blob, content_type, deleted) = planned_payload_shape(&planned.payload);
        if prepared.version.blob != blob
            || prepared.version.content_type != content_type
            || prepared.version.deleted != deleted
        {
            return invalid_builtin("prepared payload differs from its sealed plan");
        }
    }
    Ok(())
}

fn validate_stored_alias_bindings(record: &StoredPreparedBundle) -> Result<(), ProgramStoreError> {
    if matches!(
        record.authority,
        ProgramBundleAuthority::LegacyProgramOnly { .. }
    ) {
        return if record.alias_bindings.is_empty() && record.alias_registry_transitions.is_empty() {
            Ok(())
        } else {
            Err(ProgramStoreError::InvalidBundle(
                "legacy record carries alias bindings".into(),
            ))
        };
    }
    if record.alias_bindings.is_empty() {
        return if record.alias_registry_transitions.is_empty() {
            Ok(())
        } else {
            Err(ProgramStoreError::InvalidBundle(
                "stored-program alias transition has no alias binding".into(),
            ))
        };
    }
    if record
        .alias_bindings
        .windows(2)
        .any(|pair| pair[0].requested_path >= pair[1].requested_path)
    {
        return Err(ProgramStoreError::InvalidBundle(
            "stored-program alias bindings are not canonical".into(),
        ));
    }
    let mut canonical = BTreeSet::new();
    let mut participants = BTreeSet::new();
    if record
        .alias_registry_transitions
        .windows(2)
        .any(|pair| pair[0].target >= pair[1].target)
        || record
            .alias_registry_transitions
            .iter()
            .any(|transition| transition.validate().is_err())
    {
        return Err(ProgramStoreError::InvalidBundle(
            "stored-program alias transitions are not canonical".into(),
        ));
    }
    for binding in &record.alias_bindings {
        binding.validate()?;
        if !canonical.insert(binding.canonical_path.clone()) {
            return Err(ProgramStoreError::InvalidBundle(
                "one physical object is bound more than once".into(),
            ));
        }
        let target = participant(
            &record.participant_manifest.objects,
            &binding.canonical_path,
        )?;
        let expected_target = match &binding.canonical_version {
            Some(version) => ProgramPathCondition::HeadVersion {
                expected: version.clone(),
            },
            None => ProgramPathCondition::Head(ObservedHead::NeverExisted),
        };
        let expected_registry = Some(match &binding.alias_registry {
            Some(registry) => ProgramAliasRegistryCondition::Exact(registry.clone()),
            None => ProgramAliasRegistryCondition::Absent,
        });
        if target.condition != expected_target || target.alias_registry != expected_registry {
            return Err(ProgramStoreError::InvalidBundle(
                "stored-program canonical participant differs from its alias binding".into(),
            ));
        }
        participants.insert(binding.canonical_path.clone());
        if let Some(descriptor) = &binding.descriptor_version {
            let alias = participant(
                &record.participant_manifest.objects,
                &binding.requested_path,
            )?;
            let unlink = record
                .alias_registry_transitions
                .iter()
                .find(|transition| transition.target == binding.canonical_path);
            let expected_alias_intent = if unlink.is_some() {
                put_intent(true)
            } else {
                read_only_intent()
            };
            if alias.condition
                != (ProgramPathCondition::HeadVersion {
                    expected: descriptor.clone(),
                })
                || alias.intent != expected_alias_intent
                || alias.alias_registry.is_some()
            {
                return Err(ProgramStoreError::InvalidBundle(
                    "stored-program alias participant differs from its descriptor".into(),
                ));
            }
            participants.insert(binding.requested_path.clone());
            if let Some(transition) = unlink {
                let expected_replacement = transition
                    .expected
                    .aliases
                    .iter()
                    .filter(|alias| *alias != &binding.requested_path.path)
                    .cloned()
                    .collect::<Vec<_>>();
                let write = record
                    .writes
                    .iter()
                    .find(|write| write.path == binding.requested_path);
                if transition.expected
                    != binding.alias_registry.clone().ok_or_else(|| {
                        ProgramStoreError::InvalidBundle(
                            "stored-program alias transition has no exact sidecar".into(),
                        )
                    })?
                    || transition.replacement_aliases != expected_replacement
                    || write.is_none_or(|write| {
                        !write.version.deleted
                            || write.previous_version.as_ref() != Some(descriptor)
                    })
                {
                    return Err(ProgramStoreError::InvalidBundle(
                        "stored-program alias delete is not exactly sealed".into(),
                    ));
                }
            }
        }
    }
    if record.alias_registry_transitions.iter().any(|transition| {
        !record.alias_bindings.iter().any(|binding| {
            binding.canonical_path == transition.target
                && binding.requested_path != binding.canonical_path
        })
    }) {
        return Err(ProgramStoreError::InvalidBundle(
            "stored-program alias transition is unbound".into(),
        ));
    }
    if participants.len() != record.participant_manifest.objects.len()
        || record
            .participant_manifest
            .objects
            .iter()
            .any(|participant| !participants.contains(&participant.path))
    {
        return Err(ProgramStoreError::InvalidBundle(
            "stored-program manifest contains an unbound participant".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PreparedAliasPublication {
    pub identity: BucketIdentity,
    pub requested_path: String,
    pub canonical_path: String,
    pub canonical_version: VersionId,
    pub deleted: bool,
}

pub(super) fn prepared_alias_publications(
    record: &StoredPreparedBundle,
) -> Result<Vec<PreparedAliasPublication>, ProgramStoreError> {
    if record.builtin_plan.is_none() {
        let mut aliases = Vec::new();
        for binding in &record.alias_bindings {
            if let Some(transition) = record
                .alias_registry_transitions
                .iter()
                .find(|transition| transition.target == binding.canonical_path)
            {
                let canonical_version = binding
                    .canonical_version
                    .as_ref()
                    .filter(|version| !version.deleted)
                    .map(|version| version.id)
                    .ok_or_else(|| {
                        ProgramStoreError::InvalidBundle(
                            "alias delete has no live canonical target".into(),
                        )
                    })?;
                let participant = participant(
                    &record.participant_manifest.objects,
                    &binding.canonical_path,
                )?;
                if transition
                    .replacement_aliases
                    .contains(&binding.requested_path.path)
                {
                    return Err(ProgramStoreError::InvalidBundle(
                        "alias delete remains in its replacement sidecar".into(),
                    ));
                }
                aliases.push(PreparedAliasPublication {
                    identity: BucketIdentity {
                        tenant_id: TenantId(participant.tenant_id),
                        bucket_id: BucketId(participant.bucket_id),
                    },
                    requested_path: binding.requested_path.path.clone(),
                    canonical_path: binding.canonical_path.path.clone(),
                    canonical_version,
                    deleted: true,
                });
                continue;
            }
            let Some(write) = record
                .writes
                .iter()
                .find(|write| write.path == binding.canonical_path)
            else {
                continue;
            };
            let participant = participant(
                &record.participant_manifest.objects,
                &binding.canonical_path,
            )?;
            aliases.extend(
                binding
                    .alias_registry
                    .iter()
                    .flat_map(|registry| registry.aliases.iter())
                    .map(|requested_path| PreparedAliasPublication {
                        identity: BucketIdentity {
                            tenant_id: TenantId(participant.tenant_id),
                            bucket_id: BucketId(participant.bucket_id),
                        },
                        requested_path: requested_path.clone(),
                        canonical_path: binding.canonical_path.path.clone(),
                        canonical_version: write.version.id,
                        deleted: write.version.deleted,
                    }),
            );
        }
        aliases.sort_by(|left, right| {
            (
                left.identity.tenant_id,
                left.identity.bucket_id,
                left.requested_path.as_str(),
            )
                .cmp(&(
                    right.identity.tenant_id,
                    right.identity.bucket_id,
                    right.requested_path.as_str(),
                ))
        });
        if aliases.windows(2).any(|pair| {
            pair[0].identity == pair[1].identity && pair[0].requested_path == pair[1].requested_path
        }) {
            return Err(ProgramStoreError::InvalidBundle(
                "stored-program alias publications are not unique".into(),
            ));
        }
        return Ok(aliases);
    }
    let plan = record.builtin_plan.as_ref().expect("checked above");
    plan.alias_observations
        .iter()
        .map(|observation| {
            let canonical = plan
                .participant_manifest
                .objects
                .get(observation.canonical_participant_index as usize)
                .ok_or_else(|| invalid_builtin_error("alias canonical participant is absent"))?;
            let canonical_version = record
                .writes
                .iter()
                .find(|write| write.path == canonical.path)
                .map(|write| write.version.id)
                .or_else(|| match &canonical.condition {
                    ProgramPathCondition::Head(ObservedHead::Version { version }) => {
                        version.parse::<u64>().ok().map(VersionId)
                    }
                    ProgramPathCondition::HeadVersion { expected }
                    | ProgramPathCondition::RetainedVersion { expected } => Some(expected.id),
                    ProgramPathCondition::HeadAndRetainedVersion { head, .. } => Some(head.id),
                    ProgramPathCondition::Head(ObservedHead::NeverExisted) => None,
                })
                .filter(|version| version.0 != 0)
                .ok_or_else(|| invalid_builtin_error("alias canonical version is absent"))?;
            Ok(PreparedAliasPublication {
                identity: BucketIdentity {
                    tenant_id: TenantId(canonical.tenant_id),
                    bucket_id: BucketId(canonical.bucket_id),
                },
                requested_path: observation.requested_path.path.clone(),
                canonical_path: canonical.path.path.clone(),
                canonical_version,
                deleted: observation.deleted,
            })
        })
        .collect()
}

pub(super) fn publishes_physical_writes(record: &StoredPreparedBundle) -> bool {
    !matches!(
        record.authority,
        ProgramBundleAuthority::BuiltInObjectTransaction { kind: 2 | 3, .. }
    )
}

pub(super) fn publishes_physical_write(record: &StoredPreparedBundle, path: &ObjectPath) -> bool {
    publishes_physical_writes(record)
        && !record.alias_registry_transitions.iter().any(|transition| {
            record.alias_bindings.iter().any(|binding| {
                transition.target == binding.canonical_path && binding.requested_path == *path
            })
        })
}

fn planned_payload_shape(payload: &BuiltInWritePayload) -> (Option<BlobRef>, Option<String>, bool) {
    match payload {
        BuiltInWritePayload::Inline {
            bytes,
            content_type,
        } => (
            Some(BlobRef {
                hash: *blake3::hash(bytes).as_bytes(),
                length: bytes.len() as u64,
            }),
            Some(content_type.clone()),
            false,
        ),
        BuiltInWritePayload::ExistingReference(value) => (
            Some(BlobRef {
                hash: value.blob_hash,
                length: value.blob_length,
            }),
            value.content_type.clone(),
            false,
        ),
        BuiltInWritePayload::StagedReference {
            blob_hash,
            blob_length,
            content_type,
            ..
        } => (
            Some(BlobRef {
                hash: *blob_hash,
                length: *blob_length,
            }),
            content_type.clone(),
            false,
        ),
        BuiltInWritePayload::Tombstone => (None, None, true),
    }
}

fn validate_builtin_receipt(receipt: &CommandReceipt) -> Result<(), ProgramStoreError> {
    let fingerprint = hex::decode(&receipt.input_fingerprint).unwrap_or_default();
    if receipt.program_path_hash != [0; 32]
        || receipt.command_id.is_empty()
        || fingerprint.len() != 32
        || !receipt.outputs.is_empty()
    {
        return invalid_builtin("receipt shape is invalid");
    }
    Ok(())
}

fn validate_clone_plan(plan: &BuiltInObjectTransactionPlan) -> Result<(), ProgramStoreError> {
    let manifest = &plan.participant_manifest;
    if manifest.governance.len() != 1 || plan.assertions.len() != 1 || plan.writes.len() != 1 {
        return invalid_builtin("clone cardinality is invalid");
    }
    let BuiltInTransactionAssertion::ClonePaths {
        source_requested_path,
        destination_requested_path,
        source_participant_index,
        destination_participant_index,
    } = &plan.assertions[0]
    else {
        return invalid_builtin("clone path assertion is absent");
    };
    let source = manifest
        .objects
        .get(*source_participant_index as usize)
        .ok_or_else(|| invalid_builtin_error("clone source participant is absent"))?;
    let destination = manifest
        .objects
        .get(*destination_participant_index as usize)
        .ok_or_else(|| invalid_builtin_error("clone destination participant is absent"))?;
    let source_version = source
        .condition
        .retained_version()
        .ok_or_else(|| invalid_builtin_error("clone source is not exact retained state"))?;
    let destination_expected = destination
        .condition
        .observed_head()
        .ok_or_else(|| invalid_builtin_error("clone destination has no exact head"))?;
    let same_canonical = source.path == destination.path;
    require_same_governed_bucket(manifest, &manifest.objects.iter().collect::<Vec<_>>())?;
    if source_requested_path == destination_requested_path
        || source_requested_path.tenant != destination_requested_path.tenant
        || source_requested_path.bucket != destination_requested_path.bucket
        || source.path.tenant != destination.path.tenant
        || source.path.bucket != destination.path.bucket
        || crate::key::contains_reserved_keldra_segment(&source.path.path)
        || crate::key::contains_reserved_keldra_segment(&destination.path.path)
        || (!same_canonical && source.intent != read_only_intent())
        || destination.intent != put_intent(false)
        || (same_canonical
            && !matches!(
                source.condition,
                ProgramPathCondition::HeadAndRetainedVersion { .. }
            ))
        || (!same_canonical
            && !matches!(
                source.condition,
                ProgramPathCondition::RetainedVersion { .. }
            ))
    {
        return invalid_builtin("clone participants or paths are invalid");
    }
    let write = &plan.writes[0];
    let BuiltInWritePayload::ExistingReference(reference) = &write.payload else {
        return invalid_builtin("clone payload is not an exact existing reference");
    };
    if source_version.deleted
        || source_version.protected_link_descriptor
        || source_version.blob.as_ref().is_none_or(|blob| {
            blob.hash != reference.blob_hash || blob.length != reference.blob_length
        })
        || source_version.content_type != reference.content_type
        || manifest
            .objects
            .get(reference.source_participant_index as usize)
            != Some(source)
        || write.path != destination.path
        || write.expected != destination_expected
        || !previous_matches(&destination_expected, write.previous_version.as_ref())
        || destination
            .condition
            .head_version()
            .is_some_and(|version| version.protected_link_descriptor)
        || write
            .previous_version
            .as_ref()
            .is_some_and(|version| version.protected_link_descriptor)
    {
        return invalid_builtin("clone write is not bound to exact source and destination");
    }
    let source_alias = validate_clone_requested_path(plan, source_requested_path, source)?;
    let destination_alias =
        validate_clone_requested_path(plan, destination_requested_path, destination)?;
    let mut required_proof_indices = [source_alias, destination_alias]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    required_proof_indices.sort_unstable();
    required_proof_indices.dedup();
    let mut actual_proof_indices = plan
        .read_proofs
        .iter()
        .map(|proof| proof.participant_index)
        .collect::<Vec<_>>();
    actual_proof_indices.sort_unstable();
    if actual_proof_indices != required_proof_indices {
        return invalid_builtin("clone alias proofs differ from requested paths");
    }
    let mut required_object_indices =
        vec![*source_participant_index, *destination_participant_index];
    required_object_indices.extend(required_proof_indices.iter().copied());
    required_object_indices.sort_unstable();
    required_object_indices.dedup();
    if required_object_indices.len() != manifest.objects.len()
        || required_object_indices
            .iter()
            .copied()
            .ne(0..u32::try_from(manifest.objects.len()).unwrap_or(u32::MAX))
    {
        return invalid_builtin("clone contains unrelated participants");
    }

    let mut required_registry_targets = vec![*destination_participant_index];
    if source_alias.is_some() {
        required_registry_targets.push(*source_participant_index);
    }
    required_registry_targets.sort_unstable();
    required_registry_targets.dedup();
    let actual_registry_targets = plan
        .alias_registries
        .iter()
        .map(|access| match access {
            BuiltInAliasRegistryAccess::Read {
                target_participant_index,
                ..
            } => Ok(*target_participant_index),
            BuiltInAliasRegistryAccess::Write { .. } => {
                Err(invalid_builtin_error("clone cannot change alias sidecars"))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if actual_registry_targets != required_registry_targets {
        return invalid_builtin("clone sidecar reads differ from canonical paths");
    }
    let destination_registry = plan
        .alias_registries
        .iter()
        .find_map(|access| match access {
            BuiltInAliasRegistryAccess::Read {
                target_participant_index,
                expected,
            } if target_participant_index == destination_participant_index => Some(expected),
            _ => None,
        })
        .ok_or_else(|| invalid_builtin_error("clone destination sidecar read is absent"))?;
    let destination_aliases = destination_registry
        .as_ref()
        .map_or(&[][..], |registry| registry.aliases.as_slice())
        .iter()
        .map(|path| ObjectPath::new(&destination.path.tenant, &destination.path.bucket, path))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid_builtin_error(&error))?;
    validate_manifest_preconditions(plan)?;
    validate_exact_alias_observations(plan, &destination_aliases, &destination.path, false)
}

fn validate_clone_requested_path(
    plan: &BuiltInObjectTransactionPlan,
    requested: &ObjectPath,
    canonical: &ProgramObjectParticipant,
) -> Result<Option<u32>, ProgramStoreError> {
    if requested == &canonical.path {
        return Ok(None);
    }
    let requested_index = participant_index(&plan.participant_manifest.objects, requested)?;
    let requested_participant = &plan.participant_manifest.objects[requested_index as usize];
    let proof = plan
        .read_proofs
        .iter()
        .find(|proof| proof.participant_index == requested_index)
        .ok_or_else(|| invalid_builtin_error("clone alias descriptor proof is absent"))?;
    let descriptor = validate_protected_link_descriptor(
        &proof.expected,
        read_proof_bytes(plan, requested_index, &proof.expected)?,
    )?;
    let canonical_index = participant_index(&plan.participant_manifest.objects, &canonical.path)?;
    let registry = plan
        .alias_registries
        .iter()
        .find_map(|access| match access {
            BuiltInAliasRegistryAccess::Read {
                target_participant_index,
                expected: Some(registry),
            } if *target_participant_index == canonical_index => Some(registry),
            _ => None,
        })
        .ok_or_else(|| invalid_builtin_error("clone alias sidecar proof is absent"))?;
    if requested_participant.intent != read_only_intent()
        || requested_participant.alias_registry.is_some()
        || descriptor.target_path() != canonical.path.path
        || canonical
            .condition
            .head_version()
            .is_some_and(|version| version.protected_link_descriptor)
        || registry.aliases.binary_search(&requested.path).is_err()
    {
        return invalid_builtin("clone alias provenance is invalid");
    }
    Ok(Some(requested_index))
}

fn validate_link_plan(plan: &BuiltInObjectTransactionPlan) -> Result<(), ProgramStoreError> {
    let manifest = &plan.participant_manifest;
    if manifest.objects.len() != 2
        || manifest.governance.len() != 1
        || plan.head_preconditions.len() != 2
        || !plan.read_proofs.is_empty()
        || !plan.assertions.is_empty()
        || plan.alias_registries.len() != 1
        || plan.writes.len() != 1
    {
        return invalid_builtin("link cardinality is invalid");
    }
    let descriptor_write = &plan.writes[0];
    let BuiltInWritePayload::Inline {
        bytes: descriptor_bytes,
        content_type,
    } = &descriptor_write.payload
    else {
        return invalid_builtin("link descriptor payload is invalid");
    };
    let descriptor = crate::ObjectLinkDescriptor::decode(descriptor_bytes)
        .map_err(|error| invalid_builtin_error(&error.to_string()))?;
    let target = ObjectPath::new(
        &descriptor_write.path.tenant,
        &descriptor_write.path.bucket,
        descriptor.target_path(),
    )
    .map_err(|error| invalid_builtin_error(&error))?;
    let target_participant = participant(&manifest.objects, &target)?;
    let link_participant = participant(&manifest.objects, &descriptor_write.path)?;
    let BuiltInAliasRegistryAccess::Write {
        target_participant_index,
        expected,
        replacement_aliases,
    } = &plan.alias_registries[0]
    else {
        return invalid_builtin("link registry access is not a write");
    };
    let mut expected_aliases = expected
        .as_ref()
        .map_or_else(Vec::new, |value| value.aliases.clone());
    match expected_aliases.binary_search(&descriptor_write.path.path) {
        Ok(_) => return invalid_builtin("link already exists in target registry"),
        Err(index) => expected_aliases.insert(index, descriptor_write.path.path.clone()),
    }
    require_same_governed_bucket(manifest, &[target_participant, link_participant])?;
    if content_type != crate::OBJECT_LINK_CONTENT_TYPE
        || participant_index(&manifest.objects, &target)? != *target_participant_index
        || expected
            .as_ref()
            .is_some_and(|value| value.validate(&target.path).is_err())
        || *replacement_aliases != expected_aliases
        || descriptor_write.path == target
        || crate::key::contains_reserved_keldra_segment(&descriptor_write.path.path)
        || crate::key::contains_reserved_keldra_segment(&target.path)
        || descriptor_write.previous_version.is_some()
        || !matches!(descriptor_write.expected, ObservedHead::NeverExisted)
        || link_participant.intent != put_intent(false)
        || target_participant.intent != read_only_intent()
        || target_participant
            .condition
            .head_version()
            .is_none_or(|version| version.protected_link_descriptor)
    {
        return invalid_builtin("link descriptor, target, or sidecar transition is invalid");
    }
    validate_manifest_preconditions(plan)?;
    validate_exact_alias_observations(plan, &[descriptor_write.path.clone()], &target, false)
}

fn validate_unlink_plan(plan: &BuiltInObjectTransactionPlan) -> Result<(), ProgramStoreError> {
    let manifest = &plan.participant_manifest;
    if manifest.objects.len() != 2
        || manifest.governance.len() != 1
        || plan.head_preconditions.len() != 2
        || plan.read_proofs.len() != 1
        || !plan.assertions.is_empty()
        || plan.alias_registries.len() != 1
        || plan.writes.len() != 1
    {
        return invalid_builtin("unlink cardinality is invalid");
    }
    let link_write = &plan.writes[0];
    let link_previous = link_write
        .previous_version
        .as_ref()
        .ok_or_else(|| invalid_builtin_error("unlink descriptor predecessor is absent"))?;
    let link_index = participant_index(&manifest.objects, &link_write.path)?;
    let descriptor = validate_protected_link_descriptor(
        link_previous,
        read_proof_bytes(plan, link_index, link_previous)?,
    )?;
    let target = ObjectPath::new(
        &link_write.path.tenant,
        &link_write.path.bucket,
        descriptor.target_path(),
    )
    .map_err(|error| invalid_builtin_error(&error))?;
    let target_participant = participant(&manifest.objects, &target)?;
    let link_participant = participant(&manifest.objects, &link_write.path)?;
    let BuiltInAliasRegistryAccess::Write {
        target_participant_index,
        expected: Some(expected),
        replacement_aliases,
    } = &plan.alias_registries[0]
    else {
        return invalid_builtin("unlink sidecar write omits exact prior registry");
    };
    let mut expected_aliases = expected.aliases.clone();
    let Ok(index) = expected_aliases.binary_search(&link_write.path.path) else {
        return invalid_builtin("unlink alias is absent from target registry");
    };
    expected_aliases.remove(index);
    require_same_governed_bucket(manifest, &[target_participant, link_participant])?;
    if participant_index(&manifest.objects, &target)? != *target_participant_index
        || expected.validate(&target.path).is_err()
        || *replacement_aliases != expected_aliases
        || link_write.path == target
        || crate::key::contains_reserved_keldra_segment(&link_write.path.path)
        || crate::key::contains_reserved_keldra_segment(&target.path)
        || link_previous.deleted
        || descriptor.target_path() != target.path
        || !previous_matches(&link_write.expected, Some(link_previous))
        || !matches!(link_write.payload, BuiltInWritePayload::Tombstone)
        || link_participant.intent != put_intent(true)
        || target_participant.intent != read_only_intent()
        || target_participant
            .condition
            .head_version()
            .is_none_or(|version| version.protected_link_descriptor)
    {
        return invalid_builtin("unlink descriptor, target, or sidecar transition is invalid");
    }
    validate_manifest_preconditions(plan)?;
    validate_exact_alias_observations(plan, &[link_write.path.clone()], &target, true)
}

fn validate_link_put_plan(plan: &BuiltInObjectTransactionPlan) -> Result<(), ProgramStoreError> {
    let manifest = &plan.participant_manifest;
    if manifest.objects.len() != 2
        || manifest.governance.len() != 1
        || plan.head_preconditions.len() != 2
        || plan.read_proofs.len() != 1
        || !plan.assertions.is_empty()
        || plan.alias_registries.len() != 1
        || plan.writes.len() != 1
    {
        return invalid_builtin("link put contract cardinality is invalid");
    }
    let write = &plan.writes[0];
    let BuiltInWritePayload::StagedReference {
        blob_hash,
        upload_source_node_id,
        ..
    } = &write.payload
    else {
        return invalid_builtin("link put requires one exact staged reference");
    };
    if *blob_hash == [0; 32] || *upload_source_node_id == 0 {
        return invalid_builtin("link put staged reference identity is invalid");
    }
    let target = participant(&manifest.objects, &write.path)?;
    let alias_proof = &plan.read_proofs[0];
    let alias = manifest
        .objects
        .get(alias_proof.participant_index as usize)
        .ok_or_else(|| invalid_builtin_error("link put alias participant is absent"))?;
    let alias_index = participant_index(&manifest.objects, &alias.path)?;
    let descriptor = validate_protected_link_descriptor(
        &alias_proof.expected,
        read_proof_bytes(plan, alias_index, &alias_proof.expected)?,
    )?;
    let BuiltInAliasRegistryAccess::Read {
        target_participant_index,
        expected: Some(registry),
    } = &plan.alias_registries[0]
    else {
        return invalid_builtin("link put omits exact target sidecar");
    };
    let expected_alias_paths = registry
        .aliases
        .iter()
        .map(|path| ObjectPath::new(&target.path.tenant, &target.path.bucket, path))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid_builtin_error(&error))?;
    require_same_governed_bucket(manifest, &[alias, target])?;
    if alias.intent != read_only_intent()
        || target.intent != put_intent(false)
        || !nonzero_version_head(&alias.condition)
        || !nonzero_version_head(&target.condition)
        || participant_index(&manifest.objects, &target.path)? != *target_participant_index
        || registry.validate(&target.path.path).is_err()
        || registry.aliases.binary_search(&alias.path.path).is_err()
        || alias.path == target.path
        || crate::key::contains_reserved_keldra_segment(&alias.path.path)
        || crate::key::contains_reserved_keldra_segment(&target.path.path)
    {
        return invalid_builtin("link put participants or paths are invalid");
    }
    let ProgramPathCondition::Head(target_expected) = &target.condition else {
        unreachable!();
    };
    if alias_proof.participant_index != alias_index
        || alias_proof.expected.deleted
        || descriptor.target_path() != target.path.path
        || write
            .previous_version
            .as_ref()
            .is_some_and(|version| version.protected_link_descriptor)
        || write.expected != *target_expected
        || !previous_matches(target_expected, write.previous_version.as_ref())
        || write
            .previous_version
            .as_ref()
            .is_none_or(|version| version.deleted)
    {
        return invalid_builtin("link put descriptor or destination binding is invalid");
    }
    validate_manifest_preconditions(plan)?;
    validate_exact_alias_observations(plan, &expected_alias_paths, &target.path, false)
}

fn validate_link_put_immutable_plan(
    plan: &BuiltInObjectTransactionPlan,
) -> Result<(), ProgramStoreError> {
    let manifest = &plan.participant_manifest;
    if manifest.objects.len() != 2
        || manifest.governance.len() != 1
        || plan.head_preconditions.len() != 2
        || plan.read_proofs.len() != 1
        || plan.assertions.len() != 1
        || plan.alias_registries.len() != 1
        || !plan.alias_observations.is_empty()
        || !plan.writes.is_empty()
    {
        return invalid_builtin("link PutImmutable contract cardinality is invalid");
    }
    let BuiltInTransactionAssertion::PutImmutableMatches {
        target_participant_index,
        blob_hash,
        blob_length,
        content_type,
        upload_source_node_id,
    } = &plan.assertions[0]
    else {
        return invalid_builtin("PutImmutable payload assertion is absent");
    };
    let target = manifest
        .objects
        .get(*target_participant_index as usize)
        .ok_or_else(|| invalid_builtin_error("PutImmutable target participant is absent"))?;
    let ProgramPathCondition::HeadVersion {
        expected: target_version,
    } = &target.condition
    else {
        return invalid_builtin("PutImmutable target does not bind its exact head version");
    };
    if target_version.deleted
        || target_version
            .blob
            .as_ref()
            .is_none_or(|blob| blob.hash != *blob_hash || blob.length != *blob_length)
        || target_version.content_type != *content_type
        || *upload_source_node_id == 0
        || target.intent != read_only_intent()
        || crate::key::contains_reserved_keldra_segment(&target.path.path)
    {
        return invalid_builtin("PutImmutable assertion differs from exact target payload");
    }
    let alias_proof = &plan.read_proofs[0];
    let alias = manifest
        .objects
        .get(alias_proof.participant_index as usize)
        .ok_or_else(|| invalid_builtin_error("PutImmutable alias participant is absent"))?;
    let descriptor = validate_protected_link_descriptor(
        &alias_proof.expected,
        read_proof_bytes(plan, alias_proof.participant_index, &alias_proof.expected)?,
    )?;
    let BuiltInAliasRegistryAccess::Read {
        target_participant_index: registry_target,
        expected: Some(registry),
    } = &plan.alias_registries[0]
    else {
        return invalid_builtin("PutImmutable omits exact target sidecar");
    };
    require_same_governed_bucket(manifest, &[alias, target])?;
    if alias.intent != read_only_intent()
        || descriptor.target_path() != target.path.path
        || target_version.protected_link_descriptor
        || *registry_target != *target_participant_index
        || registry.validate(&target.path.path).is_err()
        || registry.aliases.binary_search(&alias.path.path).is_err()
        || crate::key::contains_reserved_keldra_segment(&alias.path.path)
        || alias.path == target.path
    {
        return invalid_builtin("PutImmutable alias provenance is invalid");
    }
    if manifest
        .objects
        .iter()
        .any(|participant| participant.condition.head_version().is_none())
    {
        return invalid_builtin("PutImmutable exact preconditions are invalid");
    }
    validate_manifest_preconditions(plan)
}

fn validate_manifest_preconditions(
    plan: &BuiltInObjectTransactionPlan,
) -> Result<(), ProgramStoreError> {
    let expected_preconditions = plan
        .participant_manifest
        .objects
        .iter()
        .filter_map(|participant| {
            participant
                .condition
                .observed_head()
                .map(|expected| HeadPrecondition {
                    path: participant.path.clone(),
                    expected,
                })
        })
        .collect::<Vec<_>>();
    if plan.head_preconditions != expected_preconditions {
        return invalid_builtin("head preconditions differ from manifest");
    }
    Ok(())
}

fn validate_exact_alias_observations(
    plan: &BuiltInObjectTransactionPlan,
    requested_paths: &[ObjectPath],
    canonical_path: &ObjectPath,
    deleted: bool,
) -> Result<(), ProgramStoreError> {
    let canonical_index = participant_index(&plan.participant_manifest.objects, canonical_path)?;
    if plan.alias_observations.len() != requested_paths.len()
        || plan
            .alias_observations
            .iter()
            .zip(requested_paths)
            .any(|(observation, requested)| {
                observation.requested_path != *requested
                    || observation.canonical_participant_index != canonical_index
                    || observation.deleted != deleted
                    || observation.requested_path == *canonical_path
                    || observation.requested_path.tenant != canonical_path.tenant
                    || observation.requested_path.bucket != canonical_path.bucket
                    || crate::key::contains_reserved_keldra_segment(
                        &observation.requested_path.path,
                    )
            })
    {
        return invalid_builtin("alias observations differ from canonical provenance");
    }
    Ok(())
}

fn participant<'a>(
    objects: &'a [ProgramObjectParticipant],
    path: &ObjectPath,
) -> Result<&'a ProgramObjectParticipant, ProgramStoreError> {
    objects
        .iter()
        .find(|participant| participant.path == *path)
        .ok_or_else(|| invalid_builtin_error("link participant set is incomplete"))
}

fn participant_index(
    objects: &[ProgramObjectParticipant],
    path: &ObjectPath,
) -> Result<u32, ProgramStoreError> {
    objects
        .iter()
        .position(|participant| participant.path == *path)
        .and_then(|index| u32::try_from(index).ok())
        .ok_or_else(|| invalid_builtin_error("built-in proof participant is absent"))
}

fn read_proof_bytes<'a>(
    plan: &'a BuiltInObjectTransactionPlan,
    participant_index: u32,
    version: &Version,
) -> Result<&'a [u8], ProgramStoreError> {
    let mut matching = plan
        .read_proofs
        .iter()
        .filter(|proof| proof.participant_index == participant_index);
    let proof = matching
        .next()
        .ok_or_else(|| invalid_builtin_error("required built-in read proof is absent"))?;
    let participant = plan
        .participant_manifest
        .objects
        .get(participant_index as usize)
        .ok_or_else(|| invalid_builtin_error("built-in read proof participant is absent"))?;
    if matching.next().is_some()
        || proof.expected != *version
        || version.deleted
        || !matches!(
            &participant.condition,
            ProgramPathCondition::Head(ObservedHead::Version { version: head })
                if head.parse::<u64>().ok() == Some(version.id.0)
        ) && !matches!(
            &participant.condition,
            ProgramPathCondition::HeadVersion { expected } if expected == version
        )
        || version.blob.as_ref().is_none_or(|blob| {
            blob.hash != *blake3::hash(&proof.bytes).as_bytes()
                || blob.length != proof.bytes.len() as u64
        })
    {
        return invalid_builtin("built-in read proof differs from its exact version");
    }
    Ok(&proof.bytes)
}

fn require_same_governed_bucket(
    manifest: &ProgramParticipantManifest,
    objects: &[&ProgramObjectParticipant],
) -> Result<(), ProgramStoreError> {
    let governance = manifest
        .governance
        .first()
        .ok_or_else(|| invalid_builtin_error("governance participant is absent"))?;
    if manifest.governance.len() != 1
        || objects.iter().any(|object| {
            object.path.tenant != governance.tenant
                || object.path.bucket != governance.bucket
                || object.tenant_id != governance.tenant_id
                || object.bucket_id != governance.bucket_id
        })
    {
        return invalid_builtin("participants do not share exact governance");
    }
    Ok(())
}

const fn read_only_intent() -> ProgramParticipantIntent {
    ProgramParticipantIntent {
        read: true,
        put: false,
        delete: false,
    }
}

const fn put_intent(delete: bool) -> ProgramParticipantIntent {
    ProgramParticipantIntent {
        read: true,
        put: true,
        delete,
    }
}

fn nonzero_version_head(condition: &ProgramPathCondition) -> bool {
    matches!(condition,
        ProgramPathCondition::Head(ObservedHead::Version { version })
            if version.parse::<u64>().is_ok_and(|value| value != 0))
}

fn previous_matches(expected: &ObservedHead, previous: Option<&Version>) -> bool {
    match (expected, previous) {
        (ObservedHead::NeverExisted, None) => true,
        (ObservedHead::Version { version }, Some(previous)) => {
            version.parse::<u64>().ok() == Some(previous.id.0)
        }
        _ => false,
    }
}

fn invalid_builtin<T>(message: &str) -> Result<T, ProgramStoreError> {
    Err(invalid_builtin_error(message))
}

fn invalid_builtin_error(message: &str) -> ProgramStoreError {
    ProgramStoreError::InvalidBundle(format!("built-in contract violation: {message}"))
}

pub(super) fn validate_atomic_delivery_bound(
    record: &StoredPreparedBundle,
) -> Result<(), ProgramStoreError> {
    let aliases = prepared_alias_publications(record)?;
    let physical_count = record
        .writes
        .iter()
        .filter(|write| publishes_physical_write(record, &write.path))
        .count();
    let mutation_count = physical_count.checked_add(aliases.len()).ok_or_else(|| {
        ProgramStoreError::InvalidBundle("atomic batch mutation count is exhausted".into())
    })?;
    if mutation_count == 0 {
        return Ok(());
    }
    if mutation_count > crate::MAX_ATOMIC_BATCH_MUTATIONS {
        return Err(ProgramStoreError::InvalidBundle(format!(
            "atomic batch has {mutation_count} published mutations; maximum is {}",
            crate::MAX_ATOMIC_BATCH_MUTATIONS
        )));
    }
    let mut affected_routes = Vec::with_capacity(mutation_count);
    let mut mutations = Vec::with_capacity(mutation_count);
    for (index, write) in record
        .writes
        .iter()
        .filter(|write| publishes_physical_write(record, &write.path))
        .enumerate()
    {
        let ordinal = u64::try_from(index).map_err(|_| {
            ProgramStoreError::InvalidBundle("atomic batch write count is exhausted".into())
        })?;
        let tenant_id = u64::MAX.checked_sub(ordinal).ok_or_else(|| {
            ProgramStoreError::InvalidBundle("atomic batch route identity is exhausted".into())
        })?;
        affected_routes.push(crate::AtomicBatchRoute {
            tenant_id,
            bucket_id: u64::MAX,
        });
        mutations.push(crate::AtomicBatchMutation {
            tenant_id,
            bucket_id: u64::MAX,
            exact_path: write.path.path.clone(),
            canonical_path: None,
            path_version: VersionId(u64::MAX),
            // `false` is the longer JSON spelling and therefore conservative.
            deleted: false,
            source_id: crate::SourceId {
                node_id: u16::MAX,
                source_epoch: [u8::MAX; 32],
            },
            source_journal_position: u64::MAX,
        });
    }
    for alias in aliases {
        affected_routes.push(crate::AtomicBatchRoute {
            tenant_id: alias.identity.tenant_id.0,
            bucket_id: alias.identity.bucket_id.0,
        });
        mutations.push(crate::AtomicBatchMutation {
            tenant_id: alias.identity.tenant_id.0,
            bucket_id: alias.identity.bucket_id.0,
            exact_path: alias.requested_path,
            canonical_path: Some(alias.canonical_path),
            path_version: VersionId(u64::MAX),
            deleted: false,
            source_id: crate::SourceId {
                node_id: u16::MAX,
                source_epoch: [u8::MAX; 32],
            },
            source_journal_position: u64::MAX,
        });
    }
    affected_routes.sort_unstable();
    affected_routes.dedup();
    mutations.sort_unstable();
    let event = crate::LocalChange::atomic_batch_published(
        u64::MAX,
        u64::MAX,
        PreparedBundleHash([u8::MAX; 32]),
        affected_routes,
        mutations,
    );
    let bytes = crate::watch::encoded_change_len(&event).map_err(program_storage_error)?;
    if bytes > crate::MAX_ATOMIC_BATCH_PUBLISHED_BYTES {
        return Err(ProgramStoreError::InvalidBundle(format!(
            "atomic batch publication requires at most {bytes} bytes; maximum is {}",
            crate::MAX_ATOMIC_BATCH_PUBLISHED_BYTES
        )));
    }
    Ok(())
}

pub(super) fn conservative_atomic_source_journal_changes(
    source: &AtomicWriteBundle,
    alias_bindings: &[ProgramAliasBinding],
) -> Result<Vec<crate::LocalChange>, ProgramStoreError> {
    if source.writes.is_empty() {
        return Ok(Vec::new());
    }
    let alias_count = alias_bindings
        .iter()
        .filter(|binding| {
            source
                .writes
                .iter()
                .any(|write| write.path == binding.canonical_path)
        })
        .map(|binding| {
            binding
                .alias_registry
                .as_ref()
                .map_or(0, |registry| registry.aliases.len())
        })
        .sum::<usize>();
    let mutation_count = source
        .writes
        .len()
        .checked_add(alias_count)
        .ok_or_else(|| {
            ProgramStoreError::InvalidBundle("atomic batch count is exhausted".into())
        })?;
    if mutation_count > crate::MAX_ATOMIC_BATCH_MUTATIONS {
        return Err(ProgramStoreError::InvalidBundle(format!(
            "atomic batch has {mutation_count} physical and alias writes; maximum is {}",
            crate::MAX_ATOMIC_BATCH_MUTATIONS
        )));
    }
    let capacity = mutation_count.checked_add(1).ok_or_else(|| {
        ProgramStoreError::InvalidBundle("atomic batch count is exhausted".into())
    })?;
    let mut changes = Vec::with_capacity(capacity);
    let mut routes = Vec::with_capacity(source.writes.len());
    let mut mutations = Vec::with_capacity(source.writes.len());
    for (index, write) in source.writes.iter().enumerate() {
        let ordinal = u64::try_from(index).map_err(|_| {
            ProgramStoreError::InvalidBundle("atomic batch write count is exhausted".into())
        })?;
        let tenant_id = u64::MAX.checked_sub(ordinal).ok_or_else(|| {
            ProgramStoreError::InvalidBundle("atomic batch route identity is exhausted".into())
        })?;
        let bucket_id = u64::MAX;
        let reference = BlobRef {
            hash: [u8::MAX; 32],
            length: u64::MAX,
        };
        changes.push(crate::LocalChange::object_head_with_program_cursor(
            u64::MAX,
            tenant_id,
            bucket_id,
            write.path.path.clone(),
            VersionId(u64::MAX),
            false,
            Some(u64::MAX),
            vec![
                ReferenceDelta {
                    blob: reference.clone(),
                    change: i64::MIN,
                },
                ReferenceDelta {
                    blob: reference,
                    change: i64::MAX,
                },
            ],
            Some(AccountingHeadTransition::new(
                Some(u64::MAX),
                Some(u64::MAX),
            )),
            None,
        ));
        routes.push(crate::AtomicBatchRoute {
            tenant_id,
            bucket_id,
        });
        mutations.push(crate::AtomicBatchMutation {
            tenant_id,
            bucket_id,
            exact_path: write.path.path.clone(),
            canonical_path: None,
            path_version: VersionId(u64::MAX),
            deleted: false,
            source_id: crate::SourceId {
                node_id: u16::MAX,
                source_epoch: [u8::MAX; 32],
            },
            source_journal_position: u64::MAX,
        });
    }
    for binding in alias_bindings.iter().filter(|binding| {
        source
            .writes
            .iter()
            .any(|write| write.path == binding.canonical_path)
    }) {
        for requested_path in binding
            .alias_registry
            .iter()
            .flat_map(|registry| registry.aliases.iter())
        {
            changes.push(crate::LocalChange::alias_object_head_with_program_cursor(
                u64::MAX,
                u64::MAX,
                u64::MAX,
                requested_path.clone(),
                binding.canonical_path.path.clone(),
                VersionId(u64::MAX),
                false,
                Some(u64::MAX),
            ));
            routes.push(crate::AtomicBatchRoute {
                tenant_id: u64::MAX,
                bucket_id: u64::MAX,
            });
            mutations.push(crate::AtomicBatchMutation {
                tenant_id: u64::MAX,
                bucket_id: u64::MAX,
                exact_path: requested_path.clone(),
                canonical_path: Some(binding.canonical_path.path.clone()),
                path_version: VersionId(u64::MAX),
                deleted: false,
                source_id: crate::SourceId {
                    node_id: u16::MAX,
                    source_epoch: [u8::MAX; 32],
                },
                source_journal_position: u64::MAX,
            });
        }
    }
    routes.sort_unstable();
    mutations.sort_unstable();
    changes.push(crate::LocalChange::atomic_batch_published(
        u64::MAX,
        u64::MAX,
        PreparedBundleHash([u8::MAX; 32]),
        routes,
        mutations,
    ));
    Ok(changes)
}

#[cfg(test)]
mod builtin_contract_tests {
    use super::*;

    #[test]
    fn descriptor_mime_without_protected_origin_is_not_alias_provenance() {
        let bytes = crate::ObjectLinkDescriptor::new("target").unwrap().encode();
        let mut version = Version {
            id: VersionId(7),
            blob: Some(BlobRef {
                hash: *blake3::hash(&bytes).as_bytes(),
                length: bytes.len() as u64,
            }),
            content_type: Some(crate::OBJECT_LINK_CONTENT_TYPE.into()),
            deleted: false,
            committed_at_unix_millis: 1,
            protected_link_descriptor: false,
        };
        assert!(validate_protected_link_descriptor(&version, &bytes).is_err());
        version.protected_link_descriptor = true;
        assert!(validate_protected_link_descriptor(&version, &bytes).is_ok());
    }

    #[test]
    fn protected_descriptor_cannot_be_a_canonical_alias_target() {
        let descriptor_bytes = crate::ObjectLinkDescriptor::new("target").unwrap().encode();
        let descriptor_version = Version {
            id: VersionId(7),
            blob: Some(BlobRef {
                hash: *blake3::hash(&descriptor_bytes).as_bytes(),
                length: descriptor_bytes.len() as u64,
            }),
            content_type: Some(crate::OBJECT_LINK_CONTENT_TYPE.into()),
            deleted: false,
            committed_at_unix_millis: 1,
            protected_link_descriptor: true,
        };
        let mut canonical_version = descriptor_version.clone();
        canonical_version.id = VersionId(8);
        let binding = ProgramAliasBinding {
            requested_path: ObjectPath::new("tenant", "bucket", "alias").unwrap(),
            canonical_path: ObjectPath::new("tenant", "bucket", "target").unwrap(),
            descriptor_version: Some(descriptor_version),
            descriptor_bytes: Some(descriptor_bytes),
            canonical_version: Some(canonical_version),
            alias_registry: Some(crate::ObjectAliasRegistry {
                format: crate::OBJECT_ALIAS_REGISTRY_FORMAT,
                revision: 1,
                aliases: vec!["alias".into()],
                program_commit_cursor: Some(1),
            }),
        };

        assert!(matches!(
            binding.validate(),
            Err(ProgramStoreError::InvalidBundle(message))
                if message == "stored-program canonical snapshot is invalid"
        ));
    }

    fn clone_plan() -> BuiltInObjectTransactionPlan {
        let destination = ObjectPath::new("tenant", "bucket", "copy").unwrap();
        let source = ObjectPath::new("tenant", "bucket", "source").unwrap();
        let source_version = Version {
            id: VersionId(1),
            blob: Some(BlobRef {
                hash: [7; 32],
                length: 11,
            }),
            content_type: Some("application/octet-stream".into()),
            deleted: false,
            committed_at_unix_millis: 1,
            protected_link_descriptor: false,
        };
        BuiltInObjectTransactionPlan {
            authority_kind: 1,
            contract_version: 1,
            participant_manifest: ProgramParticipantManifest {
                format: PROGRAM_PARTICIPANT_MANIFEST_FORMAT,
                objects: vec![
                    ProgramObjectParticipant {
                        tenant_id: 1,
                        bucket_id: 2,
                        path: destination.clone(),
                        condition: ProgramPathCondition::Head(ObservedHead::NeverExisted),
                        alias_registry: Some(ProgramAliasRegistryCondition::Absent),
                        intent: put_intent(false),
                    },
                    ProgramObjectParticipant {
                        tenant_id: 1,
                        bucket_id: 2,
                        path: source,
                        condition: ProgramPathCondition::RetainedVersion {
                            expected: source_version.clone(),
                        },
                        alias_registry: None,
                        intent: read_only_intent(),
                    },
                ],
                governance: vec![ProgramGovernanceParticipant {
                    tenant: "tenant".into(),
                    bucket: "bucket".into(),
                    tenant_id: 1,
                    bucket_id: 2,
                    policy: crate::BucketPolicy::default(),
                    versioning: crate::ObjectVersioning::Enabled,
                }],
            },
            head_preconditions: vec![HeadPrecondition {
                path: destination.clone(),
                expected: ObservedHead::NeverExisted,
            }],
            read_proofs: Vec::new(),
            assertions: vec![BuiltInTransactionAssertion::ClonePaths {
                source_requested_path: ObjectPath::new("tenant", "bucket", "source").unwrap(),
                destination_requested_path: destination.clone(),
                source_participant_index: 1,
                destination_participant_index: 0,
            }],
            alias_registries: vec![BuiltInAliasRegistryAccess::Read {
                target_participant_index: 0,
                expected: None,
            }],
            alias_observations: Vec::new(),
            writes: vec![BuiltInVersionWrite {
                path: destination,
                expected: ObservedHead::NeverExisted,
                previous_version: None,
                payload: BuiltInWritePayload::ExistingReference(ExistingReferenceWrite {
                    source_participant_index: 1,
                    blob_hash: [7; 32],
                    blob_length: 11,
                    content_type: Some("application/octet-stream".into()),
                }),
            }],
            receipt: CommandReceipt {
                program_path_hash: [0; 32],
                command_id: "clone-command".into(),
                input_fingerprint: hex::encode([9; 32]),
                outputs: BTreeMap::new(),
            },
        }
    }

    #[test]
    fn clone_contract_rejects_unknown_authority_and_reserved_destination() {
        let plan = clone_plan();
        validate_builtin_plan(&plan).unwrap();

        let mut unknown = plan.clone();
        unknown.authority_kind = 99;
        assert!(matches!(
            validate_builtin_plan(&unknown),
            Err(ProgramStoreError::InvalidBundle(_))
        ));

        let mut reserved = plan;
        let path = ObjectPath::new("tenant", "bucket", "_keldra/copy").unwrap();
        reserved.participant_manifest.objects[0].path = path.clone();
        reserved.head_preconditions[0].path = path.clone();
        reserved.writes[0].path = path;
        assert!(matches!(
            validate_builtin_plan(&reserved),
            Err(ProgramStoreError::InvalidBundle(_))
        ));
    }

    #[test]
    fn prepared_clone_is_exactly_bound_to_sealed_plan_payload() {
        let plan = clone_plan();
        let plan_bytes = serde_json::to_vec(&plan).unwrap();
        let source = plan
            .participant_manifest
            .objects
            .get(1)
            .and_then(|participant| match &participant.condition {
                ProgramPathCondition::RetainedVersion { expected } => Some(expected),
                _ => None,
            })
            .unwrap();
        let mut record = StoredPreparedBundle {
            format: PREPARED_BUNDLE_FORMAT,
            source_bundle_hash: PreparedBundleHash(tagged_hash(
                b"keldra.builtin-object-transaction.v1",
                &plan_bytes,
            )),
            program_hash: ProgramHash([0; 32]),
            authority: ProgramBundleAuthority::BuiltInObjectTransaction {
                kind: 1,
                contract_version: 1,
            },
            participant_manifest: plan.participant_manifest.clone(),
            builtin_plan: Some(plan.clone()),
            alias_bindings: Vec::new(),
            alias_registry_transitions: Vec::new(),
            preconditions: plan.head_preconditions.clone(),
            writes: vec![PreparedVersionWrite {
                path: plan.writes[0].path.clone(),
                expected: plan.writes[0].expected.clone(),
                previous_version: None,
                version: Version {
                    id: VersionId(2),
                    blob: source.blob.clone(),
                    content_type: source.content_type.clone(),
                    deleted: false,
                    committed_at_unix_millis: 2,
                    protected_link_descriptor: false,
                },
            }],
            receipt: plan.receipt,
        };
        validate_builtin_record(&record).unwrap();
        record.writes[0].version.blob.as_mut().unwrap().hash = [8; 32];
        assert!(matches!(
            validate_builtin_record(&record),
            Err(ProgramStoreError::InvalidBundle(_))
        ));
    }
}
