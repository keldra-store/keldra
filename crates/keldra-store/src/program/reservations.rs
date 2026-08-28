use std::collections::{BTreeMap, BTreeSet};

use keldra_atomic_program::{
    AtomicWriteBundle, CommandReceipt, HeadPrecondition, ObjectPath, ObservedHead,
};
use serde::{Deserialize, Serialize};

use crate::{BucketPolicy, ObjectVersioning, PlacementLogId, Store, Version};

use super::{PreparedBundleHash, StoredPreparedBundle};
use super::{ProgramStoreError, program_mutation_error};

pub const PROGRAM_PARTICIPANT_MANIFEST_FORMAT: u16 = 1;
pub const PROGRAM_PATH_RESERVATION_FORMAT: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgramBundleAuthority {
    StoredProgram {
        program_path_hash: [u8; 32],
        program_hash: [u8; 32],
    },
    BuiltInObjectTransaction {
        kind: u16,
        contract_version: u16,
    },
    LegacyProgramOnly {
        program_path_hash: [u8; 32],
        program_hash: [u8; 32],
    },
}

impl ProgramBundleAuthority {
    pub fn validate(self, allow_legacy: bool) -> Result<(), &'static str> {
        match self {
            Self::StoredProgram {
                program_path_hash,
                program_hash,
            } if program_path_hash != [0; 32] && program_hash != [0; 32] => Ok(()),
            Self::BuiltInObjectTransaction {
                kind,
                contract_version,
            } if kind != 0 && contract_version != 0 => Ok(()),
            Self::LegacyProgramOnly {
                program_path_hash,
                program_hash,
            } if allow_legacy && program_path_hash != [0; 32] && program_hash != [0; 32] => Ok(()),
            _ => Err("atomic bundle authority is malformed"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgramPathCondition {
    Head(ObservedHead),
    HeadVersion { expected: Version },
    RetainedVersion { expected: Version },
    HeadAndRetainedVersion { head: Version, retained: Version },
}

impl ProgramPathCondition {
    pub fn observed_head(&self) -> Option<ObservedHead> {
        match self {
            Self::Head(expected) => Some(expected.clone()),
            Self::HeadVersion { expected }
            | Self::HeadAndRetainedVersion { head: expected, .. } => Some(ObservedHead::Version {
                version: expected.id.0.to_string(),
            }),
            Self::RetainedVersion { .. } => None,
        }
    }

    pub fn head_version(&self) -> Option<&Version> {
        match self {
            Self::HeadVersion { expected } => Some(expected),
            Self::HeadAndRetainedVersion { head, .. } => Some(head),
            Self::Head(_) | Self::RetainedVersion { .. } => None,
        }
    }

    pub fn retained_version(&self) -> Option<&Version> {
        match self {
            Self::RetainedVersion { expected }
            | Self::HeadAndRetainedVersion {
                retained: expected, ..
            } => Some(expected),
            Self::Head(_) | Self::HeadVersion { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramParticipantIntent {
    pub read: bool,
    pub put: bool,
    pub delete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramObjectParticipant {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub path: ObjectPath,
    pub condition: ProgramPathCondition,
    pub alias_registry: Option<ProgramAliasRegistryCondition>,
    pub intent: ProgramParticipantIntent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgramAliasRegistryCondition {
    Absent,
    Exact(crate::ObjectAliasRegistry),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramGovernanceParticipant {
    pub tenant: String,
    pub bucket: String,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub policy: BucketPolicy,
    pub versioning: ObjectVersioning,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramParticipantManifest {
    pub format: u16,
    pub objects: Vec<ProgramObjectParticipant>,
    pub governance: Vec<ProgramGovernanceParticipant>,
}

/// Exact logical-to-physical path resolution sealed for one stored-program
/// evaluation. Descriptor bytes bind alias provenance; the target version and
/// target-local registry bind the snapshot and its complete publication fanout.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProgramAliasBinding {
    pub requested_path: ObjectPath,
    pub canonical_path: ObjectPath,
    pub descriptor_version: Option<Version>,
    pub descriptor_bytes: Option<Vec<u8>>,
    pub canonical_version: Option<Version>,
    pub alias_registry: Option<crate::ObjectAliasRegistry>,
}

impl ProgramAliasBinding {
    pub fn validate(&self) -> Result<(), ProgramStoreError> {
        if self.requested_path.tenant != self.canonical_path.tenant
            || self.requested_path.bucket != self.canonical_path.bucket
            || crate::key::contains_reserved_keldra_segment(&self.requested_path.path)
            || crate::key::contains_reserved_keldra_segment(&self.canonical_path.path)
            || self
                .alias_registry
                .as_ref()
                .is_some_and(|registry| registry.validate(&self.canonical_path.path).is_err())
        {
            return Err(ProgramStoreError::InvalidBundle(
                "stored-program alias binding is malformed".into(),
            ));
        }
        match (&self.descriptor_version, &self.descriptor_bytes) {
            (None, None) if self.requested_path == self.canonical_path => {}
            (Some(version), Some(bytes)) if self.requested_path != self.canonical_path => {
                let descriptor =
                    super::validation::validate_protected_link_descriptor(version, bytes)?;
                if descriptor.target_path() != self.canonical_path.path
                    || self.alias_registry.as_ref().is_none_or(|registry| {
                        registry
                            .aliases
                            .binary_search(&self.requested_path.path)
                            .is_err()
                    })
                {
                    return Err(ProgramStoreError::InvalidBundle(
                        "stored-program alias provenance is not exact".into(),
                    ));
                }
            }
            _ => {
                return Err(ProgramStoreError::InvalidBundle(
                    "stored-program alias descriptor shape is invalid".into(),
                ));
            }
        }
        if self.descriptor_version.is_some()
            && self
                .canonical_version
                .as_ref()
                .is_none_or(|version| version.deleted || version.protected_link_descriptor)
            || self
                .canonical_version
                .as_ref()
                .is_some_and(|version| version.protected_link_descriptor)
            || self.canonical_version.is_none() && self.alias_registry.is_some()
        {
            return Err(ProgramStoreError::InvalidBundle(
                "stored-program canonical snapshot is invalid".into(),
            ));
        }
        Ok(())
    }
}

impl ProgramParticipantManifest {
    pub fn encoded(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| error.to_string())
    }

    pub fn hash(&self) -> Result<[u8; 32], String> {
        Ok(*blake3::hash(&self.encoded()?).as_bytes())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.format != PROGRAM_PARTICIPANT_MANIFEST_FORMAT
            || self.objects.is_empty()
            || self
                .objects
                .windows(2)
                .any(|pair| pair[0].path >= pair[1].path)
            || self.governance.windows(2).any(|pair| {
                (&pair[0].tenant, &pair[0].bucket) >= (&pair[1].tenant, &pair[1].bucket)
            })
        {
            return Err("atomic participant manifest is malformed or non-canonical".into());
        }
        for object in &self.objects {
            if object.tenant_id == 0
                || object.bucket_id == 0
                || object.path.tenant.is_empty()
                || (!object.intent.read && !object.intent.put && !object.intent.delete)
            {
                return Err("atomic object participant is malformed".into());
            }
            if let Some(ProgramAliasRegistryCondition::Exact(registry)) = &object.alias_registry
                && registry.validate(&object.path.path).is_err()
            {
                return Err("atomic object participant alias registry is malformed".into());
            }
        }
        for governance in &self.governance {
            if governance.tenant_id == 0 || governance.bucket_id == 0 {
                return Err("atomic governance participant is malformed".into());
            }
            governance
                .policy
                .validate()
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgramReservationState {
    Prepared,
    Committed { commit_cursor: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramPathReservation {
    pub format: u16,
    pub begin_cursor: u64,
    pub invocation_id: [u8; 32],
    pub bundle_hash: [u8; 32],
    pub participant_manifest_hash: [u8; 32],
    pub authority: ProgramBundleAuthority,
    pub executor_node_id: u64,
    pub nomination_log_index: u64,
    pub placement: PlacementLogId,
    pub participant: ProgramObjectParticipant,
    pub state: ProgramReservationState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramGovernanceReservation {
    pub format: u16,
    pub begin_cursor: u64,
    pub invocation_id: [u8; 32],
    pub bundle_hash: [u8; 32],
    pub participant_manifest_hash: [u8; 32],
    pub authority: ProgramBundleAuthority,
    pub executor_node_id: u64,
    pub nomination_log_index: u64,
    pub placement: PlacementLogId,
    pub participant: ProgramGovernanceParticipant,
    pub state: ProgramReservationState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgramReservation {
    Object(ProgramPathReservation),
    Governance(ProgramGovernanceReservation),
}

impl ProgramReservation {
    pub fn begin_cursor(&self) -> u64 {
        match self {
            Self::Object(value) => value.begin_cursor,
            Self::Governance(value) => value.begin_cursor,
        }
    }

    pub fn nomination_log_index(&self) -> u64 {
        match self {
            Self::Object(value) => value.nomination_log_index,
            Self::Governance(value) => value.nomination_log_index,
        }
    }

    pub fn path(&self) -> ObjectPath {
        match self {
            Self::Object(value) => value.participant.path.clone(),
            Self::Governance(value) => ObjectPath::new(
                &value.participant.tenant,
                &value.participant.bucket,
                "_keldra/policy",
            )
            .expect("validated governance participant has canonical names"),
        }
    }

    pub fn stable_bucket_ids(&self) -> (u64, u64) {
        match self {
            Self::Object(value) => (value.participant.tenant_id, value.participant.bucket_id),
            Self::Governance(value) => (value.participant.tenant_id, value.participant.bucket_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExistingReferenceWrite {
    pub source_participant_index: u32,
    pub blob_hash: [u8; 32],
    pub blob_length: u64,
    pub content_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BuiltInWritePayload {
    Inline {
        bytes: Vec<u8>,
        content_type: String,
    },
    ExistingReference(ExistingReferenceWrite),
    /// A blob already staged by the authenticated public write flow. Built-in
    /// preparation proves that the exact content-addressed object is locally
    /// readable before it can enter the sealed prepared bundle; distributed
    /// preparation subsequently stages and attests this reference exactly as
    /// it does every other prepared write blob.
    StagedReference {
        blob_hash: [u8; 32],
        blob_length: u64,
        content_type: Option<String>,
        upload_source_node_id: u64,
    },
    Tombstone,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BuiltInVersionWrite {
    pub path: ObjectPath,
    pub expected: ObservedHead,
    pub previous_version: Option<Version>,
    pub payload: BuiltInWritePayload,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BuiltInReadProof {
    pub participant_index: u32,
    pub expected: Version,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BuiltInTransactionAssertion {
    ClonePaths {
        source_requested_path: ObjectPath,
        destination_requested_path: ObjectPath,
        source_participant_index: u32,
        destination_participant_index: u32,
    },
    PutImmutableMatches {
        target_participant_index: u32,
        blob_hash: [u8; 32],
        blob_length: u64,
        content_type: Option<String>,
        upload_source_node_id: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuiltInAliasRegistryAccess {
    Read {
        target_participant_index: u32,
        expected: Option<crate::ObjectAliasRegistry>,
    },
    Write {
        target_participant_index: u32,
        expected: Option<crate::ObjectAliasRegistry>,
        replacement_aliases: Vec<String>,
    },
}

/// A target-local alias-sidecar transition sealed into a stored-program
/// bundle. Unlike built-in accesses this names the target directly because a
/// stored program has no public contract-specific participant ordinals.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredProgramAliasRegistryTransition {
    pub target: ObjectPath,
    pub expected: crate::ObjectAliasRegistry,
    pub replacement_aliases: Vec<String>,
}

impl StoredProgramAliasRegistryTransition {
    pub fn validate(&self) -> Result<(), String> {
        self.expected
            .validate(&self.target.path)
            .map_err(|error| error.to_string())?;
        if !self
            .expected
            .aliases
            .windows(2)
            .all(|pair| pair[0] < pair[1])
            || self.replacement_aliases.len() >= self.expected.aliases.len()
            || !self
                .replacement_aliases
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self
                .replacement_aliases
                .iter()
                .any(|alias| !self.expected.aliases.contains(alias))
        {
            return Err("stored-program alias transition is malformed".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltInAliasObservation {
    pub requested_path: ObjectPath,
    pub canonical_participant_index: u32,
    pub deleted: bool,
}

/// A sealed, authorized internal transaction plan. Callers supply every read
/// and write participant explicitly; preparation validates that the manifest,
/// exact conditions and payload source are identical to the durable record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BuiltInObjectTransactionPlan {
    pub authority_kind: u16,
    pub contract_version: u16,
    pub participant_manifest: ProgramParticipantManifest,
    pub head_preconditions: Vec<HeadPrecondition>,
    pub read_proofs: Vec<BuiltInReadProof>,
    pub assertions: Vec<BuiltInTransactionAssertion>,
    pub alias_registries: Vec<BuiltInAliasRegistryAccess>,
    pub alias_observations: Vec<BuiltInAliasObservation>,
    pub writes: Vec<BuiltInVersionWrite>,
    pub receipt: CommandReceipt,
}

impl Store {
    pub(crate) fn program_participant_manifest(
        &self,
        source: &AtomicWriteBundle,
        alias_bindings: &[ProgramAliasBinding],
        alias_registry_transitions: &[StoredProgramAliasRegistryTransition],
    ) -> Result<ProgramParticipantManifest, ProgramStoreError> {
        let write_paths = source
            .writes
            .iter()
            .map(|write| write.path.clone())
            .collect::<BTreeSet<_>>();
        let delete_paths = source
            .writes
            .iter()
            .filter(|write| write.value.is_none())
            .map(|write| write.path.clone())
            .collect::<BTreeSet<_>>();
        let mut objects = Vec::with_capacity(source.head_preconditions.len() * 2);
        let mut governance_by_bucket = BTreeMap::new();
        for precondition in &source.head_preconditions {
            let identity = self
                .resolve_bucket_identity(&precondition.path.tenant, &precondition.path.bucket)
                .map_err(program_mutation_error)?;
            governance_by_bucket
                .entry((
                    precondition.path.tenant.clone(),
                    precondition.path.bucket.clone(),
                ))
                .or_insert(ProgramGovernanceParticipant {
                    tenant: precondition.path.tenant.clone(),
                    bucket: precondition.path.bucket.clone(),
                    tenant_id: identity.tenant_id.0,
                    bucket_id: identity.bucket_id.0,
                    policy: self
                        .bucket_policy(&precondition.path.tenant, &precondition.path.bucket)
                        .map_err(program_mutation_error)?,
                    versioning: self
                        .bucket_versioning(&precondition.path.tenant, &precondition.path.bucket)
                        .map_err(program_mutation_error)?,
                });
            let binding = alias_bindings
                .iter()
                .find(|binding| binding.canonical_path == precondition.path);
            let unlinks_alias = alias_registry_transitions
                .iter()
                .any(|transition| transition.target == precondition.path);
            let (condition, alias_registry) = binding.map_or_else(
                || {
                    (
                        ProgramPathCondition::Head(precondition.expected.clone()),
                        None,
                    )
                },
                |binding| {
                    let condition = binding.canonical_version.as_ref().map_or(
                        ProgramPathCondition::Head(ObservedHead::NeverExisted),
                        |version| ProgramPathCondition::HeadVersion {
                            expected: version.clone(),
                        },
                    );
                    let registry = Some(match &binding.alias_registry {
                        Some(registry) => ProgramAliasRegistryCondition::Exact(registry.clone()),
                        None => ProgramAliasRegistryCondition::Absent,
                    });
                    (condition, registry)
                },
            );
            if condition.observed_head().as_ref() != Some(&precondition.expected) {
                return Err(ProgramStoreError::InvalidBundle(
                    "stored-program canonical snapshot differs from evaluation precondition".into(),
                ));
            }
            objects.push(ProgramObjectParticipant {
                tenant_id: identity.tenant_id.0,
                bucket_id: identity.bucket_id.0,
                path: precondition.path.clone(),
                condition,
                alias_registry,
                intent: ProgramParticipantIntent {
                    read: true,
                    put: write_paths.contains(&precondition.path) && !unlinks_alias,
                    delete: delete_paths.contains(&precondition.path) && !unlinks_alias,
                },
            });
        }
        for binding in alias_bindings {
            binding.validate()?;
            let Some(descriptor) = &binding.descriptor_version else {
                continue;
            };
            let identity = self
                .resolve_bucket_identity(
                    &binding.requested_path.tenant,
                    &binding.requested_path.bucket,
                )
                .map_err(program_mutation_error)?;
            objects.push(ProgramObjectParticipant {
                tenant_id: identity.tenant_id.0,
                bucket_id: identity.bucket_id.0,
                path: binding.requested_path.clone(),
                condition: ProgramPathCondition::HeadVersion {
                    expected: descriptor.clone(),
                },
                alias_registry: None,
                intent: ProgramParticipantIntent {
                    read: true,
                    put: alias_registry_transitions.iter().any(|transition| {
                        transition.target == binding.canonical_path
                            && !transition
                                .replacement_aliases
                                .contains(&binding.requested_path.path)
                    }),
                    delete: alias_registry_transitions.iter().any(|transition| {
                        transition.target == binding.canonical_path
                            && !transition
                                .replacement_aliases
                                .contains(&binding.requested_path.path)
                    }),
                },
            });
        }
        objects.sort_by(|left, right| left.path.cmp(&right.path));
        let manifest = ProgramParticipantManifest {
            format: PROGRAM_PARTICIPANT_MANIFEST_FORMAT,
            objects,
            governance: governance_by_bucket.into_values().collect(),
        };
        manifest
            .validate()
            .map_err(ProgramStoreError::InvalidBundle)?;
        Ok(manifest)
    }
}

impl StoredPreparedBundle {
    #[allow(clippy::too_many_arguments)]
    pub fn reservations(
        &self,
        begin_cursor: u64,
        invocation_id: [u8; 32],
        bundle_hash: PreparedBundleHash,
        executor_node_id: u64,
        nomination_log_index: u64,
        placement: PlacementLogId,
    ) -> Result<Vec<ProgramReservation>, ProgramStoreError> {
        validate_reservation_inputs(
            begin_cursor,
            invocation_id,
            bundle_hash,
            executor_node_id,
            nomination_log_index,
            placement,
        )?;
        if matches!(
            self.authority,
            ProgramBundleAuthority::LegacyProgramOnly { .. }
        ) {
            return Ok(Vec::new());
        }
        self.participant_manifest
            .validate()
            .map_err(ProgramStoreError::InvalidBundle)?;
        let manifest_hash = self
            .participant_manifest
            .hash()
            .map_err(ProgramStoreError::InvalidBundle)?;
        let common = |participant| ProgramPathReservation {
            format: PROGRAM_PATH_RESERVATION_FORMAT,
            begin_cursor,
            invocation_id,
            bundle_hash: bundle_hash.0,
            participant_manifest_hash: manifest_hash,
            authority: self.authority,
            executor_node_id,
            nomination_log_index,
            placement,
            participant,
            state: ProgramReservationState::Prepared,
        };
        let mut reservations = self
            .participant_manifest
            .objects
            .iter()
            .cloned()
            .map(|participant| ProgramReservation::Object(common(participant)))
            .collect::<Vec<_>>();
        reservations.extend(self.participant_manifest.governance.iter().cloned().map(
            |participant| {
                ProgramReservation::Governance(ProgramGovernanceReservation {
                    format: PROGRAM_PATH_RESERVATION_FORMAT,
                    begin_cursor,
                    invocation_id,
                    bundle_hash: bundle_hash.0,
                    participant_manifest_hash: manifest_hash,
                    authority: self.authority,
                    executor_node_id,
                    nomination_log_index,
                    placement,
                    participant,
                    state: ProgramReservationState::Prepared,
                })
            },
        ));
        reservations.sort_by_key(ProgramReservation::path);
        Ok(reservations)
    }
}

fn validate_reservation_inputs(
    begin_cursor: u64,
    invocation_id: [u8; 32],
    bundle_hash: PreparedBundleHash,
    executor_node_id: u64,
    nomination_log_index: u64,
    placement: PlacementLogId,
) -> Result<(), ProgramStoreError> {
    if begin_cursor == 0
        || invocation_id == [0; 32]
        || bundle_hash.0 == [0; 32]
        || executor_node_id == 0
        || nomination_log_index == 0
        || placement.term == 0
        || placement.index == 0
    {
        return Err(ProgramStoreError::InvalidBundle(
            "atomic reservation identity is malformed".into(),
        ));
    }
    Ok(())
}
