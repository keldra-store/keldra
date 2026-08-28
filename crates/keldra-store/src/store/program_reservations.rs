use super::*;
use crate::{
    PlacementLogId, ProgramAliasRegistryCondition, ProgramGovernanceReservation,
    ProgramPathCondition, ProgramPathReservation, ProgramReservation, ProgramReservationState,
};
use keldra_atomic_program::ObservedHead;

const OBJECT_RESERVATION_PREFIX: &[u8] = b"atomic-reservation/object/v1/";
const GOVERNANCE_RESERVATION_PREFIX: &[u8] = b"atomic-reservation/governance/v1/";

impl Store {
    pub fn program_reservations(&self) -> Result<Vec<ProgramReservation>, MutationError> {
        let mut reservations = Vec::new();
        for (prefix, governance) in [
            (OBJECT_RESERVATION_PREFIX, false),
            (GOVERNANCE_RESERVATION_PREFIX, true),
        ] {
            for entry in self.db.iterator_cf(
                self.cf(CF_METADATA)?,
                IteratorMode::From(prefix, Direction::Forward),
            ) {
                let (key, value) = entry.map_err(storage_error)?;
                if !key.starts_with(prefix) {
                    break;
                }
                reservations.push(if governance {
                    ProgramReservation::Governance(
                        serde_json::from_slice(&value).map_err(storage_error)?,
                    )
                } else {
                    ProgramReservation::Object(
                        serde_json::from_slice(&value).map_err(storage_error)?,
                    )
                });
            }
        }
        reservations.sort_by_key(ProgramReservation::path);
        Ok(reservations)
    }

    pub async fn reserve_program_participant(
        &self,
        reservation: &ProgramReservation,
    ) -> Result<(), MutationError> {
        match reservation {
            ProgramReservation::Object(value) => self.reserve_program_path(value).await,
            ProgramReservation::Governance(value) => self.reserve_program_governance(value).await,
        }
    }

    pub async fn commit_program_participant(
        &self,
        reservation: &ProgramReservation,
        commit_cursor: u64,
    ) -> Result<ProgramReservation, MutationError> {
        match reservation {
            ProgramReservation::Object(value) => self
                .commit_program_path_reservation(value, commit_cursor)
                .await
                .map(ProgramReservation::Object),
            ProgramReservation::Governance(value) => self
                .commit_program_governance_reservation(value, commit_cursor)
                .await
                .map(ProgramReservation::Governance),
        }
    }

    pub async fn release_program_participant(
        &self,
        reservation: &ProgramReservation,
        finalized_commit_cursor: Option<u64>,
    ) -> Result<(), MutationError> {
        match reservation {
            ProgramReservation::Object(value) => {
                self.release_program_path_reservation(value, finalized_commit_cursor)
                    .await
            }
            ProgramReservation::Governance(value) => {
                self.release_program_governance_reservation(value, finalized_commit_cursor)
                    .await
            }
        }
    }

    pub async fn reserve_program_path(
        &self,
        reservation: &ProgramPathReservation,
    ) -> Result<(), MutationError> {
        validate_path_reservation(reservation)?;
        let identity = reservation_identity(reservation);
        let key = ObjectKey::new(
            &reservation.participant.path.tenant,
            &reservation.participant.path.bucket,
            &reservation.participant.path.path,
        )
        .map_err(|error| MutationError::InvalidObjectMutation(error.to_string()))?;
        if self.resolve_bucket_identity(key.tenant(), key.bucket())? != identity {
            return Err(MutationError::InvalidObjectMutation(
                "atomic participant stable bucket identity does not match its path".into(),
            ));
        }
        let _path_guard = self
            .ordinary_locks
            .acquire(&[reservation.participant.path.clone()])
            .await;
        let _commit_guard = self.lock_commit("atomic_path_reservation").await;
        let policy = self
            .bucket_policy_by_key(&identity.encode())?
            .unwrap_or_default();
        if matches!(
            reservation.authority,
            crate::ProgramBundleAuthority::StoredProgram { .. }
        ) && contains_reserved_keldra_segment(key.path())
        {
            return Err(MutationError::InvalidObjectMutation(
                "stored atomic program cannot address a reserved internal path".into(),
            ));
        }
        if policy.is_immutable(key.path())
            && (reservation.participant.intent.delete
                || (reservation.participant.intent.put
                    && !matches!(
                        reservation.participant.condition,
                        ProgramPathCondition::Head(ObservedHead::NeverExisted)
                    )))
        {
            return Err(MutationError::Immutable);
        }
        self.validate_program_path_condition_locked(
            identity,
            &key,
            &reservation.participant.condition,
        )?;
        let current_alias_registry = self.alias_registry_locked(identity, key.path())?;
        if reservation.participant.intent.delete && current_alias_registry.is_some() {
            return Err(MutationError::ObjectHasInboundAliases);
        }
        if let Some(expected) = &reservation.participant.alias_registry {
            let matches = match expected {
                ProgramAliasRegistryCondition::Absent => current_alias_registry.is_none(),
                ProgramAliasRegistryCondition::Exact(expected) => {
                    current_alias_registry.as_ref() == Some(expected)
                }
            };
            if !matches {
                return Err(MutationError::AtomicReservationConflict {
                    begin_cursor: reservation.begin_cursor,
                });
            }
        }
        let storage_key = object_reservation_key(identity, key.path());
        if let Some(existing) =
            self.read_json::<ProgramPathReservation>(CF_METADATA, &storage_key)?
        {
            require_refreshable_path_reservation(&existing, reservation)?;
            if matches!(existing.state, ProgramReservationState::Committed { .. }) {
                if reservation.nomination_log_index > existing.nomination_log_index {
                    let mut rebound = existing;
                    rebound.executor_node_id = reservation.executor_node_id;
                    rebound.nomination_log_index = reservation.nomination_log_index;
                    self.write_reservation(&storage_key, &rebound)?;
                }
                return Ok(());
            }
        }
        self.write_reservation(&storage_key, reservation)
    }

    pub async fn reserve_program_governance(
        &self,
        reservation: &ProgramGovernanceReservation,
    ) -> Result<(), MutationError> {
        validate_governance_reservation(reservation)?;
        let path = ObjectPath::new(
            &reservation.participant.tenant,
            &reservation.participant.bucket,
            "_keldra/policy",
        )
        .map_err(MutationError::InvalidPolicy)?;
        let identity = governance_identity(reservation);
        if self.resolve_bucket_identity(
            &reservation.participant.tenant,
            &reservation.participant.bucket,
        )? != identity
        {
            return Err(MutationError::InvalidPolicy(
                "atomic governance stable bucket identity does not match its name".into(),
            ));
        }
        let _policy_guard = self.policy_gate.read().await;
        let _path_guard = self.ordinary_locks.acquire(&[path]).await;
        let _commit_guard = self.lock_commit("atomic_governance_reservation").await;
        if self
            .bucket_policy_by_key(&identity.encode())?
            .unwrap_or_default()
            != reservation.participant.policy
            || self.bucket_versioning_by_key(&identity.encode())?
                != reservation.participant.versioning
        {
            return Err(MutationError::PreconditionFailed { current: None });
        }
        let storage_key = governance_reservation_key(identity);
        if let Some(existing) =
            self.read_json::<ProgramGovernanceReservation>(CF_METADATA, &storage_key)?
        {
            require_refreshable_governance_reservation(&existing, reservation)?;
            if matches!(existing.state, ProgramReservationState::Committed { .. }) {
                if reservation.nomination_log_index > existing.nomination_log_index {
                    let mut rebound = existing;
                    rebound.executor_node_id = reservation.executor_node_id;
                    rebound.nomination_log_index = reservation.nomination_log_index;
                    self.write_reservation(&storage_key, &rebound)?;
                }
                return Ok(());
            }
        }
        self.write_reservation(&storage_key, reservation)
    }

    pub async fn commit_program_path_reservation(
        &self,
        expected: &ProgramPathReservation,
        commit_cursor: u64,
    ) -> Result<ProgramPathReservation, MutationError> {
        if commit_cursor == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "atomic commit cursor must be non-zero".into(),
            ));
        }
        let identity = reservation_identity(expected);
        let storage_key = object_reservation_key(identity, &expected.participant.path.path);
        let _path_guard = self
            .ordinary_locks
            .acquire(&[expected.participant.path.clone()])
            .await;
        let _commit_guard = self.lock_commit("atomic_reservation_commit").await;
        let mut stored = self
            .read_json::<ProgramPathReservation>(CF_METADATA, &storage_key)?
            .ok_or(MutationError::AtomicReservationConflict {
                begin_cursor: expected.begin_cursor,
            })?;
        require_exact_path_reservation_fence(&stored, expected)?;
        match stored.state {
            ProgramReservationState::Prepared => {
                stored.state = ProgramReservationState::Committed { commit_cursor };
                self.write_reservation(&storage_key, &stored)?;
            }
            ProgramReservationState::Committed {
                commit_cursor: current,
            } if current == commit_cursor => {}
            ProgramReservationState::Committed { .. } => {
                return Err(MutationError::AtomicReservationConflict {
                    begin_cursor: stored.begin_cursor,
                });
            }
        }
        Ok(stored)
    }

    pub async fn commit_program_governance_reservation(
        &self,
        expected: &ProgramGovernanceReservation,
        commit_cursor: u64,
    ) -> Result<ProgramGovernanceReservation, MutationError> {
        if commit_cursor == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "atomic commit cursor must be non-zero".into(),
            ));
        }
        let identity = governance_identity(expected);
        let storage_key = governance_reservation_key(identity);
        let path = ObjectPath::new(
            &expected.participant.tenant,
            &expected.participant.bucket,
            "_keldra/policy",
        )
        .map_err(MutationError::InvalidPolicy)?;
        let _path_guard = self.ordinary_locks.acquire(&[path]).await;
        let _commit_guard = self.lock_commit("atomic_governance_commit").await;
        let mut stored = self
            .read_json::<ProgramGovernanceReservation>(CF_METADATA, &storage_key)?
            .ok_or(MutationError::AtomicReservationConflict {
                begin_cursor: expected.begin_cursor,
            })?;
        require_exact_governance_reservation_fence(&stored, expected)?;
        match stored.state {
            ProgramReservationState::Prepared => {
                stored.state = ProgramReservationState::Committed { commit_cursor };
                self.write_reservation(&storage_key, &stored)?;
            }
            ProgramReservationState::Committed {
                commit_cursor: current,
            } if current == commit_cursor => {}
            ProgramReservationState::Committed { .. } => {
                return Err(MutationError::AtomicReservationConflict {
                    begin_cursor: stored.begin_cursor,
                });
            }
        }
        Ok(stored)
    }

    pub async fn release_program_path_reservation(
        &self,
        expected: &ProgramPathReservation,
        finalized_commit_cursor: Option<u64>,
    ) -> Result<(), MutationError> {
        let identity = reservation_identity(expected);
        let storage_key = object_reservation_key(identity, &expected.participant.path.path);
        let _path_guard = self
            .ordinary_locks
            .acquire(&[expected.participant.path.clone()])
            .await;
        let _commit_guard = self.lock_commit("atomic_reservation_release").await;
        let Some(stored) = self.read_json::<ProgramPathReservation>(CF_METADATA, &storage_key)?
        else {
            return Ok(());
        };
        require_exact_path_reservation_fence(&stored, expected)?;
        require_releasable_reservation_state(
            stored.state,
            finalized_commit_cursor,
            stored.begin_cursor,
        )?;
        self.delete_reservation(&storage_key)
    }

    pub async fn release_program_governance_reservation(
        &self,
        expected: &ProgramGovernanceReservation,
        finalized_commit_cursor: Option<u64>,
    ) -> Result<(), MutationError> {
        let identity = governance_identity(expected);
        let storage_key = governance_reservation_key(identity);
        let path = ObjectPath::new(
            &expected.participant.tenant,
            &expected.participant.bucket,
            "_keldra/policy",
        )
        .map_err(MutationError::InvalidPolicy)?;
        let _path_guard = self.ordinary_locks.acquire(&[path]).await;
        let _commit_guard = self.lock_commit("atomic_governance_release").await;
        let Some(stored) =
            self.read_json::<ProgramGovernanceReservation>(CF_METADATA, &storage_key)?
        else {
            return Ok(());
        };
        require_exact_governance_reservation_fence(&stored, expected)?;
        require_releasable_reservation_state(
            stored.state,
            finalized_commit_cursor,
            stored.begin_cursor,
        )?;
        self.delete_reservation(&storage_key)
    }

    pub(crate) fn require_unreserved_object_locked(
        &self,
        identity: BucketIdentity,
        path: &str,
        allowed_begin_cursor: Option<u64>,
    ) -> Result<(), MutationError> {
        let key = object_reservation_key(identity, path);
        if let Some(reservation) = self.read_json::<ProgramPathReservation>(CF_METADATA, &key)?
            && Some(reservation.begin_cursor) != allowed_begin_cursor
        {
            return Err(MutationError::AtomicReservationConflict {
                begin_cursor: reservation.begin_cursor,
            });
        }
        Ok(())
    }

    pub(crate) fn require_committed_program_reservation_locked(
        &self,
        identity: BucketIdentity,
        path: &str,
        begin_cursor: u64,
        commit_cursor: u64,
        nomination_log_index: u64,
        placement: PlacementLogId,
    ) -> Result<(), MutationError> {
        let key = object_reservation_key(identity, path);
        let reservation = self
            .read_json::<ProgramPathReservation>(CF_METADATA, &key)?
            .ok_or(MutationError::AtomicReservationConflict { begin_cursor })?;
        if reservation.begin_cursor != begin_cursor
            || reservation.nomination_log_index != nomination_log_index
            || reservation.placement != placement
            || !matches!(
                reservation.state,
                ProgramReservationState::Committed { commit_cursor: stored }
                    if stored == commit_cursor
            )
        {
            return Err(MutationError::AtomicReservationConflict {
                begin_cursor: reservation.begin_cursor,
            });
        }
        Ok(())
    }

    pub(crate) fn require_unreserved_governance_locked(
        &self,
        identity: BucketIdentity,
        allowed_begin_cursor: Option<u64>,
    ) -> Result<(), MutationError> {
        let key = governance_reservation_key(identity);
        if let Some(reservation) =
            self.read_json::<ProgramGovernanceReservation>(CF_METADATA, &key)?
            && Some(reservation.begin_cursor) != allowed_begin_cursor
        {
            return Err(MutationError::AtomicReservationConflict {
                begin_cursor: reservation.begin_cursor,
            });
        }
        Ok(())
    }

    pub(crate) fn require_committed_governance_reservation_locked(
        &self,
        identity: BucketIdentity,
        begin_cursor: u64,
        commit_cursor: u64,
        nomination_log_index: u64,
        placement: PlacementLogId,
    ) -> Result<(), MutationError> {
        let key = governance_reservation_key(identity);
        let reservation = self
            .read_json::<ProgramGovernanceReservation>(CF_METADATA, &key)?
            .ok_or(MutationError::AtomicReservationConflict { begin_cursor })?;
        if reservation.begin_cursor != begin_cursor
            || reservation.nomination_log_index != nomination_log_index
            || reservation.placement != placement
            || !matches!(
                reservation.state,
                ProgramReservationState::Committed { commit_cursor: stored }
                    if stored == commit_cursor
            )
        {
            return Err(MutationError::AtomicReservationConflict {
                begin_cursor: reservation.begin_cursor,
            });
        }
        Ok(())
    }

    fn validate_program_path_condition_locked(
        &self,
        identity: BucketIdentity,
        key: &ObjectKey,
        condition: &ProgramPathCondition,
    ) -> Result<(), MutationError> {
        match condition {
            ProgramPathCondition::Head(expected) => {
                let current = self.head_by_storage_key(&identity.head_key(key.path()))?;
                let matches = match (expected, current.as_ref()) {
                    (ObservedHead::NeverExisted, None) => true,
                    (ObservedHead::Version { version }, Some(head)) => {
                        version.parse::<u64>().ok() == Some(head.version.0)
                    }
                    _ => false,
                };
                if !matches {
                    return Err(MutationError::PreconditionFailed {
                        current: current.map(|head| head.version),
                    });
                }
            }
            ProgramPathCondition::RetainedVersion { expected } => {
                let current = self.user_retained_version(identity, key, expected.id)?;
                if current.as_ref() != Some(expected) {
                    return Err(MutationError::PreconditionFailed {
                        current: current.map(|version| version.id),
                    });
                }
            }
            ProgramPathCondition::HeadVersion { expected } => {
                let current_head = self.head_by_storage_key(&identity.head_key(key.path()))?;
                let current = self.user_retained_version(identity, key, expected.id)?;
                if current_head.as_ref().map(|head| head.version) != Some(expected.id)
                    || current.as_ref() != Some(expected)
                {
                    return Err(MutationError::PreconditionFailed {
                        current: current_head.map(|head| head.version),
                    });
                }
            }
            ProgramPathCondition::HeadAndRetainedVersion { head, retained } => {
                let current_head = self.head_by_storage_key(&identity.head_key(key.path()))?;
                let current_head_version = self.user_retained_version(identity, key, head.id)?;
                let current_retained = self.user_retained_version(identity, key, retained.id)?;
                if current_head.as_ref().map(|value| value.version) != Some(head.id)
                    || current_head_version.as_ref() != Some(head)
                    || current_retained.as_ref() != Some(retained)
                {
                    return Err(MutationError::PreconditionFailed {
                        current: current_head.map(|value| value.version),
                    });
                }
            }
        }
        Ok(())
    }

    fn write_reservation<T: Serialize>(
        &self,
        key: &[u8],
        reservation: &T,
    ) -> Result<(), MutationError> {
        let encoded = serde_json::to_vec(reservation).map_err(storage_error)?;
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(self.cf(CF_METADATA)?, key, encoded, &options)
            .map_err(storage_error)
    }

    fn delete_reservation(&self, key: &[u8]) -> Result<(), MutationError> {
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .delete_cf_opt(self.cf(CF_METADATA)?, key, &options)
            .map_err(storage_error)
    }
}

fn require_releasable_reservation_state(
    stored: ProgramReservationState,
    finalized_commit_cursor: Option<u64>,
    begin_cursor: u64,
) -> Result<(), MutationError> {
    match (stored, finalized_commit_cursor) {
        (ProgramReservationState::Prepared, None) => Ok(()),
        (ProgramReservationState::Prepared, Some(_)) => Ok(()),
        (
            ProgramReservationState::Committed {
                commit_cursor: stored,
            },
            Some(finalized),
        ) if stored == finalized => Ok(()),
        _ => Err(MutationError::AtomicReservationConflict { begin_cursor }),
    }
}

fn validate_path_reservation(reservation: &ProgramPathReservation) -> Result<(), MutationError> {
    if reservation.format != crate::PROGRAM_PATH_RESERVATION_FORMAT
        || reservation.begin_cursor == 0
        || reservation.invocation_id == [0; 32]
        || reservation.bundle_hash == [0; 32]
        || reservation.participant_manifest_hash == [0; 32]
        || reservation.executor_node_id == 0
        || reservation.nomination_log_index == 0
        || reservation.placement.term == 0
        || reservation.placement.index == 0
        || reservation.participant.tenant_id == 0
        || reservation.participant.bucket_id == 0
    {
        return Err(MutationError::InvalidObjectMutation(
            "atomic path reservation is malformed".into(),
        ));
    }
    reservation
        .authority
        .validate(false)
        .map_err(|message| MutationError::InvalidObjectMutation(message.into()))
}

fn validate_governance_reservation(
    reservation: &ProgramGovernanceReservation,
) -> Result<(), MutationError> {
    if reservation.format != crate::PROGRAM_PATH_RESERVATION_FORMAT
        || reservation.begin_cursor == 0
        || reservation.invocation_id == [0; 32]
        || reservation.bundle_hash == [0; 32]
        || reservation.participant_manifest_hash == [0; 32]
        || reservation.executor_node_id == 0
        || reservation.nomination_log_index == 0
        || reservation.placement.term == 0
        || reservation.placement.index == 0
    {
        return Err(MutationError::InvalidObjectMutation(
            "atomic governance reservation is malformed".into(),
        ));
    }
    reservation
        .authority
        .validate(false)
        .map_err(|message| MutationError::InvalidObjectMutation(message.into()))
}

fn require_refreshable_path_reservation(
    existing: &ProgramPathReservation,
    requested: &ProgramPathReservation,
) -> Result<(), MutationError> {
    if existing.begin_cursor != requested.begin_cursor
        || existing.invocation_id != requested.invocation_id
        || existing.bundle_hash != requested.bundle_hash
        || existing.participant_manifest_hash != requested.participant_manifest_hash
        || existing.authority != requested.authority
        || existing.participant != requested.participant
        || existing.placement != requested.placement
        || !matches!(requested.state, ProgramReservationState::Prepared)
    {
        return Err(MutationError::AtomicReservationConflict {
            begin_cursor: existing.begin_cursor,
        });
    }
    match existing.state {
        ProgramReservationState::Prepared
            if requested.nomination_log_index < existing.nomination_log_index
                || (requested.nomination_log_index == existing.nomination_log_index
                    && requested.executor_node_id != existing.executor_node_id) =>
        {
            return Err(MutationError::AtomicReservationConflict {
                begin_cursor: existing.begin_cursor,
            });
        }
        ProgramReservationState::Prepared => {}
        ProgramReservationState::Committed { .. }
            if requested.nomination_log_index >= existing.nomination_log_index
                && (requested.nomination_log_index > existing.nomination_log_index
                    || requested.executor_node_id == existing.executor_node_id) => {}
        ProgramReservationState::Committed { .. } => {
            return Err(MutationError::AtomicReservationConflict {
                begin_cursor: existing.begin_cursor,
            });
        }
    }
    Ok(())
}

fn require_refreshable_governance_reservation(
    existing: &ProgramGovernanceReservation,
    requested: &ProgramGovernanceReservation,
) -> Result<(), MutationError> {
    if existing.begin_cursor != requested.begin_cursor
        || existing.invocation_id != requested.invocation_id
        || existing.bundle_hash != requested.bundle_hash
        || existing.participant_manifest_hash != requested.participant_manifest_hash
        || existing.authority != requested.authority
        || existing.participant != requested.participant
        || existing.placement != requested.placement
        || !matches!(requested.state, ProgramReservationState::Prepared)
    {
        return Err(MutationError::AtomicReservationConflict {
            begin_cursor: existing.begin_cursor,
        });
    }
    match existing.state {
        ProgramReservationState::Prepared
            if requested.nomination_log_index < existing.nomination_log_index
                || (requested.nomination_log_index == existing.nomination_log_index
                    && requested.executor_node_id != existing.executor_node_id) =>
        {
            return Err(MutationError::AtomicReservationConflict {
                begin_cursor: existing.begin_cursor,
            });
        }
        ProgramReservationState::Prepared => {}
        ProgramReservationState::Committed { .. }
            if requested.nomination_log_index >= existing.nomination_log_index
                && (requested.nomination_log_index > existing.nomination_log_index
                    || requested.executor_node_id == existing.executor_node_id) => {}
        ProgramReservationState::Committed { .. } => {
            return Err(MutationError::AtomicReservationConflict {
                begin_cursor: existing.begin_cursor,
            });
        }
    }
    Ok(())
}

fn require_exact_path_reservation_fence(
    existing: &ProgramPathReservation,
    expected: &ProgramPathReservation,
) -> Result<(), MutationError> {
    if existing.begin_cursor != expected.begin_cursor
        || existing.invocation_id != expected.invocation_id
        || existing.bundle_hash != expected.bundle_hash
        || existing.participant_manifest_hash != expected.participant_manifest_hash
        || existing.authority != expected.authority
        || existing.executor_node_id != expected.executor_node_id
        || existing.nomination_log_index != expected.nomination_log_index
        || existing.placement != expected.placement
        || existing.participant != expected.participant
    {
        return Err(MutationError::AtomicReservationConflict {
            begin_cursor: existing.begin_cursor,
        });
    }
    Ok(())
}

fn require_exact_governance_reservation_fence(
    existing: &ProgramGovernanceReservation,
    expected: &ProgramGovernanceReservation,
) -> Result<(), MutationError> {
    if existing.begin_cursor != expected.begin_cursor
        || existing.invocation_id != expected.invocation_id
        || existing.bundle_hash != expected.bundle_hash
        || existing.participant_manifest_hash != expected.participant_manifest_hash
        || existing.authority != expected.authority
        || existing.executor_node_id != expected.executor_node_id
        || existing.nomination_log_index != expected.nomination_log_index
        || existing.placement != expected.placement
        || existing.participant != expected.participant
    {
        return Err(MutationError::AtomicReservationConflict {
            begin_cursor: existing.begin_cursor,
        });
    }
    Ok(())
}

fn reservation_identity(reservation: &ProgramPathReservation) -> BucketIdentity {
    BucketIdentity {
        tenant_id: TenantId(reservation.participant.tenant_id),
        bucket_id: BucketId(reservation.participant.bucket_id),
    }
}

fn governance_identity(reservation: &ProgramGovernanceReservation) -> BucketIdentity {
    BucketIdentity {
        tenant_id: TenantId(reservation.participant.tenant_id),
        bucket_id: BucketId(reservation.participant.bucket_id),
    }
}

fn object_reservation_key(identity: BucketIdentity, path: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(OBJECT_RESERVATION_PREFIX.len() + 17 + path.len());
    key.extend_from_slice(OBJECT_RESERVATION_PREFIX);
    key.extend_from_slice(&identity.encode());
    key.extend_from_slice(path.as_bytes());
    key
}

fn governance_reservation_key(identity: BucketIdentity) -> Vec<u8> {
    let mut key = Vec::with_capacity(GOVERNANCE_RESERVATION_PREFIX.len() + 17);
    key.extend_from_slice(GOVERNANCE_RESERVATION_PREFIX);
    key.extend_from_slice(&identity.encode());
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_accepts_abort_or_finalized_proof_without_requiring_commit_replication() {
        assert_eq!(
            require_releasable_reservation_state(ProgramReservationState::Prepared, None, 7),
            Ok(())
        );
        assert_eq!(
            require_releasable_reservation_state(ProgramReservationState::Prepared, Some(11), 7),
            Ok(())
        );
        assert_eq!(
            require_releasable_reservation_state(
                ProgramReservationState::Committed { commit_cursor: 11 },
                Some(11),
                7
            ),
            Ok(())
        );
        assert!(matches!(
            require_releasable_reservation_state(
                ProgramReservationState::Committed { commit_cursor: 11 },
                Some(12),
                7
            ),
            Err(MutationError::AtomicReservationConflict { begin_cursor: 7 })
        ));
    }
}
