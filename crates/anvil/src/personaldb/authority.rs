use personaldb_core::{DatabaseGroupMembership, DatabaseId, LeaderLease};
use personaldb_server_core::ObjectStoreError;
use serde::{Deserialize, Serialize};

use super::object_store::AnvilPersonalDbObjectStore;

const AUTHORITY_FORMAT_VERSION: u32 = 0;
const MAX_AUTHORITY_BYTES: usize = 16 * 1024 * 1024;

/// The one durable authority floor for a PersonalDB group. This is an ordinary
/// Anvil object, not a registry or a second persistence plane.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityRecord {
    format_version: u32,
    membership: DatabaseGroupMembership,
    leader_lease: LeaderLease,
}

pub(crate) struct RecoveredAuthority {
    pub(crate) membership: DatabaseGroupMembership,
    pub(crate) leader_lease: LeaderLease,
}

impl AnvilPersonalDbObjectStore {
    pub(crate) async fn load_authority(
        &self,
        database_id: &DatabaseId,
    ) -> Result<Option<RecoveredAuthority>, ObjectStoreError> {
        let key = authority_key(database_id);
        let Some(bytes) = self.read_versioned(&key).await?.bytes else {
            return Ok(None);
        };
        decode_authority(database_id, &bytes).map(Some)
    }

    pub(crate) async fn persist_authority(
        &self,
        membership: &DatabaseGroupMembership,
        leader_lease: &LeaderLease,
    ) -> Result<(), ObjectStoreError> {
        if membership.database_id != leader_lease.database_id {
            return Err(unavailable(
                "PersonalDB membership and leader lease name different database groups",
            ));
        }
        let record = AuthorityRecord {
            format_version: AUTHORITY_FORMAT_VERSION,
            membership: membership.clone(),
            leader_lease: leader_lease.clone(),
        };
        let bytes = serde_json::to_vec(&record)
            .map_err(|error| unavailable(format!("encode PersonalDB authority: {error}")))?;
        if bytes.len() > MAX_AUTHORITY_BYTES {
            return Err(unavailable(
                "PersonalDB authority exceeds the 16 MiB canonical JSON limit",
            ));
        }
        let key = authority_key(&membership.database_id);
        let current = self.read_versioned(&key).await?;
        if let Some(current_bytes) = current.bytes.as_deref() {
            let current_record = decode_record(&membership.database_id, current_bytes)?;
            let current_generation = generation(&current_record);
            let candidate_generation = generation(&record);
            if current_generation > candidate_generation {
                return Err(unavailable(
                    "PersonalDB authority would move its durable generation backwards",
                ));
            }
            if current_generation == candidate_generation {
                return if current_record == record {
                    Ok(())
                } else {
                    Err(unavailable(
                        "PersonalDB authority conflicts at its durable generation",
                    ))
                };
            }
        }
        if self.put_at_version(&key, bytes, current.version).await? {
            Ok(())
        } else {
            Err(unavailable(
                "PersonalDB authority changed during its conditional write",
            ))
        }
    }
}

fn authority_key(database_id: &DatabaseId) -> String {
    format!("groups/{}/authority/current.json", database_id.0)
}

fn decode_authority(
    expected_database_id: &DatabaseId,
    bytes: &[u8],
) -> Result<RecoveredAuthority, ObjectStoreError> {
    let record = decode_record(expected_database_id, bytes)?;
    Ok(RecoveredAuthority {
        membership: record.membership,
        leader_lease: record.leader_lease,
    })
}

fn decode_record(
    expected_database_id: &DatabaseId,
    bytes: &[u8],
) -> Result<AuthorityRecord, ObjectStoreError> {
    if bytes.is_empty() || bytes.len() > MAX_AUTHORITY_BYTES {
        return Err(unavailable(
            "PersonalDB authority is empty or exceeds the 16 MiB canonical JSON limit",
        ));
    }
    let record: AuthorityRecord = serde_json::from_slice(bytes)
        .map_err(|error| unavailable(format!("decode PersonalDB authority: {error}")))?;
    if record.format_version != AUTHORITY_FORMAT_VERSION {
        return Err(unavailable(format!(
            "unsupported PersonalDB authority format version {}",
            record.format_version
        )));
    }
    if record.membership.database_id != *expected_database_id
        || record.leader_lease.database_id != *expected_database_id
    {
        return Err(unavailable(
            "PersonalDB authority is bound to another database group",
        ));
    }
    Ok(record)
}

fn generation(record: &AuthorityRecord) -> (u64, u64) {
    (
        record.leader_lease.client_log_epoch.0,
        record.leader_lease.lease_generation,
    )
}

fn unavailable(message: impl Into<String>) -> ObjectStoreError {
    ObjectStoreError::Unavailable(message.into())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use personaldb_core::{
        ClientLogEpoch, LogPosition, PlacementEpoch, PolicyEpoch, ReplicaId, ServerId,
    };

    use super::*;

    fn authority(database_id: DatabaseId) -> AuthorityRecord {
        let leader = ReplicaId::new("leader");
        let membership =
            DatabaseGroupMembership::single_replica(database_id.clone(), leader.clone());
        let lease = LeaderLease::grant(
            database_id,
            PlacementEpoch(1),
            ClientLogEpoch(1),
            leader,
            membership.membership_epoch,
            PolicyEpoch(1),
            "voter-set".into(),
            LogPosition::genesis(),
            ServerId::new("server"),
            1,
            Duration::from_secs(30),
        );
        AuthorityRecord {
            format_version: AUTHORITY_FORMAT_VERSION,
            membership,
            leader_lease: lease,
        }
    }

    #[test]
    fn record_is_one_versioned_canonical_json_object() {
        let database_id = DatabaseId::new("database");
        let bytes = serde_json::to_vec(&authority(database_id.clone())).unwrap();
        let decoded = decode_authority(&database_id, &bytes).unwrap();
        assert_eq!(decoded.membership.database_id, database_id);
        assert_eq!(decoded.leader_lease.database_id, database_id);

        let object = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
        let keys = object
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(keys, ["format_version", "leader_lease", "membership"]);
    }

    #[test]
    fn load_rejects_another_database_and_unknown_format() {
        let bytes = serde_json::to_vec(&authority(DatabaseId::new("first"))).unwrap();
        assert!(decode_authority(&DatabaseId::new("second"), &bytes).is_err());

        let mut record = authority(DatabaseId::new("first"));
        record.format_version += 1;
        let bytes = serde_json::to_vec(&record).unwrap();
        assert!(decode_authority(&DatabaseId::new("first"), &bytes).is_err());
    }
}
