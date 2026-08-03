use std::cmp::Ordering;

use personaldb_core::{CommittedHead, DatabaseId};
use personaldb_server_core::ObjectStoreError;

use super::object_store::AnvilPersonalDbObjectStore;

const HEAD_PREFIX: &str = "groups/";
const HEAD_SUFFIX: &str = "/heads/committed.json";

pub(super) fn is_committed_head_key(key: &str) -> bool {
    database_id_from_key(key).is_some()
}

impl AnvilPersonalDbObjectStore {
    /// Keeps PersonalDB's one mutable committed-head pointer monotonic while
    /// retaining the ordinary Anvil object path and CAS implementation.
    pub(super) async fn put_committed_head(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<(), ObjectStoreError> {
        let database_id = database_id_from_key(key)
            .ok_or_else(|| unavailable("PersonalDB committed-head key is malformed"))?;
        let candidate = decode_head(&database_id, &bytes)?;
        let current = self.read_versioned(key).await?;
        if let Some(current_bytes) = current.bytes.as_deref() {
            let current_head = decode_head(&database_id, current_bytes)?;
            match compare_heads(&current_head, &candidate)? {
                Ordering::Equal => return Ok(()),
                Ordering::Greater => {
                    return Err(unavailable(
                        "PersonalDB committed head would move backwards",
                    ));
                }
                Ordering::Less => {}
            }
        }
        if self.put_at_version(key, bytes, current.version).await? {
            Ok(())
        } else {
            Err(unavailable(
                "PersonalDB committed head changed during its conditional write",
            ))
        }
    }
}

fn database_id_from_key(key: &str) -> Option<DatabaseId> {
    let database_id = key.strip_prefix(HEAD_PREFIX)?.strip_suffix(HEAD_SUFFIX)?;
    (!database_id.is_empty()).then(|| DatabaseId::new(database_id))
}

fn decode_head(database_id: &DatabaseId, bytes: &[u8]) -> Result<CommittedHead, ObjectStoreError> {
    let head: CommittedHead = serde_json::from_slice(bytes)
        .map_err(|error| unavailable(format!("decode PersonalDB committed head: {error}")))?;
    if head.database_id != *database_id
        || head.position.index != head.log_index
        || head.position.hash != head.log_hash
    {
        return Err(unavailable(
            "PersonalDB committed head is not bound to its database and log position",
        ));
    }
    Ok(head)
}

/// Equal means exact idempotence. Less means the candidate advances. Greater
/// means regression. An equal index with different content is never ordered.
fn compare_heads(
    current: &CommittedHead,
    candidate: &CommittedHead,
) -> Result<Ordering, ObjectStoreError> {
    match current.log_index.cmp(&candidate.log_index) {
        Ordering::Equal if current == candidate => Ok(Ordering::Equal),
        Ordering::Equal => Err(unavailable(
            "PersonalDB committed head conflicts at the current log index",
        )),
        ordering => Ok(ordering),
    }
}

fn unavailable(message: impl Into<String>) -> ObjectStoreError {
    ObjectStoreError::Unavailable(message.into())
}

#[cfg(test)]
mod tests {
    use personaldb_core::{
        ClientLogEpoch, LogPosition, MembershipEpoch, PlacementEpoch, PolicyEpoch,
    };

    use super::*;

    fn head(index: u64, hash: &str) -> CommittedHead {
        CommittedHead {
            object_layout_version: 0,
            database_id: DatabaseId::new("database"),
            position: LogPosition {
                index,
                hash: hash.into(),
            },
            placement_epoch: PlacementEpoch(1),
            client_log_epoch: ClientLogEpoch(1),
            membership_epoch: MembershipEpoch(1),
            policy_epoch: PolicyEpoch(1),
            log_index: index,
            entry_hash: format!("entry-{index}"),
            log_hash: hash.into(),
        }
    }

    #[test]
    fn canonical_key_binds_the_complete_database_id() {
        assert_eq!(
            database_id_from_key("groups/tenant/path/database/heads/committed.json"),
            Some(DatabaseId::new("tenant/path/database"))
        );
        assert!(database_id_from_key("groups//heads/committed.json").is_none());
        assert!(database_id_from_key("groups/database/heads/other.json").is_none());
    }

    #[test]
    fn head_transition_is_idempotent_advancing_and_never_regressive() {
        let first = head(1, "hash-1");
        let second = head(2, "hash-2");
        assert_eq!(compare_heads(&first, &first).unwrap(), Ordering::Equal);
        assert_eq!(compare_heads(&first, &second).unwrap(), Ordering::Less);
        assert_eq!(compare_heads(&second, &first).unwrap(), Ordering::Greater);

        let conflict = head(1, "another-hash");
        assert!(compare_heads(&first, &conflict).is_err());
    }

    #[test]
    fn head_payload_must_match_its_key_and_position() {
        let mut candidate = head(1, "hash-1");
        let bytes = serde_json::to_vec(&candidate).unwrap();
        assert!(decode_head(&DatabaseId::new("another"), &bytes).is_err());

        candidate.position.hash = "wrong".into();
        let bytes = serde_json::to_vec(&candidate).unwrap();
        assert!(decode_head(&DatabaseId::new("database"), &bytes).is_err());
    }
}
