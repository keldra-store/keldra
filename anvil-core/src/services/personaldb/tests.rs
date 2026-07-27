use super::*;
use crate::formats::hash32;

#[test]
fn public_commits_use_mvcc_assignment_without_legacy_witness_admission() {
    let actor = PersonalDbCommitActor::public(7, "app".into(), "token".into());
    assert!(actor.require_public_commit_authorization);
    assert!(!actor.require_admission_protocol);
}

#[test]
fn watch_cursor_split_round_trips() {
    let value = (37u128 << 64) | 99;
    let (low, high) = split_u128(value);
    assert_eq!(join_u128(low, high), value);
}

#[test]
fn database_id_validation_rejects_path_escape() {
    assert!(validate_database_id("db-alpha").is_ok());
    assert!(validate_database_id("../db").is_err());
    assert!(validate_database_id("tenant/db").is_err());
}

#[test]
fn log_record_hex_encodes_hashes() {
    let record = crate::formats::personaldb::PersonalDbLogRecord::new(
        1,
        1,
        1,
        1,
        [1; 32],
        [2; 32],
        [3; 32],
        [4; 32],
        b"payload".to_vec(),
        b"certificate".to_vec(),
        Vec::new(),
    );
    let encoded = log_record(record);
    assert_eq!(encoded.previous_log_hash, hex::encode([1; 32]));
    assert_eq!(encoded.changeset_payload_hash, hex::encode([2; 32]));
}

#[test]
fn genesis_hash_uses_blake3_hash_format() {
    assert!(validate_hex32(&hex::encode(hash32(b"genesis")), "genesis_hash").is_ok());
}

#[test]
fn snapshot_policy_uses_spec_defaults_for_zero_config_values() {
    let config = crate::config::Config::default();
    let policy = configured_personaldb_snapshot_policy(&config);
    assert_eq!(policy, PersonalDbSnapshotPolicy::default());
}
