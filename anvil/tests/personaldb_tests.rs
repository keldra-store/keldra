#![recursion_limit = "512"]

use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::personal_db_service_client::PersonalDbServiceClient;
use anvil::anvil_api::{
    CreatePersonalDbGroupRequest, CreatePersonalDbProjectionRequest, GetPersonalDbGroupRequest,
    GetPersonalDbProjectionRequest, PersonalDbCatchUpRequest, PersonalDbVoterAck,
    SubmitPersonalDbChangesetRequest, WatchPersonalDbGroupRequest,
    WatchPersonalDbProjectionRequest, WriteAuthzTupleRequest,
};
use anvil::anvil_personaldb_sqlite_changeset::iterate_changeset;
use anvil::authz_scope::DEFAULT_AUTHZ_REALM_ID;
use anvil::formats::hash32;
use anvil::partition_fence::{
    AcquireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal, OwnershipResource,
    OwnershipResourceKind, acquire_ownership, read_ownership_fence, read_partition_owner,
};
use anvil::personaldb_envelope::{
    PersonalDbEnvelopeDerivationInput, derive_verified_mutation_envelope,
};
use anvil::personaldb_projection::{
    ColumnMapping, ProjectionDefinition, ProjectionResourceBinding, RowFilter, TableMapping,
    WriteBackPolicy,
};
use anvil::personaldb_row_index::{personaldb_row_index_data_id, read_personaldb_row_index};
use anvil::personaldb_snapshot_store::{
    read_personaldb_snapshot_manifest_by_ref, read_personaldb_snapshot_object,
};
use anvil::personaldb_watch::{
    PersonalDbGroupWatchPayload, PersonalDbProjectionWatchPayload,
    append_personaldb_group_watch_record, append_personaldb_projection_watch_record,
};
use anvil_test_utils::*;
use futures_util::StreamExt;
use rusqlite::{Connection, session::Session};
use std::time::Duration;
use tonic::{Code, Request};

const PERSONALDB_TEST_SCHEMA_SQL: &str = "CREATE TABLE items(
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    payload BLOB
);";

const PERSONALDB_PROJECTION_TEST_SCHEMA_SQL: &str = "CREATE TABLE items_projection(
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
);";

fn personaldb_test_schema_hash() -> String {
    hex::encode(hash32(PERSONALDB_TEST_SCHEMA_SQL.as_bytes()))
}

fn personaldb_projection_test_schema_hash() -> String {
    hex::encode(hash32(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL.as_bytes()))
}

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

async fn create_personaldb_test_actor(
    cluster: &DockerTestCluster,
    label: &str,
) -> DockerTestStorageActor {
    create_docker_storage_test_actor(cluster, label).await
}

async fn grant_default_authz_tuple_writer(
    cluster: &DockerTestCluster,
    actor: &DockerTestStorageActor,
) {
    cluster
        .grant_application_policy(
            actor.tenant_id,
            &actor.app_name,
            "authz:tuple_write",
            DEFAULT_AUTHZ_REALM_ID,
        )
        .await;
}

#[path = "personaldb_tests/groups_and_commits.rs"]
mod groups_and_commits;
#[path = "personaldb_tests/projections_and_writeback.rs"]
mod projections_and_writeback;
#[path = "personaldb_tests/watch_and_reserved_events.rs"]
mod watch_and_reserved_events;

fn valid_submit_request(
    database_id: &str,
    genesis_hash: &str,
    session_token: &str,
) -> SubmitPersonalDbChangesetRequest {
    let changeset_bytes = sqlite_insert_changeset();
    submit_request(database_id, genesis_hash, session_token, changeset_bytes)
}

fn valid_submit_request_for_actor(
    actor: &DockerTestStorageActor,
    database_id: &str,
    genesis_hash: &str,
) -> SubmitPersonalDbChangesetRequest {
    submit_request_for_actor(actor, database_id, genesis_hash, sqlite_insert_changeset())
}

async fn create_group(
    client: &mut PersonalDbServiceClient<tonic::transport::Channel>,
    token: &str,
    database_id: &str,
) -> String {
    create_group_with_schema(
        client,
        token,
        database_id,
        PERSONALDB_TEST_SCHEMA_SQL,
        &personaldb_test_schema_hash(),
    )
    .await
}

async fn create_group_with_schema(
    client: &mut PersonalDbServiceClient<tonic::transport::Channel>,
    token: &str,
    database_id: &str,
    schema_sql: &str,
    schema_hash: &str,
) -> String {
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));
    client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.to_string(),
                schema_hash: schema_hash.to_string(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: schema_sql.to_string(),
            },
            token,
        ))
        .await
        .unwrap();
    genesis_hash
}

fn personaldb_group_partition_id_for_test(tenant_id: i64, database_id: &str) -> String {
    hex::encode(hash32(
        format!("personaldb_group\0{tenant_id}\0{database_id}").as_bytes(),
    ))
}

fn projection_definition(
    projection_database_id: &str,
    source_database_id: &str,
) -> ProjectionDefinition {
    projection_definition_for_tenant(1, projection_database_id, source_database_id)
}

fn projection_definition_for_tenant(
    tenant_id: i64,
    projection_database_id: &str,
    source_database_id: &str,
) -> ProjectionDefinition {
    ProjectionDefinition {
        format_version: 1,
        tenant_id: tenant_id.to_string(),
        database_id: projection_database_id.to_string(),
        projection_id: "projection-items".to_string(),
        source_database_ids: vec![source_database_id.to_string()],
        target_database_id: projection_database_id.to_string(),
        target_actor_or_scope: "scope-primary".to_string(),
        table_mappings: vec![TableMapping {
            source_database_id: source_database_id.to_string(),
            source_table: "items".to_string(),
            target_table: "items_projection".to_string(),
        }],
        column_mappings: vec![
            ColumnMapping {
                source_table: "items".to_string(),
                source_column: "id".to_string(),
                target_table: "items_projection".to_string(),
                target_column: "id".to_string(),
            },
            ColumnMapping {
                source_table: "items".to_string(),
                source_column: "name".to_string(),
                target_table: "items_projection".to_string(),
                target_column: "name".to_string(),
            },
        ],
        row_filters: vec![RowFilter::FieldEqualsLiteral {
            table: "items".to_string(),
            field: "id".to_string(),
            literal: "1".to_string(),
        }],
        resource_bindings: vec![ProjectionResourceBinding {
            source_table: "items".to_string(),
            primary_key_column: "id".to_string(),
            resource_type: "items".to_string(),
            resource_id_column: "id".to_string(),
            parent_resource_id_column: None,
        }],
        writeback_policy: WriteBackPolicy::Deny,
        definition_hash: None,
    }
}

fn projection_definition_allowing_name_writeback_for_tenant(
    tenant_id: i64,
    projection_database_id: &str,
    source_database_id: &str,
) -> ProjectionDefinition {
    let mut definition =
        projection_definition_for_tenant(tenant_id, projection_database_id, source_database_id);
    definition.writeback_policy = WriteBackPolicy::AllowMappedColumns {
        protected_columns: vec!["id".to_string()],
        allowed_columns: vec!["name".to_string()],
    };
    definition
}

fn projection_definition_with_resource_filter_for_tenant(
    tenant_id: i64,
    projection_database_id: &str,
    source_database_id: &str,
) -> ProjectionDefinition {
    let mut definition =
        projection_definition_for_tenant(tenant_id, projection_database_id, source_database_id);
    definition.row_filters = vec![RowFilter::ResourceRelationAllows {
        table: "items".to_string(),
        resource_id_field: "id".to_string(),
        relation: "viewer".to_string(),
    }];
    definition
}

fn projection_definition_allowing_id_name_writeback_for_tenant(
    tenant_id: i64,
    projection_database_id: &str,
    source_database_id: &str,
) -> ProjectionDefinition {
    let mut definition =
        projection_definition_for_tenant(tenant_id, projection_database_id, source_database_id);
    definition.writeback_policy = WriteBackPolicy::AllowMappedColumns {
        protected_columns: Vec::new(),
        allowed_columns: vec!["id".to_string(), "name".to_string()],
    };
    definition
}

fn projection_definition_with_ambiguous_writeback_for_tenant(
    tenant_id: i64,
    projection_database_id: &str,
    first_source_database_id: &str,
    second_source_database_id: &str,
) -> ProjectionDefinition {
    let mut definition = projection_definition_allowing_name_writeback_for_tenant(
        tenant_id,
        projection_database_id,
        first_source_database_id,
    );
    definition
        .source_database_ids
        .push(second_source_database_id.to_string());
    definition.table_mappings.push(TableMapping {
        source_database_id: second_source_database_id.to_string(),
        source_table: "items".to_string(),
        target_table: "items_projection".to_string(),
    });
    definition
}

fn malformed_submit_request(
    database_id: &str,
    genesis_hash: &str,
    session_token: &str,
) -> SubmitPersonalDbChangesetRequest {
    submit_request(
        database_id,
        genesis_hash,
        session_token,
        b"not a sqlite changeset".to_vec(),
    )
}

fn submit_request(
    database_id: &str,
    genesis_hash: &str,
    session_token: &str,
    changeset_bytes: Vec<u8>,
) -> SubmitPersonalDbChangesetRequest {
    submit_request_at_base(database_id, 0, genesis_hash, session_token, changeset_bytes)
}

fn submit_request_for_actor(
    actor: &DockerTestStorageActor,
    database_id: &str,
    genesis_hash: &str,
    changeset_bytes: Vec<u8>,
) -> SubmitPersonalDbChangesetRequest {
    submit_request_at_base_for_actor(actor, database_id, 0, genesis_hash, changeset_bytes)
}

fn submit_request_at_base(
    database_id: &str,
    base_log_index: u64,
    base_log_hash: &str,
    session_token: &str,
    changeset_bytes: Vec<u8>,
) -> SubmitPersonalDbChangesetRequest {
    submit_request_at_base_for_principal(
        database_id,
        base_log_index,
        base_log_hash,
        "2",
        session_token,
        changeset_bytes,
    )
}

fn submit_request_at_base_for_actor(
    actor: &DockerTestStorageActor,
    database_id: &str,
    base_log_index: u64,
    base_log_hash: &str,
    changeset_bytes: Vec<u8>,
) -> SubmitPersonalDbChangesetRequest {
    submit_request_at_base_for_tenant_and_principal(
        actor.tenant_id,
        database_id,
        base_log_index,
        base_log_hash,
        &actor.app_id,
        &actor.token,
        changeset_bytes,
    )
}

fn submit_request_at_base_for_principal(
    database_id: &str,
    base_log_index: u64,
    base_log_hash: &str,
    principal: &str,
    session_token: &str,
    changeset_bytes: Vec<u8>,
) -> SubmitPersonalDbChangesetRequest {
    submit_request_at_base_for_tenant_and_principal(
        1,
        database_id,
        base_log_index,
        base_log_hash,
        principal,
        session_token,
        changeset_bytes,
    )
}

fn submit_request_at_base_for_tenant_and_principal(
    tenant_id: i64,
    database_id: &str,
    base_log_index: u64,
    base_log_hash: &str,
    principal: &str,
    session_token: &str,
    changeset_bytes: Vec<u8>,
) -> SubmitPersonalDbChangesetRequest {
    SubmitPersonalDbChangesetRequest {
        tenant_id,
        database_id: database_id.to_string(),
        principal: principal.to_string(),
        session_token: session_token.to_string(),
        request_id: format!("request-{principal}-{base_log_index}"),
        idempotency_key: format!("idem-{principal}-{base_log_index}"),
        base_log_index,
        base_log_hash: base_log_hash.to_string(),
        client_log_epoch: base_log_index.saturating_add(1),
        membership_epoch: 1,
        policy_epoch: 1,
        leader_replica_id: "leader-a".to_string(),
        voter_acks: vec![PersonalDbVoterAck {
            replica_id: "replica-a".to_string(),
            log_index: base_log_index.saturating_add(1),
            log_hash: hex::encode([8; 32]),
            signature: "signature-a".to_string(),
        }],
        changeset_payload_hash: hex::encode(hash32(&changeset_bytes)),
        changeset_bytes,
        client_debug_metadata_json: String::new(),
    }
}

fn sqlite_insert_changeset() -> Vec<u8> {
    sqlite_insert_changeset_with_item(1, "alpha", &[1_u8, 2, 3])
}

fn sqlite_insert_changeset_with_item(id: i64, name: &str, payload: &[u8]) -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE items(
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            payload BLOB
        );",
    )
    .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute(
        "INSERT INTO items (id, name, payload) VALUES (?1, ?2, ?3)",
        rusqlite::params![id, name, payload],
    )
    .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}

fn sqlite_item_update_changeset() -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(PERSONALDB_TEST_SCHEMA_SQL).unwrap();
    db.execute(
        "INSERT INTO items (id, name, payload) VALUES (1, 'alpha', x'010203')",
        [],
    )
    .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute("UPDATE items SET name = 'beta' WHERE id = 1", [])
        .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}

fn sqlite_projection_update_changeset() -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    db.execute(
        "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
        [],
    )
    .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute("UPDATE items_projection SET name = 'beta' WHERE id = 1", [])
        .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}

fn sqlite_projection_insert_changeset() -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute(
        "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
        [],
    )
    .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}

fn sqlite_projection_delete_changeset() -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    db.execute(
        "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
        [],
    )
    .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute("DELETE FROM items_projection WHERE id = 1", [])
        .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}

fn sqlite_projection_id_update_changeset() -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    db.execute(
        "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
        [],
    )
    .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute("UPDATE items_projection SET id = 2 WHERE id = 1", [])
        .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}
