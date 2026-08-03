//! Test-only PersonalDB v0 client for the three-node Docker qualification.

use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use anvil_storage::v1::personal_db_service_client::PersonalDbServiceClient;
use anvil_storage::v1::{
    PersonalDbExchangeRequest, PersonalDbExchangeResponse, PersonalDbGrantLeaderLeaseRequest,
    PersonalDbRenewLeaderLeaseRequest, PersonalDbWitnessCommitRequest,
};
use anvil_storage::{BearerToken, connect_channel, exchange_client_credentials};
use personaldb_core::{
    CatchUpRequest, CommittedEntry, ConsistencyPolicy, DatabaseId, LeaderLease, LogPosition,
    MutationPayload, ProposalId, ProposedLogEntry, ReplicaId, SnapshotFormat, SnapshotId,
    SnapshotMetadata, VoterAck, WIRE_PROTOCOL_VERSION_V0,
};
use personaldb_server::{
    CatchUpBatchMessage, CatchUpCompleteMessage, ClientCapabilities, ClientMessage,
    GetSnapshotRequest, HelloRequest, JsonTransportCodec, MessageId, OpenGroupRequest,
    RegisterSnapshotRequest, ServerMessage, SnapshotManifestResponse, TransportCodec,
    TransportLimits, WireMessageKind,
};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type PersonalDbClient = PersonalDbServiceClient<InterceptedService<Channel, BearerToken>>;

const DATABASE_GROUP: &str = "qualification-database-group";
const SNAPSHOT_BYTES: &[u8] = b"personaldb qualification state";
const FIRST_CHANGESET: &str =
    "CREATE TABLE qualification_events(id INTEGER PRIMARY KEY, value TEXT NOT NULL);";
const SECOND_CHANGESET: &str =
    "INSERT INTO qualification_events(id, value) VALUES (1, 'after restart');";
const LEASE_DURATION_MILLIS: u64 = 3_600_000;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct QualificationState {
    schema_version: u32,
    bucket: String,
    database: DatabaseId,
    leader: ReplicaId,
    prior_lease: LeaderLease,
    first_entry: CommittedEntry,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let endpoints = required("ANVIL_PERSONALDB_QUALIFICATION_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if endpoints.len() != 3 {
        return Err(invalid("qualification requires exactly three endpoints"));
    }
    let client_id = required("ANVIL_PERSONALDB_QUALIFICATION_CLIENT_ID")?;
    let client_secret = required("ANVIL_PERSONALDB_QUALIFICATION_CLIENT_SECRET")?;
    let bucket = required("ANVIL_PERSONALDB_QUALIFICATION_BUCKET")?;
    let state_path = PathBuf::from(required("ANVIL_PERSONALDB_QUALIFICATION_STATE_PATH")?);
    let verify_existing = env::var_os("ANVIL_PERSONALDB_QUALIFICATION_VERIFY_EXISTING").is_some();
    let prior_state = if verify_existing {
        Some(load_state(&state_path, &bucket)?)
    } else {
        if state_path.exists() {
            return Err(invalid(format!(
                "qualification state already exists: {}",
                state_path.display()
            )));
        }
        None
    };

    let mut channels = Vec::new();
    for endpoint in &endpoints {
        channels.push(connect_channel(endpoint).await?);
    }
    let token = exchange_client_credentials(channels[0].clone(), client_id, client_secret)
        .await?
        .access_token;
    let mut clients = channels
        .into_iter()
        .map(|channel| personaldb_client(channel, &token))
        .collect::<Result<Vec<_>, tonic::metadata::errors::InvalidMetadataValue>>()?;

    let codec = JsonTransportCodec;
    let limits = TransportLimits::default();
    let database = DatabaseId::new(DATABASE_GROUP);
    let genesis = LogPosition::genesis();
    if prior_state
        .as_ref()
        .is_some_and(|state| state.database != database)
    {
        return Err(invalid(
            "qualification state names a different PersonalDB database group",
        ));
    }
    let expected_open_head = prior_state
        .as_ref()
        .map(|state| state.first_entry.position())
        .unwrap_or_else(|| genesis.clone());
    let mut cluster_servers = BTreeSet::new();
    for (position, client) in clients.iter_mut().enumerate() {
        let messages = exchange(
            client,
            &bucket,
            &codec,
            &limits,
            &format!("personaldb-hello-{position}"),
            ClientMessage::Hello(HelloRequest {
                client_version: "anvil-three-node-qualification/0.5.2".into(),
                supported_protocol_versions: vec![WIRE_PROTOCOL_VERSION_V0],
                replica_id: Some(ReplicaId::new(format!("qualification-replica-{position}"))),
                capabilities: ClientCapabilities::default(),
            }),
        )
        .await?;
        match messages.as_slice() {
            [ServerMessage::Hello(response)]
                if response.selected_protocol_version == WIRE_PROTOCOL_VERSION_V0 =>
            {
                cluster_servers.insert(response.server_id.0.clone());
            }
            other => return Err(unexpected("Hello", other)),
        }
    }
    if cluster_servers.len() != 3 {
        return Err(invalid(format!(
            "three public ingresses did not reach three PersonalDB servers: {cluster_servers:?}"
        )));
    }

    let mut primaries = BTreeSet::new();

    for (position, client) in clients.iter_mut().enumerate() {
        let messages = exchange(
            client,
            &bucket,
            &codec,
            &limits,
            &format!("personaldb-open-{position}"),
            ClientMessage::OpenGroup(OpenGroupRequest {
                database_group: database.clone(),
                replica_id: ReplicaId::new(format!("qualification-replica-{position}")),
                last_applied: Some(genesis.clone()),
                requested_consistency: ConsistencyPolicy::strict_witnessed(),
                client_capabilities: ClientCapabilities::default(),
            }),
        )
        .await?;
        match messages.as_slice() {
            [ServerMessage::OpenGroup(response)]
                if response.database_group == database
                    && response.current_head.as_ref() == Some(&expected_open_head)
                    && response.primary.database_id == database
                    && response.primary.placement_epoch.0 > 0 =>
            {
                primaries.insert(response.primary.primary_server_id.0.clone());
            }
            other => return Err(unexpected("OpenGroup", other)),
        }
    }
    if primaries.len() != 1
        || !primaries
            .first()
            .is_some_and(|primary| cluster_servers.contains(primary))
    {
        return Err(invalid(format!(
            "three ingresses did not resolve one HRW PersonalDB primary: {primaries:?}"
        )));
    }

    if let Some(state) = prior_state {
        require_production_entry(&state.first_entry)?;
        for (position, client) in clients.iter_mut().enumerate() {
            let caught_up = catch_up(
                client,
                &bucket,
                &codec,
                &limits,
                &format!("personaldb-before-replacement-{position}"),
                &database,
                &genesis,
            )
            .await?;
            require_committed_catch_up(
                &caught_up,
                &database,
                &genesis,
                std::slice::from_ref(&state.first_entry),
            )?;
        }

        let replacement = grant_lease(&mut clients[0], &bucket, &database, &state.leader).await?;
        if replacement.database_id != database
            || replacement.leader_replica_id != state.leader
            || replacement.lease_id == state.prior_lease.lease_id
            || replacement.client_log_epoch < state.prior_lease.client_log_epoch
            || (replacement.client_log_epoch == state.prior_lease.client_log_epoch
                && replacement.lease_generation <= state.prior_lease.lease_generation)
            || (replacement.placement_epoch != state.prior_lease.placement_epoch
                && replacement.client_log_epoch <= state.prior_lease.client_log_epoch)
            || replacement.starts_at_log_index != state.first_entry.entry.log_index
            || replacement.starts_after_log_hash != state.first_entry.entry.log_hash
        {
            return Err(invalid(format!(
                "replacement PersonalDB lease did not monotonically supersede prior authority: prior={:?}, replacement={replacement:?}",
                state.prior_lease
            )));
        }

        require_renew_rejected(&mut clients[1], &bucket, &state.prior_lease).await?;
        let stale = proposed_entry(
            &database,
            &state.prior_lease,
            &state.leader,
            2,
            state.first_entry.entry.log_hash.clone(),
            SECOND_CHANGESET,
        );
        let stale_ack = voter_ack(&stale, state.leader.clone())?;
        require_witness_rejected(&mut clients[2], &bucket, &stale, &[stale_ack]).await?;

        let proposed = proposed_entry(
            &database,
            &replacement,
            &state.leader,
            2,
            state.first_entry.entry.log_hash.clone(),
            SECOND_CHANGESET,
        );
        let ack = voter_ack(&proposed, state.leader.clone())?;
        let second_entry = witness_commit(&mut clients[2], &bucket, &proposed, &[ack]).await?;
        require_production_entry(&second_entry)?;
        if second_entry.entry.log_index != 2
            || second_entry.entry.previous_hash != state.first_entry.entry.log_hash
            || second_entry.entry.payload.as_utf8()? != SECOND_CHANGESET
        {
            return Err(invalid(
                "PersonalDB witness returned an invalid post-restart successor",
            ));
        }

        let expected = [state.first_entry, second_entry];
        for (position, client) in clients.iter_mut().enumerate() {
            let caught_up = catch_up(
                client,
                &bucket,
                &codec,
                &limits,
                &format!("personaldb-two-entry-catch-up-{position}"),
                &database,
                &genesis,
            )
            .await?;
            require_committed_catch_up(&caught_up, &database, &genesis, &expected)?;
        }
    } else {
        let leader = ReplicaId::new("qualification-replica-0");
        let granted = grant_lease(&mut clients[0], &bucket, &database, &leader).await?;
        let lease = renew_lease(&mut clients[1], &bucket, &granted).await?;
        if lease.lease_generation <= granted.lease_generation
            || lease.leader_replica_id != leader
            || lease.database_id != database
        {
            return Err(invalid(
                "PersonalDB lease renewal did not preserve authority",
            ));
        }
        let proposed = proposed_entry(
            &database,
            &lease,
            &leader,
            1,
            genesis.hash.clone(),
            FIRST_CHANGESET,
        );
        let ack = voter_ack(&proposed, leader.clone())?;
        let committed = witness_commit(&mut clients[2], &bucket, &proposed, &[ack]).await?;
        if committed.position().index != 1 || committed.entry.payload.as_utf8()? != FIRST_CHANGESET
        {
            return Err(invalid(
                "PersonalDB witness returned the wrong committed entry",
            ));
        }
        require_production_entry(&committed)?;
        let snapshot = SnapshotMetadata::new(
            SnapshotId::new("qualification-snapshot"),
            database.clone(),
            committed.position(),
            "qualification/snapshot.bin".into(),
            SnapshotFormat::ApplicationDefined,
            SNAPSHOT_BYTES,
            1,
        );
        let registered = exchange(
            &mut clients[1],
            &bucket,
            &codec,
            &limits,
            "personaldb-register-snapshot",
            ClientMessage::RegisterSnapshot(RegisterSnapshotRequest {
                database_group: database.clone(),
                manifest: snapshot.clone(),
                producer_replica: ReplicaId::new("qualification-replica-1"),
            }),
        )
        .await?;
        require_snapshot("RegisterSnapshot", &registered, &database, &snapshot)?;

        for (position, client) in clients.iter_mut().enumerate() {
            let loaded = exchange(
                client,
                &bucket,
                &codec,
                &limits,
                &format!("personaldb-get-snapshot-{position}"),
                ClientMessage::GetSnapshot(GetSnapshotRequest {
                    database_group: database.clone(),
                    min_index: Some(1),
                }),
            )
            .await?;
            require_snapshot("GetSnapshot", &loaded, &database, &snapshot)?;

            let caught_up = catch_up(
                client,
                &bucket,
                &codec,
                &limits,
                &format!("personaldb-catch-up-{position}"),
                &database,
                &genesis,
            )
            .await?;
            require_committed_catch_up(
                &caught_up,
                &database,
                &genesis,
                std::slice::from_ref(&committed),
            )?;
        }

        save_state(
            &state_path,
            &QualificationState {
                schema_version: 1,
                bucket: bucket.clone(),
                database: database.clone(),
                leader,
                prior_lease: lease,
                first_entry: committed,
            },
        )?;
    }

    println!(
        "three-node PersonalDB qualification passed: primary={}, exact committed chain visible through 3 ingresses, hydrated_after_restart={verify_existing}",
        primaries.first().expect("one primary was checked")
    );
    Ok(())
}

fn personaldb_client(
    channel: Channel,
    token: &str,
) -> Result<PersonalDbClient, tonic::metadata::errors::InvalidMetadataValue> {
    Ok(
        PersonalDbServiceClient::with_interceptor(channel, BearerToken::new(token)?)
            .max_encoding_message_size(18 * 1024 * 1024)
            .max_decoding_message_size(18 * 1024 * 1024),
    )
}

async fn grant_lease(
    client: &mut PersonalDbClient,
    bucket: &str,
    database: &DatabaseId,
    leader: &ReplicaId,
) -> TestResult<LeaderLease> {
    let response = client
        .grant_leader_lease(PersonalDbGrantLeaderLeaseRequest {
            bucket: bucket.into(),
            database_id_json: serde_json::to_vec(database)?,
            leader_replica_json: serde_json::to_vec(leader)?,
            duration_millis: LEASE_DURATION_MILLIS,
        })
        .await?
        .into_inner();
    Ok(serde_json::from_slice(&response.leader_lease_json)?)
}

async fn renew_lease(
    client: &mut PersonalDbClient,
    bucket: &str,
    lease: &LeaderLease,
) -> TestResult<LeaderLease> {
    let response = client
        .renew_leader_lease(PersonalDbRenewLeaderLeaseRequest {
            bucket: bucket.into(),
            leader_lease_json: serde_json::to_vec(lease)?,
            duration_millis: LEASE_DURATION_MILLIS,
        })
        .await?
        .into_inner();
    Ok(serde_json::from_slice(&response.leader_lease_json)?)
}

async fn witness_commit(
    client: &mut PersonalDbClient,
    bucket: &str,
    proposed: &ProposedLogEntry,
    voter_acks: &[VoterAck],
) -> TestResult<CommittedEntry> {
    let response = client
        .witness_commit(PersonalDbWitnessCommitRequest {
            bucket: bucket.into(),
            proposed_log_entry_json: serde_json::to_vec(proposed)?,
            voter_ack_json: voter_acks
                .iter()
                .map(serde_json::to_vec)
                .collect::<Result<Vec<_>, _>>()?,
        })
        .await?
        .into_inner();
    Ok(serde_json::from_slice(&response.committed_entry_json)?)
}

fn proposed_entry(
    database: &DatabaseId,
    lease: &LeaderLease,
    leader: &ReplicaId,
    log_index: u64,
    previous_hash: String,
    changeset: &str,
) -> ProposedLogEntry {
    ProposedLogEntry {
        database_id: database.clone(),
        placement_epoch: lease.placement_epoch,
        client_log_epoch: lease.client_log_epoch,
        membership_epoch: lease.membership_epoch,
        policy_epoch: lease.policy_epoch,
        voter_set_id: lease.voter_set_id.clone(),
        leader_lease_id: Some(lease.lease_id),
        log_index,
        previous_hash,
        payload: MutationPayload::sql_batch_for_testing(changeset),
        proposal_id: ProposalId::new(),
        origin_replica: leader.clone(),
        leader_replica: leader.clone(),
    }
}

fn voter_ack(proposed: &ProposedLogEntry, replica_id: ReplicaId) -> TestResult<VoterAck> {
    Ok(VoterAck {
        replica_id,
        placement_epoch: proposed.placement_epoch,
        client_log_epoch: proposed.client_log_epoch,
        membership_epoch: proposed.membership_epoch,
        policy_epoch: proposed.policy_epoch,
        voter_set_id: proposed.voter_set_id.clone(),
        log_index: proposed.log_index,
        previous_hash: proposed.previous_hash.clone(),
        entry_hash: proposed.entry_hash()?,
    })
}

async fn require_renew_rejected(
    client: &mut PersonalDbClient,
    bucket: &str,
    stale_lease: &LeaderLease,
) -> TestResult<()> {
    let result = client
        .renew_leader_lease(PersonalDbRenewLeaderLeaseRequest {
            bucket: bucket.into(),
            leader_lease_json: serde_json::to_vec(stale_lease)?,
            duration_millis: LEASE_DURATION_MILLIS,
        })
        .await;
    require_failed_precondition("superseded leader lease renewal", result.err())
}

async fn require_witness_rejected(
    client: &mut PersonalDbClient,
    bucket: &str,
    stale_proposal: &ProposedLogEntry,
    voter_acks: &[VoterAck],
) -> TestResult<()> {
    let result = client
        .witness_commit(PersonalDbWitnessCommitRequest {
            bucket: bucket.into(),
            proposed_log_entry_json: serde_json::to_vec(stale_proposal)?,
            voter_ack_json: voter_acks
                .iter()
                .map(serde_json::to_vec)
                .collect::<Result<Vec<_>, _>>()?,
        })
        .await;
    require_failed_precondition("superseded leader lease proposal", result.err())
}

fn require_failed_precondition(operation: &str, rejection: Option<Status>) -> TestResult<()> {
    match rejection {
        Some(status) if status.code() == Code::FailedPrecondition => Ok(()),
        Some(status) => Err(invalid(format!(
            "{operation} returned {}, not FAILED_PRECONDITION: {}",
            status.code(),
            status.message()
        ))),
        None => Err(invalid(format!("{operation} unexpectedly succeeded"))),
    }
}

async fn catch_up(
    client: &mut PersonalDbClient,
    bucket: &str,
    codec: &JsonTransportCodec,
    limits: &TransportLimits,
    message_id: &str,
    database: &DatabaseId,
    from: &LogPosition,
) -> TestResult<Vec<ServerMessage>> {
    exchange(
        client,
        bucket,
        codec,
        limits,
        message_id,
        ClientMessage::CatchUp(CatchUpRequest {
            database_id: database.clone(),
            from: from.clone(),
        }),
    )
    .await
}

async fn exchange(
    client: &mut PersonalDbClient,
    bucket: &str,
    codec: &JsonTransportCodec,
    limits: &TransportLimits,
    message_id: &str,
    message: ClientMessage,
) -> TestResult<Vec<ServerMessage>> {
    let request_id = MessageId::new(message_id);
    let frame = codec.encode_client_message(request_id.clone(), &message)?;
    let response = client
        .exchange(PersonalDbExchangeRequest {
            bucket: bucket.into(),
            frame_json: codec.encode_frame(&frame, limits)?,
        })
        .await?
        .into_inner();
    decode_response(codec, limits, &request_id, response)
}

fn decode_response(
    codec: &JsonTransportCodec,
    limits: &TransportLimits,
    request_id: &MessageId,
    response: PersonalDbExchangeResponse,
) -> TestResult<Vec<ServerMessage>> {
    response
        .frame_json
        .into_iter()
        .map(|bytes| {
            let frame = codec.decode_frame(&bytes, limits)?;
            if frame.kind != WireMessageKind::ServerMessage
                || frame.correlation_id.as_ref() != Some(request_id)
            {
                return Err(invalid("PersonalDB response frame has invalid correlation"));
            }
            serde_json::from_slice(&frame.payload).map_err(|error| error.into())
        })
        .collect()
}

fn require_snapshot(
    operation: &str,
    messages: &[ServerMessage],
    database: &DatabaseId,
    expected: &SnapshotMetadata,
) -> TestResult<()> {
    match messages {
        [
            ServerMessage::SnapshotManifest(SnapshotManifestResponse {
                database_group,
                manifest: Some(manifest),
            }),
        ] if database_group == database && manifest == expected => Ok(()),
        other => Err(unexpected(operation, other)),
    }
}

fn require_committed_catch_up(
    messages: &[ServerMessage],
    database: &DatabaseId,
    genesis: &LogPosition,
    expected: &[CommittedEntry],
) -> TestResult<()> {
    match messages {
        [
            ServerMessage::CatchUpBatch(CatchUpBatchMessage {
                database_group,
                from,
                entries,
                next: None,
                segment_refs,
            }),
            ServerMessage::CatchUpComplete(CatchUpCompleteMessage {
                database_group: completed_database,
                final_position,
                current_head,
            }),
        ] if database_group == database
            && completed_database == database
            && from == genesis
            && segment_refs.is_empty()
            && entries == expected
            && exact_predecessor_chain(entries, database, genesis)
            && entries
                .iter()
                .all(|entry| require_production_entry(entry).is_ok())
            && entries.last().is_some_and(|entry| {
                final_position == &entry.position() && current_head == &entry.position()
            }) =>
        {
            Ok(())
        }
        other => Err(unexpected("CatchUp", other)),
    }
}

fn exact_predecessor_chain(
    entries: &[CommittedEntry],
    database: &DatabaseId,
    from: &LogPosition,
) -> bool {
    let mut predecessor = from.clone();
    for entry in entries {
        if entry.entry.database_id != *database
            || entry.entry.log_index != predecessor.index + 1
            || entry.entry.previous_hash != predecessor.hash
        {
            return false;
        }
        predecessor = entry.position();
    }
    true
}

fn require_production_entry(entry: &CommittedEntry) -> TestResult<()> {
    entry.verify_certificate_binding()?;
    if !entry.certificate.has_production_witness() {
        return Err(invalid(format!(
            "PersonalDB entry {} does not carry a production witness certificate",
            entry.entry.log_index
        )));
    }
    Ok(())
}

fn load_state(path: &Path, bucket: &str) -> TestResult<QualificationState> {
    let state: QualificationState = serde_json::from_slice(&fs::read(path)?)?;
    if state.schema_version != 1 || state.bucket != bucket {
        return Err(invalid(
            "PersonalDB qualification state has a mismatched schema or bucket",
        ));
    }
    require_production_entry(&state.first_entry)?;
    Ok(state)
}

fn save_state(path: &Path, state: &QualificationState) -> TestResult<()> {
    fs::write(path, serde_json::to_vec(state)?)?;
    Ok(())
}

fn required(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| invalid(format!("{name} must be set")))
}

fn unexpected(operation: &str, messages: &[ServerMessage]) -> Box<dyn Error + Send + Sync> {
    invalid(format!(
        "PersonalDB {operation} returned unexpected messages: {messages:?}"
    ))
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
