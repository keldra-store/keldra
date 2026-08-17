//! Public-API PersonalDB qualification for one- and three-node Docker harnesses.

use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::io;

use anvil_storage::v1::{
    AppendPersonalDbEntryRequest, ChangePersonalDbGroupRoleRequest, CreateApplicationRequest,
    CreateBucketRequest, CreatePersonalDbGroupRequest, DescribePersonalDbGroupRequest,
    GetPersonalDbSnapshotRequest, ListPersonalDbGroupsRequest,
    MaterializePersonalDbProjectionRequest, ObjectVersioning, PersonalDbCommit, PersonalDbGroup,
    PersonalDbGroupKind, PersonalDbGroupRole, PersonalDbMirrorProjectionDefinition,
    RegisterPersonalDbSnapshotRequest,
};
use anvil_storage::{
    RawAdministrationClient, RawPersonalDbClient, administration_client, connect_channel,
    exchange_client_credentials, personaldb_client,
};
use personaldb_protocol::{
    CommitCertificateV2, CommittedHeadV2, DatabaseGroupKind, DatabaseId, GroupDescriptor,
    PersonalDbSnapshotFrameV1, PersonalDbSyncFrameV1, ProjectionDefinitionModeV1,
    ProjectionDefinitionV1, PublicKeyTrustRecord, PublicKeyTrustStore, Sha256Digest,
    SignedProjectionDerivationV1, SignedSnapshotTargetManifestV1, SnapshotCompressionV1,
    SnapshotTargetManifestV1,
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const SOURCE_DATABASE: &str = "qualification-source-database";
const SOURCE_GROUP: &str = "source";
const PROJECTION_DATABASE: &str = "qualification-projection-database";
const PROJECTION_GROUP: &str = "mirror";
const SNAPSHOT_ID: &str = "qualification-snapshot-v1";
const SNAPSHOT_PLAIN: &[u8] = b"SQLite format 3\0personaldb qualification snapshot v1";
const SNAPSHOT_ZSTD: &[u8] = &[
    0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0xa1, 0x01, 0x00, 0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20,
    0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00, 0x70, 0x65, 0x72, 0x73, 0x6f, 0x6e, 0x61,
    0x6c, 0x64, 0x62, 0x20, 0x71, 0x75, 0x61, 0x6c, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f,
    0x6e, 0x20, 0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x20, 0x76, 0x31, 0x55, 0x01, 0x75,
    0x4b,
];

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let endpoints = required("ANVIL_PERSONALDB_QUALIFICATION_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !matches!(endpoints.len(), 1 | 3) {
        return Err(invalid(
            "PersonalDB qualification requires either one or three endpoints",
        ));
    }
    let tenant = required("ANVIL_PERSONALDB_QUALIFICATION_TENANT")?;
    let client_id = required("ANVIL_PERSONALDB_QUALIFICATION_CLIENT_ID")?;
    let client_secret = required("ANVIL_PERSONALDB_QUALIFICATION_CLIENT_SECRET")?;

    let mut channels = Vec::with_capacity(endpoints.len());
    for endpoint in &endpoints {
        channels.push(connect_channel(endpoint).await?);
    }
    let owner_token = exchange_client_credentials(channels[0].clone(), client_id, client_secret)
        .await?
        .access_token;
    let mut administrators = channels
        .iter()
        .cloned()
        .map(|channel| administration_client(channel, &owner_token))
        .collect::<Result<Vec<_>, _>>()?;
    let mut databases = channels
        .iter()
        .cloned()
        .map(|channel| personaldb_client(channel, &owner_token))
        .collect::<Result<Vec<_>, _>>()?;

    let node_count = endpoints.len();
    let bucket = format!("personaldb-qualification-{node_count}-node");
    create_bucket(&mut administrators[0], &bucket).await?;
    let reader_app_id = format!("personaldb-qualification-reader-{node_count}");
    let reader_client_id = format!("personaldb-qualification-reader-client-{node_count}");
    let reader_secret = format!("personaldb-qualification-reader-secret-{node_count}-0123456789");
    create_reader_application(
        &mut administrators[1 % node_count],
        &tenant,
        &reader_app_id,
        &reader_client_id,
        &reader_secret,
    )
    .await?;
    let reader_token = exchange_client_credentials(
        channels[2 % node_count].clone(),
        reader_client_id,
        reader_secret,
    )
    .await?
    .access_token;
    let mut readers = channels
        .iter()
        .cloned()
        .map(|channel| personaldb_client(channel, &reader_token))
        .collect::<Result<Vec<_>, _>>()?;

    let schema_hash = Sha256Digest::hash(b"personaldb-qualification-schema-v1");
    let source_group = databases[0]
        .create_group(CreatePersonalDbGroupRequest {
            bucket: bucket.clone(),
            database_id: SOURCE_DATABASE.into(),
            group_id: SOURCE_GROUP.into(),
            kind: PersonalDbGroupKind::Source as i32,
            schema_hash_sha256: schema_hash.as_bytes().to_vec(),
            mirror_projection: None,
            command_id: "personaldb-qualification-create-source".into(),
        })
        .await?
        .into_inner();
    let (source_descriptor, source_trust) = verified_group(&source_group)?;
    require_group(
        &source_descriptor,
        SOURCE_DATABASE,
        SOURCE_GROUP,
        DatabaseGroupKind::Source,
        schema_hash,
    )?;

    let projection_definition = ProjectionDefinitionV1 {
        projection_database_id: DatabaseId::new(PROJECTION_DATABASE),
        projection_group_id: PROJECTION_GROUP.into(),
        source_database_id: DatabaseId::new(SOURCE_DATABASE),
        source_group_id: SOURCE_GROUP.into(),
        source_bucket: bucket.clone(),
        mode: ProjectionDefinitionModeV1::Mirror,
    };
    let projection_hash = projection_definition.canonical_sha256()?;
    let projection_group = databases[1 % node_count]
        .create_group(CreatePersonalDbGroupRequest {
            bucket: bucket.clone(),
            database_id: PROJECTION_DATABASE.into(),
            group_id: PROJECTION_GROUP.into(),
            kind: PersonalDbGroupKind::Projection as i32,
            schema_hash_sha256: schema_hash.as_bytes().to_vec(),
            mirror_projection: Some(PersonalDbMirrorProjectionDefinition {
                source_bucket: bucket.clone(),
                source_database_id: SOURCE_DATABASE.into(),
                source_group_id: SOURCE_GROUP.into(),
            }),
            command_id: "personaldb-qualification-create-projection".into(),
        })
        .await?
        .into_inner();
    let (projection_descriptor, projection_trust) = verified_group(&projection_group)?;
    require_group(
        &projection_descriptor,
        PROJECTION_DATABASE,
        PROJECTION_GROUP,
        DatabaseGroupKind::Projection,
        schema_hash,
    )?;
    if projection_descriptor.projection_definition_hash() != Some(projection_hash) {
        return Err(invalid("projection descriptor has another definition hash"));
    }

    let described = databases[2 % node_count]
        .describe_group(describe(&bucket, SOURCE_DATABASE, SOURCE_GROUP))
        .await?
        .into_inner();
    let (described_source, _) = verified_group(&described)?;
    if described_source != source_descriptor {
        return Err(invalid("DescribeGroup disagrees with CreateGroup"));
    }
    verify_owner_list(&mut databases[0], &bucket).await?;

    require_hidden(
        readers[0]
            .describe_group(describe(&bucket, SOURCE_DATABASE, SOURCE_GROUP))
            .await,
        "an application without a group role",
    )?;
    let grant_reader = ChangePersonalDbGroupRoleRequest {
        bucket: bucket.clone(),
        database_id: SOURCE_DATABASE.into(),
        group_id: SOURCE_GROUP.into(),
        app_id: reader_app_id.clone(),
        role: PersonalDbGroupRole::Reader as i32,
        command_id: "personaldb-qualification-grant-reader".into(),
    };
    let granted = databases[1 % node_count]
        .grant_group_role(grant_reader.clone())
        .await?
        .into_inner();
    if granted.authorization_revision == 0 || granted.replayed {
        return Err(invalid("GrantGroupRole returned invalid evidence"));
    }
    let reader_visible = readers[2 % node_count]
        .describe_group(describe(&bucket, SOURCE_DATABASE, SOURCE_GROUP))
        .await?
        .into_inner();
    verified_group(&reader_visible)?;
    verify_reader_list(&mut readers[1 % node_count], &bucket, true).await?;

    // The first grant advanced the realm revision. Reconstructing the same
    // protocol command must still replay its retained result rather than bind
    // the stable command ID to the newly observed revision.
    let replayed_grant = databases[2 % node_count]
        .grant_group_role(grant_reader)
        .await?
        .into_inner();
    if !replayed_grant.replayed
        || replayed_grant.authorization_revision != granted.authorization_revision
    {
        return Err(invalid(
            "GrantGroupRole did not replay after the realm revision advanced",
        ));
    }

    let revoked = databases[2 % node_count]
        .revoke_group_role(ChangePersonalDbGroupRoleRequest {
            bucket: bucket.clone(),
            database_id: SOURCE_DATABASE.into(),
            group_id: SOURCE_GROUP.into(),
            app_id: reader_app_id,
            role: PersonalDbGroupRole::Reader as i32,
            command_id: "personaldb-qualification-revoke-reader".into(),
        })
        .await?
        .into_inner();
    if revoked.authorization_revision <= granted.authorization_revision || revoked.replayed {
        return Err(invalid("RevokeGroupRole did not advance authorization"));
    }
    require_hidden(
        readers[0]
            .describe_group(describe(&bucket, SOURCE_DATABASE, SOURCE_GROUP))
            .await,
        "an application whose group role was revoked",
    )?;
    verify_reader_list(&mut readers[2 % node_count], &bucket, false).await?;

    let changeset = b"personaldb-qualification-sqlite-changeset-v1".to_vec();
    let appended = databases[2 % node_count]
        .append_entry(AppendPersonalDbEntryRequest {
            bucket: bucket.clone(),
            database_id: SOURCE_DATABASE.into(),
            group_id: SOURCE_GROUP.into(),
            expected_log_index: 0,
            expected_log_hash_sha256: Sha256Digest::ZERO.as_bytes().to_vec(),
            changeset: changeset.clone(),
            client_proposal_hash_sha256: Sha256Digest::hash(b"qualification-client-proposal")
                .as_bytes()
                .to_vec(),
            database_state_root_sha256: Sha256Digest::hash(b"qualification-state-after-entry-1")
                .as_bytes()
                .to_vec(),
            schema_hash_sha256: schema_hash.as_bytes().to_vec(),
            membership_revision: 1,
            client_log_epoch: 1,
            signed_client_proposal: b"qualification-signed-client-proposal".to_vec(),
            signed_voter_acknowledgements: vec![b"qualification-signed-voter-ack".to_vec()],
            signed_proposal_admission: b"qualification-signed-proposal-admission".to_vec(),
            command_id: "personaldb-qualification-append-source-1".into(),
        })
        .await?
        .into_inner();
    let (source_certificate, source_head) =
        verified_commit(&appended, SOURCE_GROUP, &source_trust)?;
    if source_head.state().log_index != 1
        || source_certificate
            .unsigned()
            .entry_core
            .changeset_payload_hash
            != Sha256Digest::hash(&changeset)
    {
        return Err(invalid("AppendEntry returned another committed entry"));
    }
    let current_source = databases[0]
        .describe_group(describe(&bucket, SOURCE_DATABASE, SOURCE_GROUP))
        .await?
        .into_inner();
    let (current_source, current_source_trust) = verified_group(&current_source)?;
    if current_source.committed_head() != &source_head {
        return Err(invalid("DescribeGroup did not expose the appended head"));
    }
    verify_catch_up(
        &mut databases[0],
        &bucket,
        schema_hash,
        &changeset,
        &source_certificate,
        &source_head,
        &current_source_trust,
    )
    .await?;

    let materialized = databases[1 % node_count]
        .materialize_projection(MaterializePersonalDbProjectionRequest {
            bucket: bucket.clone(),
            database_id: PROJECTION_DATABASE.into(),
            group_id: PROJECTION_GROUP.into(),
            through_source_log_index: source_head.state().log_index,
            through_source_log_hash_sha256: source_head.state().log_hash.as_bytes().to_vec(),
            max_entries: 100,
            max_bytes: 1024 * 1024,
            command_id: "personaldb-qualification-materialize-mirror".into(),
        })
        .await?
        .into_inner();
    if materialized.source_log_index != source_head.state().log_index
        || materialized.source_log_hash_sha256 != source_head.state().log_hash.as_bytes()
        || materialized.commits.len() != 1
    {
        return Err(invalid(
            "MaterializeProjection stopped at another checkpoint",
        ));
    }
    let (projection_certificate, projection_head) = verified_commit(
        &materialized.commits[0],
        PROJECTION_GROUP,
        &projection_trust,
    )?;
    verify_projection_derivation(
        &projection_certificate,
        &projection_head,
        &source_head,
        projection_hash,
        &projection_trust,
    )?;
    let current_projection = databases[2 % node_count]
        .describe_group(describe(&bucket, PROJECTION_DATABASE, PROJECTION_GROUP))
        .await?
        .into_inner();
    let (current_projection, _) = verified_group(&current_projection)?;
    if current_projection.committed_head() != &projection_head {
        return Err(invalid(
            "DescribeGroup did not expose the materialized projection head",
        ));
    }

    let snapshot_bytes = SNAPSHOT_ZSTD.to_vec();
    let unsigned_snapshot = SnapshotTargetManifestV1 {
        snapshot_id: SNAPSHOT_ID.into(),
        group_id: SOURCE_GROUP.into(),
        committed_head: source_head.clone(),
        schema_hash,
        projection_definition_hash: None,
        ordered_source_heads: Vec::new(),
        compression: SnapshotCompressionV1::ZstdFrameV1,
        compression_level: 3,
        compression_checksum: true,
        compression_content_size: false,
        dictionary_id: None,
        chunk_size: 7,
        compressed_length: snapshot_bytes.len() as u64,
        compressed_sha256: Sha256Digest::hash(&snapshot_bytes),
        uncompressed_length: SNAPSHOT_PLAIN.len() as u64,
        uncompressed_sha256: Sha256Digest::hash(SNAPSHOT_PLAIN),
        object_id: "qualification-snapshot-object-v1".into(),
    };
    let unsigned_snapshot_bytes = unsigned_snapshot.encode_deterministic()?;
    if SnapshotTargetManifestV1::decode_canonical(&unsigned_snapshot_bytes)? != unsigned_snapshot {
        return Err(invalid("unsigned snapshot manifest is not canonical"));
    }
    let registered = databases[2 % node_count]
        .register_snapshot(RegisterPersonalDbSnapshotRequest {
            bucket: bucket.clone(),
            database_id: SOURCE_DATABASE.into(),
            group_id: SOURCE_GROUP.into(),
            manifest: unsigned_snapshot_bytes,
            snapshot: snapshot_bytes.clone(),
            command_id: "personaldb-qualification-register-snapshot".into(),
        })
        .await?
        .into_inner();
    if registered.replayed {
        return Err(invalid("RegisterSnapshot unexpectedly reported a replay"));
    }
    let signed_snapshot =
        SignedSnapshotTargetManifestV1::decode_canonical(&registered.signed_manifest)?;
    signed_snapshot.verify(&source_trust)?;
    if signed_snapshot.manifest != unsigned_snapshot {
        return Err(invalid("RegisterSnapshot signed another manifest"));
    }
    verify_snapshot_stream(
        &mut databases[0],
        &bucket,
        &snapshot_bytes,
        &signed_snapshot,
        &source_trust,
    )
    .await?;

    println!(
        "PersonalDB qualification passed on {} node(s): create/describe/list, role grant/revoke, append/catch-up, mirror materialization and snapshot round-trip",
        endpoints.len()
    );
    Ok(())
}

async fn create_bucket(client: &mut RawAdministrationClient, bucket: &str) -> TestResult<()> {
    let created = client
        .create_bucket(CreateBucketRequest {
            bucket: bucket.into(),
            versioning: ObjectVersioning::Unversioned as i32,
        })
        .await?
        .into_inner();
    if created.bucket != bucket {
        return Err(invalid("bucket creation returned another bucket"));
    }
    Ok(())
}

async fn create_reader_application(
    client: &mut RawAdministrationClient,
    tenant: &str,
    app_id: &str,
    client_id: &str,
    client_secret: &str,
) -> TestResult<()> {
    let created = client
        .create_application(CreateApplicationRequest {
            app_id: app_id.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        })
        .await?
        .into_inner();
    if created.storage_tenant != tenant
        || created.app_id != app_id
        || created.client_id != client_id
        || !created.active
    {
        return Err(invalid("created reader application has another identity"));
    }
    Ok(())
}

fn describe(bucket: &str, database_id: &str, group_id: &str) -> DescribePersonalDbGroupRequest {
    DescribePersonalDbGroupRequest {
        bucket: bucket.into(),
        database_id: database_id.into(),
        group_id: group_id.into(),
    }
}

fn verified_group(group: &PersonalDbGroup) -> TestResult<(GroupDescriptor, PublicKeyTrustStore)> {
    if group.trust_records_json.is_empty() {
        return Err(invalid("PersonalDB group omitted its trust records"));
    }
    let mut records = Vec::with_capacity(group.trust_records_json.len());
    for encoded in &group.trust_records_json {
        let record: PublicKeyTrustRecord = serde_json::from_slice(encoded)?;
        record.validate()?;
        if serde_json::to_vec(&record)? != *encoded {
            return Err(invalid("PersonalDB trust record JSON is not canonical"));
        }
        records.push(record);
    }
    let trust = PublicKeyTrustStore::from_records(records)?;
    let descriptor = GroupDescriptor::decode_canonical(&group.descriptor)?;
    descriptor.verify(&trust)?;
    Ok((descriptor, trust))
}

fn require_group(
    descriptor: &GroupDescriptor,
    database_id: &str,
    group_id: &str,
    kind: DatabaseGroupKind,
    schema_hash: Sha256Digest,
) -> TestResult<()> {
    if descriptor.database_id().0 != database_id
        || descriptor.group_id() != group_id
        || descriptor.group_kind() != kind
        || descriptor.schema_hash() != schema_hash
        || descriptor.committed_head().state().log_index != 0
        || descriptor.committed_head().state().log_hash != Sha256Digest::ZERO
    {
        return Err(invalid("PersonalDB group descriptor has another identity"));
    }
    Ok(())
}

async fn verify_owner_list(client: &mut RawPersonalDbClient, bucket: &str) -> TestResult<()> {
    let page = client
        .list_groups(ListPersonalDbGroupsRequest {
            bucket: bucket.into(),
            page_token: String::new(),
            limit: 100,
        })
        .await?
        .into_inner();
    if !page.next_page_token.is_empty() || page.groups.len() != 2 {
        return Err(invalid(
            "owner ListGroups did not return exactly two groups",
        ));
    }
    let mut identities = BTreeSet::new();
    for group in &page.groups {
        let (descriptor, _) = verified_group(group)?;
        identities.insert((
            descriptor.database_id().0.clone(),
            descriptor.group_id().to_owned(),
        ));
    }
    let expected = BTreeSet::from([
        (SOURCE_DATABASE.to_owned(), SOURCE_GROUP.to_owned()),
        (PROJECTION_DATABASE.to_owned(), PROJECTION_GROUP.to_owned()),
    ]);
    if identities != expected {
        return Err(invalid("owner ListGroups returned another group set"));
    }
    Ok(())
}

async fn verify_reader_list(
    client: &mut RawPersonalDbClient,
    bucket: &str,
    granted: bool,
) -> TestResult<()> {
    let page = client
        .list_groups(ListPersonalDbGroupsRequest {
            bucket: bucket.into(),
            page_token: String::new(),
            limit: 100,
        })
        .await?
        .into_inner();
    let expected = usize::from(granted);
    if !page.next_page_token.is_empty() || page.groups.len() != expected {
        return Err(invalid(
            "ListGroups did not apply the current exact group role",
        ));
    }
    if let Some(group) = page.groups.first() {
        let (descriptor, _) = verified_group(group)?;
        if descriptor.database_id().0 != SOURCE_DATABASE || descriptor.group_id() != SOURCE_GROUP {
            return Err(invalid("reader ListGroups exposed an unauthorized group"));
        }
    }
    Ok(())
}

fn require_hidden<T>(
    response: Result<tonic::Response<T>, tonic::Status>,
    subject: &str,
) -> TestResult<()> {
    match response {
        Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
        Err(status) => Err(invalid(format!(
            "DescribeGroup for {subject} returned {} instead of NOT_FOUND",
            status.code()
        ))),
        Ok(_) => Err(invalid(format!(
            "DescribeGroup exposed a group to {subject}"
        ))),
    }
}

fn verified_commit(
    commit: &PersonalDbCommit,
    group_id: &str,
    trust: &PublicKeyTrustStore,
) -> TestResult<(CommitCertificateV2, CommittedHeadV2)> {
    if commit.replayed {
        return Err(invalid("new PersonalDB commit was reported as a replay"));
    }
    let certificate = CommitCertificateV2::decode_canonical(&commit.commit_certificate)?;
    let head = CommittedHeadV2::decode_canonical(&commit.committed_head)?;
    certificate.verify(group_id, trust)?;
    head.verify(group_id, trust)?;
    head.verify_certificate(&certificate)?;
    Ok((certificate, head))
}

async fn verify_catch_up(
    client: &mut RawPersonalDbClient,
    bucket: &str,
    schema_hash: Sha256Digest,
    changeset: &[u8],
    expected_certificate: &CommitCertificateV2,
    expected_head: &CommittedHeadV2,
    trust: &PublicKeyTrustStore,
) -> TestResult<()> {
    let mut stream = client
        .catch_up(anvil_storage::v1::PersonalDbCatchUpRequest {
            bucket: bucket.into(),
            database_id: SOURCE_DATABASE.into(),
            group_id: SOURCE_GROUP.into(),
            request_id: "personaldb-qualification-catch-up".into(),
            projection_profile_id: "personaldb-qualification-source-profile".into(),
            from_log_index: 0,
            from_log_hash_sha256: Sha256Digest::ZERO.as_bytes().to_vec(),
            expected_schema_hash_sha256: schema_hash.as_bytes().to_vec(),
            expected_projection_definition_hash_sha256: None,
            max_entries: 100,
            max_bytes: 1024 * 1024,
        })
        .await?
        .into_inner();
    let mut frames = Vec::new();
    while let Some(frame) = stream.message().await? {
        frames.push(PersonalDbSyncFrameV1::decode_canonical(&frame.value)?);
    }
    if frames.len() != 5 {
        return Err(invalid(format!(
            "CatchUp returned {} frames instead of five",
            frames.len()
        )));
    }
    let PersonalDbSyncFrameV1::Header(header) = &frames[0] else {
        return Err(invalid("CatchUp did not start with a header"));
    };
    header.advertised_head.verify(SOURCE_GROUP, trust)?;
    if header.group_id != SOURCE_GROUP
        || header.from_log_index != 0
        || header.from_log_hash != Sha256Digest::ZERO
        || header.schema_hash != schema_hash
        || header.advertised_head != *expected_head
    {
        return Err(invalid("CatchUp header describes another history"));
    }
    let PersonalDbSyncFrameV1::EntryStart(start) = &frames[1] else {
        return Err(invalid("CatchUp omitted its entry start"));
    };
    let expected_entry_id = format!(
        "sha256-{}",
        hex::encode(
            expected_certificate
                .unsigned()
                .entry_core
                .entry_hash()?
                .as_bytes()
        )
    );
    start.commit_certificate.verify(SOURCE_GROUP, trust)?;
    if start.entry_id != expected_entry_id
        || start.commit_certificate != *expected_certificate
        || start.changeset_length != changeset.len() as u64
        || start.changeset_sha256 != Sha256Digest::hash(changeset)
    {
        return Err(invalid("CatchUp entry start has another certificate"));
    }
    let PersonalDbSyncFrameV1::EntryChunk(chunk) = &frames[2] else {
        return Err(invalid("CatchUp omitted its entry bytes"));
    };
    if chunk.entry_id != expected_entry_id
        || chunk.offset != 0
        || chunk.data != changeset
        || chunk.chunk_sha256 != Sha256Digest::hash(changeset)
    {
        return Err(invalid("CatchUp entry chunk failed its digest"));
    }
    let PersonalDbSyncFrameV1::EntryEnd(end) = &frames[3] else {
        return Err(invalid("CatchUp omitted its entry end"));
    };
    end.committed_head.verify(SOURCE_GROUP, trust)?;
    end.committed_head
        .verify_certificate(expected_certificate)?;
    if end.entry_id != expected_entry_id
        || end.committed_head != *expected_head
        || end.delivered_length != changeset.len() as u64
        || end.delivered_sha256 != Sha256Digest::hash(changeset)
    {
        return Err(invalid("CatchUp entry end has another committed head"));
    }
    let PersonalDbSyncFrameV1::End(end) = &frames[4] else {
        return Err(invalid("CatchUp omitted its final checkpoint"));
    };
    end.resulting_head.verify(SOURCE_GROUP, trust)?;
    if end.delivered_entry_count != 1
        || end.delivered_byte_count != changeset.len() as u64
        || end.resulting_head != *expected_head
    {
        return Err(invalid("CatchUp final checkpoint is inconsistent"));
    }
    Ok(())
}

fn verify_projection_derivation(
    certificate: &CommitCertificateV2,
    head: &CommittedHeadV2,
    source_head: &CommittedHeadV2,
    projection_hash: Sha256Digest,
    trust: &PublicKeyTrustStore,
) -> TestResult<()> {
    let encoded = certificate
        .unsigned()
        .signed_projection_derivation
        .as_deref()
        .ok_or_else(|| invalid("projection commit omitted its signed derivation"))?;
    let signed = SignedProjectionDerivationV1::decode_canonical(encoded)?;
    signed.verify(trust)?;
    let [source] = signed.derivation.ordered_source_heads.as_slice() else {
        return Err(invalid("mirror derivation did not name one source head"));
    };
    if signed.derivation.projection_database_id.0 != PROJECTION_DATABASE
        || signed.derivation.projection_definition_hash != projection_hash
        || signed.derivation.previous_projection_log_index != 0
        || signed.derivation.previous_projection_log_hash != Sha256Digest::ZERO
        || signed.derivation.resulting_state != *head.state()
        || source.database_id.0 != SOURCE_DATABASE
        || source.log_index != source_head.state().log_index
        || source.log_hash != source_head.state().log_hash
    {
        return Err(invalid("signed projection derivation has another lineage"));
    }
    Ok(())
}

async fn verify_snapshot_stream(
    client: &mut RawPersonalDbClient,
    bucket: &str,
    expected_bytes: &[u8],
    expected_manifest: &SignedSnapshotTargetManifestV1,
    trust: &PublicKeyTrustStore,
) -> TestResult<()> {
    let mut stream = client
        .get_snapshot(GetPersonalDbSnapshotRequest {
            bucket: bucket.into(),
            database_id: SOURCE_DATABASE.into(),
            group_id: SOURCE_GROUP.into(),
            request_id: "personaldb-qualification-get-snapshot".into(),
            snapshot_id: SNAPSHOT_ID.into(),
            start_offset: 0,
            max_bytes: 0,
        })
        .await?
        .into_inner();
    let mut header_seen = false;
    let mut end_seen = false;
    let mut delivered = Vec::new();
    while let Some(frame) = stream.message().await? {
        match PersonalDbSnapshotFrameV1::decode_canonical(&frame.value)? {
            PersonalDbSnapshotFrameV1::Header(header) => {
                if header_seen || !delivered.is_empty() || end_seen {
                    return Err(invalid("snapshot stream header is out of order"));
                }
                header.signed_manifest.verify(trust)?;
                if header.signed_manifest != *expected_manifest
                    || header.start_offset != 0
                    || header.end_offset_exclusive != expected_bytes.len() as u64
                {
                    return Err(invalid("snapshot stream header has another manifest"));
                }
                header_seen = true;
            }
            PersonalDbSnapshotFrameV1::Chunk(chunk) => {
                if !header_seen || end_seen || chunk.offset != delivered.len() as u64 {
                    return Err(invalid("snapshot chunk is out of order"));
                }
                if chunk.chunk_sha256 != Sha256Digest::hash(&chunk.data) {
                    return Err(invalid("snapshot chunk failed its digest"));
                }
                delivered.extend_from_slice(&chunk.data);
            }
            PersonalDbSnapshotFrameV1::End(end) => {
                if !header_seen || end_seen {
                    return Err(invalid("snapshot stream end is out of order"));
                }
                if end.delivered_length != delivered.len() as u64
                    || end.delivered_sha256 != Sha256Digest::hash(&delivered)
                    || end.next_offset != delivered.len() as u64
                    || !end.complete
                {
                    return Err(invalid("snapshot end evidence is inconsistent"));
                }
                end_seen = true;
            }
            PersonalDbSnapshotFrameV1::Error(error) => {
                return Err(invalid(format!(
                    "snapshot stream returned protocol error {:?}",
                    error.code
                )));
            }
        }
    }
    if !header_seen || !end_seen || delivered != expected_bytes {
        return Err(invalid("snapshot stream did not reproduce its object"));
    }
    Ok(())
}

fn required(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| invalid(format!("{name} must be set")))
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
