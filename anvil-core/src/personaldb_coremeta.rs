use crate::{
    core_store::{
        AuthzScopeRef, CF_PERSONALDB, CoreByteRange, CoreManifestLocator, CoreMetaBatchOp,
        CoreMetaBatchOpKind, CoreMetaLocatorProto, CoreMetaRootPublication, CoreMetaStore,
        CoreMetaTuplePart, CoreMutationOperation, CoreMutationPrecondition, CorePrefetchPolicy,
        CoreStore, CoreTraceContext, ReadLogicalRangeRequest, TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        TABLE_PERSONALDB_GROUP_ROW, WriteLogicalFileRequest, core_meta_committed_row_common,
        core_meta_locator_from_manifest_locator, core_meta_locator_to_manifest_locator,
        core_meta_payload_digest, core_meta_record_tuple_key, core_meta_root_key_hash,
        core_meta_row_common_from_payload, core_meta_tuple_key, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::{
        hash32,
        writer::{WriterFamily, canonical_logical_file_id},
    },
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use prost::Message;

pub const PERSONALDB_DATA_LOCATOR_PAGE_MAX: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbGroupCoreMetaRow {
    pub tenant_id: i64,
    pub group_id: String,
    pub generation: u64,
    pub replica_set_hash: String,
    pub witness_policy_hash: String,
    pub latest_commit: String,
    pub snapshot_locator: Option<CoreManifestLocator>,
    pub transaction_id: String,
    pub created_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbDataLocatorCoreMetaRow {
    pub tenant_id: i64,
    pub group_id: String,
    pub data_id: String,
    pub data_kind: String,
    /// PersonalDB's logical source/writer generation.
    pub generation: u64,
    /// Contiguous CoreMeta publication generation for the group root.
    pub root_generation: u64,
    pub sqlite_changeset_hash: String,
    pub payload_locator: CoreManifestLocator,
    pub projection_keys: Vec<String>,
    pub transaction_id: String,
    pub created_at_unix_nanos: u64,
}

#[derive(Debug, Clone)]
pub struct PersonalDbDataLocatorPage {
    pub rows: Vec<PersonalDbDataLocatorCoreMetaRow>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbGroupRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    group_id: String,
    #[prost(string, tag = "3")]
    replica_set_hash: String,
    #[prost(string, tag = "4")]
    witness_policy_hash: String,
    #[prost(string, tag = "5")]
    latest_commit: String,
    #[prost(message, optional, tag = "6")]
    snapshot_locator: Option<CoreMetaLocatorProto>,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbDataLocatorRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    group_id: String,
    #[prost(string, tag = "3")]
    data_id: String,
    #[prost(string, tag = "4")]
    data_kind: String,
    #[prost(string, tag = "5")]
    sqlite_changeset_hash: String,
    #[prost(message, optional, tag = "6")]
    payload_locator: Option<CoreMetaLocatorProto>,
    #[prost(string, repeated, tag = "7")]
    projection_keys: Vec<String>,
}

pub async fn write_personaldb_bytes_as_data_locator(
    storage: &Storage,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
    data_kind: &str,
    generation: u64,
    bytes: Vec<u8>,
    sqlite_changeset_hash: String,
    projection_keys: Vec<String>,
    transaction_id: String,
) -> Result<PersonalDbDataLocatorCoreMetaRow> {
    write_personaldb_bytes_as_data_locator_with_preconditions(
        storage,
        tenant_id,
        group_id,
        data_id,
        data_kind,
        generation,
        bytes,
        sqlite_changeset_hash,
        projection_keys,
        transaction_id,
        &[],
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn write_personaldb_bytes_as_data_locator_with_preconditions(
    storage: &Storage,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
    data_kind: &str,
    generation: u64,
    bytes: Vec<u8>,
    sqlite_changeset_hash: String,
    projection_keys: Vec<String>,
    transaction_id: String,
    preconditions: &[CoreMutationPrecondition],
) -> Result<PersonalDbDataLocatorCoreMetaRow> {
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::PersonalDb,
        generation,
        data_id,
        &hash32(&bytes),
    );
    let request = WriteLogicalFileRequest {
        writer_family: WriterFamily::PersonalDb.as_str().to_string(),
        generation,
        logical_file_id,
        source: bytes,
        range_hints: Vec::new(),
        pipeline_policy: Default::default(),
        trace_context: CoreTraceContext::default(),
        boundary_values: Vec::new(),
        mutation_id: transaction_id.clone(),
        region_id: "local".to_string(),
    };
    write_personaldb_logical_file_as_data_locator_with_preconditions(
        storage,
        tenant_id,
        group_id,
        data_id,
        data_kind,
        request,
        sqlite_changeset_hash,
        projection_keys,
        transaction_id,
        preconditions,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn write_personaldb_logical_file_as_data_locator_with_preconditions(
    storage: &Storage,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
    data_kind: &str,
    request: WriteLogicalFileRequest,
    sqlite_changeset_hash: String,
    projection_keys: Vec<String>,
    transaction_id: String,
    preconditions: &[CoreMutationPrecondition],
) -> Result<PersonalDbDataLocatorCoreMetaRow> {
    validate_personaldb_scope(tenant_id, group_id)?;
    require_coremeta_ref_id(data_id, "data_id")?;
    require_safe_component(data_kind, "data_kind")?;
    if request.generation == 0 {
        bail!("PersonalDB data locator generation must be nonzero");
    }
    require_tuple_string(&request.logical_file_id, "logical_file_id")?;
    validate_coremeta_preconditions(storage, preconditions)?;
    let store = CoreStore::new(storage.clone()).await?;
    let writer_generation = request.generation;
    let logical = store.write_logical_file_with_locator(request).await?;
    // The writer generation is the PersonalDB source cursor and may begin
    // above one. CoreMeta root generations instead describe publication order
    // and must advance contiguously from the current group root.
    let root_generation = store
        .next_root_generation_for_anchor(&personaldb_root_anchor_key(tenant_id, group_id))
        .await?;
    let row = PersonalDbDataLocatorCoreMetaRow {
        tenant_id,
        group_id: group_id.to_string(),
        data_id: data_id.to_string(),
        data_kind: data_kind.to_string(),
        generation: writer_generation,
        root_generation,
        sqlite_changeset_hash,
        payload_locator: logical.locator,
        projection_keys,
        transaction_id,
        created_at_unix_nanos: current_unix_nanos()?,
    };
    write_personaldb_data_locator_row(storage, &row, preconditions).await?;
    Ok(row)
}

pub fn personaldb_group_coremeta_put_operation(
    row: &PersonalDbGroupCoreMetaRow,
) -> Result<CoreMutationOperation> {
    validate_group_row(row)?;
    Ok(CoreMutationOperation::CoreMetaPut {
        partition_id: personaldb_partition_id(row.tenant_id, &row.group_id),
        cf: CF_PERSONALDB.to_string(),
        table_id: TABLE_PERSONALDB_GROUP_ROW,
        tuple_key: personaldb_group_tuple_key(row.tenant_id, &row.group_id, row.generation)?,
        payload: encode_group_row(row)?,
    })
}

pub fn personaldb_partition_id(tenant_id: i64, group_id: &str) -> String {
    format!("personaldb:tenant:{tenant_id}:group:{group_id}")
}

pub async fn write_personaldb_data_locator_row(
    storage: &Storage,
    row: &PersonalDbDataLocatorCoreMetaRow,
    preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    validate_data_locator_row(row)?;
    let key = personaldb_data_locator_tuple_key(row.tenant_id, &row.group_id, &row.data_id)?;
    let payload = encode_data_locator_row(row)?;
    validate_coremeta_preconditions(storage, preconditions)?;
    let store = CoreStore::new(storage.clone()).await?;
    let op = CoreMetaBatchOp {
        cf: CF_PERSONALDB,
        table_id: TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        tuple_key: &key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    store
        .commit_coremeta_root_groups(
            &row.transaction_id,
            &[op],
            &[CoreMetaRootPublication::new(
                personaldb_root_anchor_key(row.tenant_id, &row.group_id),
                crate::formats::writer::WriterFamily::PersonalDb,
            )],
        )
        .await?;
    Ok(())
}

pub async fn read_personaldb_data_locator_row(
    storage: &Storage,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
) -> Result<Option<PersonalDbDataLocatorCoreMetaRow>> {
    validate_personaldb_scope(tenant_id, group_id)?;
    require_coremeta_ref_id(data_id, "data_id")?;
    let key = personaldb_data_locator_tuple_key(tenant_id, group_id, data_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(payload) =
        store.read_coremeta_row(CF_PERSONALDB, TABLE_PERSONALDB_DATA_LOCATOR_ROW, &key)?
    else {
        return Ok(None);
    };
    let row = decode_data_locator_row(&payload)?;
    if row.tenant_id != tenant_id || row.group_id != group_id || row.data_id != data_id {
        bail!("PersonalDB data locator CoreMeta row scope mismatch");
    }
    Ok(Some(row))
}

pub async fn delete_personaldb_data_locator_row(
    storage: &Storage,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
    mutation_id: &str,
) -> Result<()> {
    validate_personaldb_scope(tenant_id, group_id)?;
    require_coremeta_ref_id(data_id, "data_id")?;
    require_tuple_string(mutation_id, "mutation_id")?;
    let key = personaldb_data_locator_tuple_key(tenant_id, group_id, data_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let deleted_at_unix_nanos = current_unix_nanos()?;
    let next_root_generation = store
        .next_root_generation_for_anchor(&personaldb_root_anchor_key(tenant_id, group_id))
        .await?;
    let delete_common = store
        .read_coremeta_row(CF_PERSONALDB, TABLE_PERSONALDB_DATA_LOCATOR_ROW, &key)?
        .map(|payload| {
            core_meta_row_common_from_payload(&payload).map(|common| {
                core_meta_committed_row_common(
                    common.realm_id,
                    common.root_key_hash,
                    next_root_generation,
                    mutation_id.to_string(),
                    deleted_at_unix_nanos,
                )
            })
        })
        .transpose()?;
    let op = CoreMetaBatchOp {
        cf: CF_PERSONALDB,
        table_id: TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        tuple_key: &key,
        common: delete_common,
        kind: CoreMetaBatchOpKind::Delete,
    };
    store
        .commit_coremeta_root_groups(
            mutation_id,
            &[op],
            &[CoreMetaRootPublication::new(
                personaldb_root_anchor_key(tenant_id, group_id),
                crate::formats::writer::WriterFamily::PersonalDb,
            )],
        )
        .await?;
    Ok(())
}

pub async fn list_personaldb_data_locator_rows(
    storage: &Storage,
    tenant_id: i64,
    group_id: &str,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<PersonalDbDataLocatorPage> {
    validate_personaldb_scope(tenant_id, group_id)?;
    let prefix = personaldb_data_locator_tuple_prefix(tenant_id, group_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    page_personaldb_data_locator_rows(&store, &prefix, after_tuple_key, page_size, |row| {
        if row.tenant_id != tenant_id || row.group_id != group_id {
            bail!("PersonalDB data locator CoreMeta row scope mismatch");
        }
        Ok(())
    })
}

pub async fn list_personaldb_data_locator_rows_for_tenant(
    storage: &Storage,
    tenant_id: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<PersonalDbDataLocatorPage> {
    if tenant_id < 0 {
        bail!("PersonalDB tenant id must be nonnegative");
    }
    let prefix = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id))])?;
    let store = CoreStore::new(storage.clone()).await?;
    page_personaldb_data_locator_rows(&store, &prefix, after_tuple_key, page_size, |row| {
        if row.tenant_id != tenant_id {
            bail!("PersonalDB data locator CoreMeta tenant scope mismatch");
        }
        Ok(())
    })
}

fn page_personaldb_data_locator_rows(
    store: &CoreStore,
    prefix: &[u8],
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
    validate_scope: impl Fn(&PersonalDbDataLocatorCoreMetaRow) -> Result<()>,
) -> Result<PersonalDbDataLocatorPage> {
    if !(1..=PERSONALDB_DATA_LOCATOR_PAGE_MAX).contains(&page_size) {
        bail!(
            "PersonalDB data locator page size must be between 1 and {PERSONALDB_DATA_LOCATOR_PAGE_MAX}"
        );
    }
    let mut records = store.scan_coremeta_prefix_page(
        CF_PERSONALDB,
        TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        prefix,
        after_tuple_key,
        page_size + 1,
    )?;
    let has_more = records.len() > page_size;
    if has_more {
        records.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            core_meta_record_tuple_key(
                &records
                    .last()
                    .ok_or_else(|| anyhow!("PersonalDB locator page continuation has no row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        let row = decode_data_locator_row(&record.payload)?;
        validate_scope(&row)?;
        if core_meta_record_tuple_key(&record.key)?
            != personaldb_data_locator_tuple_key(row.tenant_id, &row.group_id, &row.data_id)?
        {
            bail!("PersonalDB data locator CoreMeta physical row key mismatch");
        }
        rows.push(row);
    }
    Ok(PersonalDbDataLocatorPage {
        rows,
        next_tuple_key,
    })
}

pub async fn read_personaldb_data_locator_bytes(
    storage: &Storage,
    row: &PersonalDbDataLocatorCoreMetaRow,
) -> Result<Vec<u8>> {
    let store = CoreStore::new(storage.clone()).await?;
    let manifest = store
        .read_logical_file_manifest(&row.payload_locator)
        .await
        .with_context(|| format!("read PersonalDB CoreMeta locator {}", row.data_id))?;
    store
        .read_logical_range(ReadLogicalRangeRequest {
            ranges: vec![CoreByteRange {
                start: 0,
                end_exclusive: manifest.logical_size,
            }],
            manifest,
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: row.tenant_id.to_string(),
                authz_realm_id: personaldb_realm_id(row.tenant_id),
            },
            expected_boundary: None,
            prefetch_policy: CorePrefetchPolicy::default(),
            trace_context: CoreTraceContext::default(),
        })
        .await
}

pub fn personaldb_data_locator_precondition(
    storage: &Storage,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
) -> Result<CoreMutationPrecondition> {
    let key = personaldb_data_locator_tuple_key(tenant_id, group_id, data_id)?;
    // A write precondition must compare the exact canonical bytes admitted by
    // the publication protocol, including absence, before staging a mutation.
    let payload = CoreMetaStore::open(storage.core_store_meta_path())?.get(
        CF_PERSONALDB,
        TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        &key,
    )?;
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_PERSONALDB.to_string(),
        table_id: TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        tuple_key: key,
        expected_payload_hash: payload
            .as_ref()
            .map(|payload| core_meta_payload_digest(TABLE_PERSONALDB_DATA_LOCATOR_ROW, payload)),
        require_absent: payload.is_none(),
        require_present: payload.is_some(),
    })
}

pub fn personaldb_group_precondition(
    storage: &Storage,
    tenant_id: i64,
    group_id: &str,
    generation: u64,
) -> Result<CoreMutationPrecondition> {
    let key = personaldb_group_tuple_key(tenant_id, group_id, generation)?;
    // This is a write-precondition snapshot, not a product read; the commit
    // protocol revalidates the same canonical row before publication.
    let payload = CoreMetaStore::open(storage.core_store_meta_path())?.get(
        CF_PERSONALDB,
        TABLE_PERSONALDB_GROUP_ROW,
        &key,
    )?;
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_PERSONALDB.to_string(),
        table_id: TABLE_PERSONALDB_GROUP_ROW,
        tuple_key: key,
        expected_payload_hash: payload
            .as_ref()
            .map(|payload| core_meta_payload_digest(TABLE_PERSONALDB_GROUP_ROW, payload)),
        require_absent: payload.is_none(),
        require_present: payload.is_some(),
    })
}

fn encode_group_row(row: &PersonalDbGroupCoreMetaRow) -> Result<Vec<u8>> {
    let locator = row
        .snapshot_locator
        .as_ref()
        .map(core_meta_locator_from_manifest_locator)
        .transpose()?;
    Ok(encode_deterministic_proto(&PersonalDbGroupRowProto {
        common: Some(core_meta_committed_row_common(
            personaldb_realm_id(row.tenant_id),
            personaldb_root_key_hash(row.tenant_id, &row.group_id),
            row.generation,
            &row.transaction_id,
            row.created_at_unix_nanos,
        )),
        group_id: row.group_id.clone(),
        replica_set_hash: row.replica_set_hash.clone(),
        witness_policy_hash: row.witness_policy_hash.clone(),
        latest_commit: row.latest_commit.clone(),
        snapshot_locator: locator,
    }))
}

fn decode_group_row(bytes: &[u8]) -> Result<PersonalDbGroupCoreMetaRow> {
    let proto =
        decode_deterministic_proto::<PersonalDbGroupRowProto>(bytes, "PersonalDB group row")?;
    let common = proto
        .common
        .ok_or_else(|| anyhow!("PersonalDB group row missing CoreMeta common"))?;
    Ok(PersonalDbGroupCoreMetaRow {
        tenant_id: tenant_id_from_realm(&common.realm_id)?,
        group_id: proto.group_id,
        generation: common.root_generation,
        replica_set_hash: proto.replica_set_hash,
        witness_policy_hash: proto.witness_policy_hash,
        latest_commit: proto.latest_commit,
        snapshot_locator: proto
            .snapshot_locator
            .as_ref()
            .map(core_meta_locator_to_manifest_locator)
            .transpose()?,
        transaction_id: common.transaction_id,
        created_at_unix_nanos: common.created_at_unix_nanos,
    })
}

fn encode_data_locator_row(row: &PersonalDbDataLocatorCoreMetaRow) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&PersonalDbDataLocatorRowProto {
        common: Some(core_meta_committed_row_common(
            personaldb_realm_id(row.tenant_id),
            personaldb_root_key_hash(row.tenant_id, &row.group_id),
            row.root_generation,
            &row.transaction_id,
            row.created_at_unix_nanos,
        )),
        group_id: row.group_id.clone(),
        data_id: row.data_id.clone(),
        data_kind: row.data_kind.clone(),
        sqlite_changeset_hash: row.sqlite_changeset_hash.clone(),
        payload_locator: Some(core_meta_locator_from_manifest_locator(
            &row.payload_locator,
        )?),
        projection_keys: row.projection_keys.clone(),
    }))
}

fn decode_data_locator_row(bytes: &[u8]) -> Result<PersonalDbDataLocatorCoreMetaRow> {
    let proto = decode_deterministic_proto::<PersonalDbDataLocatorRowProto>(
        bytes,
        "PersonalDB data locator row",
    )?;
    let common = proto
        .common
        .ok_or_else(|| anyhow!("PersonalDB data locator row missing CoreMeta common"))?;
    let payload_locator = proto
        .payload_locator
        .as_ref()
        .ok_or_else(|| anyhow!("PersonalDB data locator row missing locator"))
        .and_then(core_meta_locator_to_manifest_locator)?;
    let generation = payload_locator.manifest_ref.writer_generation;
    let row = PersonalDbDataLocatorCoreMetaRow {
        tenant_id: tenant_id_from_realm(&common.realm_id)?,
        group_id: proto.group_id,
        data_id: proto.data_id,
        data_kind: proto.data_kind,
        generation,
        root_generation: common.root_generation,
        sqlite_changeset_hash: proto.sqlite_changeset_hash,
        payload_locator,
        projection_keys: proto.projection_keys,
        transaction_id: common.transaction_id,
        created_at_unix_nanos: common.created_at_unix_nanos,
    };
    validate_data_locator_row(&row)?;
    Ok(row)
}

fn validate_coremeta_preconditions(
    storage: &Storage,
    preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    // These raw reads form local write preconditions. Candidate rows remain in
    // staging, and commit revalidates the same canonical bytes atomically.
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    for precondition in preconditions {
        let CoreMutationPrecondition::CoreMetaRow {
            cf,
            table_id,
            tuple_key,
            expected_payload_hash,
            require_absent,
            require_present,
        } = precondition
        else {
            bail!("PersonalDB CoreMeta writer received unsupported non-CoreMeta precondition");
        };
        let current = meta.get_named(cf, *table_id, tuple_key)?;
        if *require_absent && current.is_some() {
            bail!("PersonalDB CoreMeta precondition failed: row must be absent");
        }
        if *require_present && current.is_none() {
            bail!("PersonalDB CoreMeta precondition failed: row must be present");
        }
        if let (Some(expected), Some(current)) = (expected_payload_hash.as_ref(), current.as_ref())
        {
            let actual = core_meta_payload_digest(*table_id, current);
            if expected != &actual {
                bail!("PersonalDB CoreMeta precondition failed: payload hash mismatch");
            }
        }
    }
    Ok(())
}

fn validate_group_row(row: &PersonalDbGroupCoreMetaRow) -> Result<()> {
    validate_personaldb_scope(row.tenant_id, &row.group_id)?;
    if row.generation == 0 {
        bail!("PersonalDB group row generation must be nonzero");
    }
    require_nonempty(&row.transaction_id, "transaction_id")?;
    validate_optional_hash(&row.replica_set_hash, "replica_set_hash")?;
    validate_optional_hash(&row.witness_policy_hash, "witness_policy_hash")?;
    if !row.latest_commit.is_empty() {
        validate_optional_hash(&row.latest_commit, "latest_commit")?;
    }
    Ok(())
}

fn validate_data_locator_row(row: &PersonalDbDataLocatorCoreMetaRow) -> Result<()> {
    validate_personaldb_scope(row.tenant_id, &row.group_id)?;
    require_coremeta_ref_id(&row.data_id, "data_id")?;
    require_safe_component(&row.data_kind, "data_kind")?;
    if row.generation == 0 {
        bail!("PersonalDB data locator generation must be nonzero");
    }
    if row.root_generation == 0 {
        bail!("PersonalDB data locator root generation must be nonzero");
    }
    if row.payload_locator.manifest_ref.writer_generation != row.generation {
        bail!("PersonalDB data locator writer generation mismatch");
    }
    require_nonempty(&row.transaction_id, "transaction_id")?;
    if !row.sqlite_changeset_hash.is_empty() {
        validate_optional_hash(&row.sqlite_changeset_hash, "sqlite_changeset_hash")?;
    }
    Ok(())
}

fn personaldb_group_tuple_key(tenant_id: i64, group_id: &str, generation: u64) -> Result<Vec<u8>> {
    validate_personaldb_scope(tenant_id, group_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8(group_id),
        CoreMetaTuplePart::U64(generation),
    ])
}

fn personaldb_data_locator_tuple_key(
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
) -> Result<Vec<u8>> {
    validate_personaldb_scope(tenant_id, group_id)?;
    require_coremeta_ref_id(data_id, "data_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8(group_id),
        CoreMetaTuplePart::Utf8(data_id),
    ])
}

fn personaldb_data_locator_tuple_prefix(tenant_id: i64, group_id: &str) -> Result<Vec<u8>> {
    validate_personaldb_scope(tenant_id, group_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8(group_id),
    ])
}

fn validate_personaldb_scope(tenant_id: i64, group_id: &str) -> Result<()> {
    if tenant_id < 0 {
        bail!("PersonalDB tenant id must be nonnegative");
    }
    require_safe_component(group_id, "group_id")
}

pub(crate) fn personaldb_realm_id(tenant_id: i64) -> String {
    format!("tenant:{tenant_id}")
}

pub(crate) fn personaldb_root_anchor_key(tenant_id: i64, group_id: &str) -> String {
    format!("personaldb/{tenant_id}/{group_id}")
}

pub(crate) fn personaldb_root_key_hash(tenant_id: i64, group_id: &str) -> String {
    core_meta_root_key_hash(&personaldb_root_anchor_key(tenant_id, group_id))
}

pub(crate) fn tenant_id_from_realm(realm_id: &str) -> Result<i64> {
    let value = realm_id
        .strip_prefix("tenant:")
        .ok_or_else(|| anyhow!("PersonalDB CoreMeta realm is not tenant-scoped"))?;
    value
        .parse::<i64>()
        .context("PersonalDB CoreMeta realm tenant is invalid")
}

fn current_unix_nanos() -> Result<u64> {
    let nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("current timestamp cannot be represented in nanoseconds"))?;
    u64::try_from(nanos).context("current timestamp is negative")
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        bail!("{field} is not a safe component");
    }
    Ok(())
}

fn require_tuple_string(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value.contains('\0') || value.chars().any(char::is_control) {
        bail!("{field} contains an unsafe control character");
    }
    Ok(())
}

fn require_coremeta_ref_id(value: &str, field: &'static str) -> Result<()> {
    require_tuple_string(value, field)?;
    if value.contains('/') || value.contains('\\') {
        bail!("{field} must be a CoreMeta ref id, not a storage path");
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(())
}

fn validate_optional_hash(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    let hex_value = value
        .strip_prefix("blake3:")
        .or_else(|| value.strip_prefix("sha256:"))
        .unwrap_or(value);
    if hex_value.len() != 64 || !hex_value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{field} must be a 32 byte hash");
    }
    Ok(())
}

pub fn personaldb_payload_hash(bytes: &[u8]) -> String {
    hex::encode(hash32(bytes))
}

#[cfg(test)]
mod tests;
