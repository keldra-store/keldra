use crate::{
    core_store::{
        AuthzScopeRef, CF_PERSONALDB, CoreByteRange, CoreManifestLocator, CoreMetaLocatorProto,
        CoreMetaTuplePart, CorePrefetchPolicy, CoreStore, CoreTraceContext,
        ReadLogicalRangeRequest, TABLE_PERSONALDB_DATA_LOCATOR_ROW, TABLE_PERSONALDB_GROUP_ROW,
        WriteLogicalFileRequest, core_meta_committed_row_common,
        core_meta_locator_from_manifest_locator, core_meta_locator_to_manifest_locator,
        core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
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

/// All metadata made visible by one public PersonalDB operation.
///
/// Physical logical-file payloads may be written before this plan is committed:
/// their locators remain unreachable until the corresponding product rows are
/// staged here.  The plan acquires one assignment guard and receives one MVCC
/// commit version for the complete product mutation set.
#[derive(Debug)]
pub struct PersonalDbWritePlan {
    tenant_id: i64,
    group_id: String,
    principal: String,
    idempotency_key: String,
    assignment: Option<crate::mvcc_worker_authority::AssignmentGuard>,
    mutations: Vec<crate::mvcc_product::ProductMutation>,
    predicates: Vec<(
        crate::mvcc_transaction::LogicalKey,
        Option<crate::mvcc_transaction::PredicateKind>,
    )>,
}

impl PersonalDbWritePlan {
    pub async fn resolved_commit_version(
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        principal: &str,
        idempotency_key: &str,
    ) -> Result<Option<u64>> {
        let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
        let Some(status) = mvcc
            .open_transactions
            .status_by_idempotency(
                mvcc.cluster_id(),
                idempotency_key,
                principal,
                now,
            )
            ?
        else {
            return Ok(None);
        };
        match status.result {
            Some(crate::mvcc_transaction::CertificationResult::Committed {
                commit_version,
            }) => Ok(Some(commit_version)),
            Some(crate::mvcc_transaction::CertificationResult::Aborted { reason }) => {
                bail!("PersonalDB MVCC write plan previously aborted: {reason:?}")
            }
            None if status.state == "open" => Ok(None),
            None if status.state == "committing" => {
                let outcome = mvcc
                    .open_transactions
                    .commit(
                        mvcc.runtime.as_ref(),
                        &status.transaction_id,
                        principal,
                        now,
                    )
                    .await?;
                match outcome.certification {
                    crate::mvcc_transaction::CertificationResult::Committed {
                        commit_version,
                    } => Ok(Some(commit_version)),
                    crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
                        bail!("PersonalDB MVCC write plan aborted while resuming: {reason:?}")
                    }
                }
            }
            None => bail!(
                "PersonalDB MVCC write plan is not retryable while transaction is {}",
                status.state
            ),
        }
    }

    pub fn new(
        tenant_id: i64,
        group_id: impl Into<String>,
        principal: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> Result<Self> {
        let group_id = group_id.into();
        validate_personaldb_scope(tenant_id, &group_id)?;
        let principal = principal.into();
        let idempotency_key = idempotency_key.into();
        if principal.is_empty() || idempotency_key.is_empty() {
            bail!("PersonalDB write plan principal and idempotency key must be nonempty");
        }
        Ok(Self {
            tenant_id,
            group_id,
            principal,
            idempotency_key,
            assignment: None,
            mutations: Vec::new(),
            predicates: Vec::new(),
        })
    }

    pub fn with_assignment_guard(
        mut self,
        assignment: crate::mvcc_worker_authority::AssignmentGuard,
    ) -> Self {
        self.assignment = Some(assignment);
        self
    }

    pub fn stage_put(
        &mut self,
        key: crate::mvcc_transaction::LogicalKey,
        payload: Vec<u8>,
        predicate: crate::mvcc_transaction::PredicateKind,
    ) {
        self.mutations
            .push(crate::mvcc_product::ProductMutation::put(key.clone(), payload));
        self.predicates.push((key, Some(predicate)));
    }

    pub fn stage_data_locator_row(
        &mut self,
        row: &PersonalDbDataLocatorCoreMetaRow,
    ) -> Result<()> {
        validate_data_locator_row(row)?;
        self.require_scope(row.tenant_id, &row.group_id)?;
        let tuple =
            personaldb_data_locator_tuple_key(row.tenant_id, &row.group_id, &row.data_id)?;
        self.stage_coremeta_row(
            TABLE_PERSONALDB_DATA_LOCATOR_ROW,
            tuple,
            encode_data_locator_row(row)?,
        )
    }

    pub fn stage_group_row(
        &mut self,
        row: &PersonalDbGroupCoreMetaRow,
    ) -> Result<()> {
        validate_group_row(row)?;
        self.require_scope(row.tenant_id, &row.group_id)?;
        let tuple = personaldb_group_tuple_key(row.tenant_id, &row.group_id, row.generation)?;
        self.stage_coremeta_row(
            TABLE_PERSONALDB_GROUP_ROW,
            tuple,
            encode_group_row(row)?,
        )
    }

    fn stage_coremeta_row(
        &mut self,
        table_id: u16,
        tuple_key: Vec<u8>,
        payload: Vec<u8>,
    ) -> Result<()> {
        let key = crate::mvcc_product::coremeta_logical_key(CF_PERSONALDB, table_id, &tuple_key)?;
        self.mutations
            .push(crate::mvcc_product::ProductMutation::put(key.clone(), payload));
        // Resolve replace semantics at the transaction's fixed snapshot, not
        // while a potentially long-lived plan is being assembled.
        self.predicates.push((key, None));
        Ok(())
    }

    fn require_scope(&self, tenant_id: i64, group_id: &str) -> Result<()> {
        if tenant_id != self.tenant_id || group_id != self.group_id {
            bail!("PersonalDB write plan cannot span group assignments");
        }
        Ok(())
    }

    pub async fn commit(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    ) -> Result<u64> {
        if self.mutations.is_empty() {
            bail!("PersonalDB write plan has no product mutations");
        }
        let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
        let handle = mvcc
            .open_transactions
            .begin(
                mvcc.runtime.as_ref(),
                mvcc.cluster_id(),
                &self.principal,
                &self.idempotency_key,
                std::time::Duration::from_secs(30),
                crate::mvcc_transaction::DurabilityLevel::Quorum,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                now,
            )
            .await?;
        let status =
            mvcc.open_transactions
                .status(&handle.transaction_id, &self.principal, now)?;
        if status.state == "open" {
            let principal = self.principal.clone();
            self.stage_into_transaction(
                mvcc,
                &handle.transaction_id,
                &principal,
                now,
            )
            .await?;
        }
        let outcome = mvcc
            .open_transactions
            .commit(
                mvcc.runtime.as_ref(),
                &handle.transaction_id,
                &self.principal,
                now,
            )
            .await?;
        match outcome.certification {
            crate::mvcc_transaction::CertificationResult::Committed { commit_version } => {
                Ok(commit_version)
            }
            crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
                bail!("PersonalDB MVCC write plan aborted: {reason:?}")
            }
        }
    }

    pub async fn stage_into_transaction(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<()> {
        if self.mutations.is_empty() {
            bail!("PersonalDB write plan has no product mutations");
        }
        if principal != self.principal {
            bail!("PersonalDB write plan principal does not own caller transaction");
        }
        let handle = mvcc.open_transactions.handle(transaction_id)?;
        if handle.principal != principal {
            bail!("PersonalDB caller transaction principal mismatch");
        }
        let logical_identity =
            format!("tenant/{}/personaldb/{}", self.tenant_id, self.group_id);
        let assignment = match self.assignment {
            Some(assignment) => {
                let expected_partition = crate::mvcc_worker_authority::work_partition_id(
                    "personaldb-write",
                    &logical_identity,
                )?;
                if assignment.partition_id != expected_partition {
                    bail!("PersonalDB write plan assignment scope mismatch");
                }
                assignment
            }
            None => mvcc
                .reconcile_work_assignment("personaldb-write", &logical_identity)
                .await?
                .ok_or_else(|| anyhow!("local node does not own PersonalDB group assignment"))?,
        };
        mvcc.stage_product_mutations(transaction_id, principal, self.mutations, now_unix_ms)?;
        for (key, predicate) in self.predicates {
            let predicate = match predicate {
                Some(predicate) => predicate,
                None => mvcc
                    .runtime
                    .read_at(&key, handle.snapshot_version)?
                    .map(|current| {
                        crate::mvcc_transaction::PredicateKind::ValueHash(
                            *blake3::hash(&current.value).as_bytes(),
                        )
                    })
                    .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent),
            };
            mvcc.stage_predicate(transaction_id, principal, key, predicate, now_unix_ms)?;
        }
        mvcc.stage_assignment_guard(transaction_id, principal, &assignment, now_unix_ms)?;
        Ok(())
    }
}

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

#[allow(clippy::too_many_arguments)]
pub async fn write_personaldb_bytes_as_data_locator_mvcc(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
    data_kind: &str,
    generation: u64,
    bytes: Vec<u8>,
    sqlite_changeset_hash: String,
    projection_keys: Vec<String>,
    transaction_id: String,
    principal: &str,
) -> Result<PersonalDbDataLocatorCoreMetaRow> {
    let root_generation = mvcc
        .runtime
        .applied_version()?
        .checked_add(1)
        .ok_or_else(|| anyhow!("PersonalDB locator generation overflow"))?;
    let row = prepare_personaldb_bytes_as_data_locator(
        storage,
        tenant_id,
        group_id,
        data_id,
        data_kind,
        generation,
        root_generation,
        bytes,
        sqlite_changeset_hash,
        projection_keys,
        transaction_id,
    )
    .await?;
    write_personaldb_data_locator_row_mvcc(mvcc, &row, principal).await?;
    Ok(row)
}

#[allow(clippy::too_many_arguments)]
pub async fn prepare_personaldb_bytes_as_data_locator(
    storage: &Storage,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
    data_kind: &str,
    generation: u64,
    root_generation: u64,
    bytes: Vec<u8>,
    sqlite_changeset_hash: String,
    projection_keys: Vec<String>,
    transaction_id: String,
) -> Result<PersonalDbDataLocatorCoreMetaRow> {
    validate_personaldb_scope(tenant_id, group_id)?;
    if root_generation == 0 {
        bail!("PersonalDB locator root generation must be nonzero");
    }
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::PersonalDb,
        generation,
        data_id,
        &hash32(&bytes),
    );
    let logical = CoreStore::new(storage.clone())
        .await?
        .write_logical_file_with_locator(WriteLogicalFileRequest {
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
        })
        .await?;
    let row = PersonalDbDataLocatorCoreMetaRow {
        tenant_id,
        group_id: group_id.to_string(),
        data_id: data_id.to_string(),
        data_kind: data_kind.to_string(),
        generation,
        root_generation,
        sqlite_changeset_hash,
        payload_locator: logical.locator,
        projection_keys,
        transaction_id,
        created_at_unix_nanos: current_unix_nanos()?,
    };
    Ok(row)
}

#[allow(clippy::too_many_arguments)]
pub async fn write_personaldb_logical_file_as_data_locator_mvcc(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
    data_kind: &str,
    request: WriteLogicalFileRequest,
    sqlite_changeset_hash: String,
    projection_keys: Vec<String>,
    transaction_id: String,
    principal: &str,
) -> Result<PersonalDbDataLocatorCoreMetaRow> {
    let root_generation = mvcc
        .runtime
        .applied_version()?
        .checked_add(1)
        .ok_or_else(|| anyhow!("PersonalDB locator generation overflow"))?;
    let row = prepare_personaldb_logical_file_as_data_locator(
        storage,
        tenant_id,
        group_id,
        data_id,
        data_kind,
        request,
        root_generation,
        sqlite_changeset_hash,
        projection_keys,
        transaction_id,
    )
    .await?;
    write_personaldb_data_locator_row_mvcc(mvcc, &row, principal).await?;
    Ok(row)
}

#[allow(clippy::too_many_arguments)]
pub async fn prepare_personaldb_logical_file_as_data_locator(
    storage: &Storage,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
    data_kind: &str,
    request: WriteLogicalFileRequest,
    root_generation: u64,
    sqlite_changeset_hash: String,
    projection_keys: Vec<String>,
    transaction_id: String,
) -> Result<PersonalDbDataLocatorCoreMetaRow> {
    validate_personaldb_scope(tenant_id, group_id)?;
    require_coremeta_ref_id(data_id, "data_id")?;
    require_safe_component(data_kind, "data_kind")?;
    if request.generation == 0 {
        bail!("PersonalDB data locator generation must be nonzero");
    }
    if root_generation == 0 {
        bail!("PersonalDB locator root generation must be nonzero");
    }
    let generation = request.generation;
    let logical = CoreStore::new(storage.clone())
        .await?
        .write_logical_file_with_locator(request)
        .await?;
    let row = PersonalDbDataLocatorCoreMetaRow {
        tenant_id,
        group_id: group_id.to_string(),
        data_id: data_id.to_string(),
        data_kind: data_kind.to_string(),
        generation,
        root_generation,
        sqlite_changeset_hash,
        payload_locator: logical.locator,
        projection_keys,
        transaction_id,
        created_at_unix_nanos: current_unix_nanos()?,
    };
    Ok(row)
}

pub fn personaldb_partition_id(tenant_id: i64, group_id: &str) -> String {
    format!("personaldb:tenant:{tenant_id}:group:{group_id}")
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

pub fn read_personaldb_data_locator_row_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
    snapshot_version: u64,
) -> Result<Option<PersonalDbDataLocatorCoreMetaRow>> {
    let tuple = personaldb_data_locator_tuple_key(tenant_id, group_id, data_id)?;
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_PERSONALDB,
        TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        &tuple,
    )?;
    mvcc.runtime
        .read_at(&key, snapshot_version)?
        .map(|row| decode_data_locator_row(&row.value))
        .transpose()
}

pub fn read_personaldb_data_locator_row_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
) -> Result<Option<PersonalDbDataLocatorCoreMetaRow>> {
    let tuple = personaldb_data_locator_tuple_key(tenant_id, group_id, data_id)?;
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_PERSONALDB,
        TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        &tuple,
    )?;
    mvcc.read_transaction_value(transaction_id, principal, &key)?
        .map(|value| decode_data_locator_row(&value))
        .transpose()
}

pub fn list_personaldb_data_locator_rows_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    group_id: &str,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
    snapshot_version: u64,
) -> Result<PersonalDbDataLocatorPage> {
    let tuple_prefix = personaldb_data_locator_tuple_prefix(tenant_id, group_id)?;
    page_personaldb_data_locator_rows_at_snapshot(
        mvcc,
        &tuple_prefix,
        after_tuple_key,
        page_size,
        snapshot_version,
        |row| {
            if row.tenant_id != tenant_id || row.group_id != group_id {
                bail!("PersonalDB data locator CoreMeta row scope mismatch");
            }
            Ok(())
        },
    )
}

pub fn list_personaldb_data_locator_rows_for_tenant_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
    snapshot_version: u64,
) -> Result<PersonalDbDataLocatorPage> {
    if tenant_id < 0 {
        bail!("PersonalDB tenant id must be nonnegative");
    }
    let tuple_prefix =
        core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id))])?;
    page_personaldb_data_locator_rows_at_snapshot(
        mvcc,
        &tuple_prefix,
        after_tuple_key,
        page_size,
        snapshot_version,
        |row| {
            if row.tenant_id != tenant_id {
                bail!("PersonalDB data locator CoreMeta tenant scope mismatch");
            }
            Ok(())
        },
    )
}

fn page_personaldb_data_locator_rows_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tuple_prefix: &[u8],
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
    snapshot_version: u64,
    validate_scope: impl Fn(&PersonalDbDataLocatorCoreMetaRow) -> Result<()>,
) -> Result<PersonalDbDataLocatorPage> {
    if !(1..=PERSONALDB_DATA_LOCATOR_PAGE_MAX).contains(&page_size) {
        bail!(
            "PersonalDB data locator page size must be between 1 and {PERSONALDB_DATA_LOCATOR_PAGE_MAX}"
        );
    }
    if after_tuple_key.is_some_and(|cursor| !cursor.starts_with(tuple_prefix)) {
        bail!("PersonalDB data locator cursor is outside the requested scope");
    }
    let prefix = crate::mvcc_product::coremeta_application_prefix(CF_PERSONALDB, tuple_prefix)?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        &prefix,
        snapshot_version,
    )?;
    if let Some(after) = after_tuple_key {
        rows.retain(|(key, _)| {
            crate::mvcc_product::coremeta_tuple_from_logical_key(key, CF_PERSONALDB)
                .is_ok_and(|tuple| tuple > after)
        });
    }
    let has_more = rows.len() > page_size;
    if has_more {
        rows.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            crate::mvcc_product::coremeta_tuple_from_logical_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("PersonalDB locator page lost final row"))?
                    .0,
                CF_PERSONALDB,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let rows = rows
        .into_iter()
        .map(|(_, row)| {
            let row = decode_data_locator_row(&row.value)?;
            validate_scope(&row)?;
            Ok(row)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PersonalDbDataLocatorPage {
        rows,
        next_tuple_key,
    })
}

pub async fn write_personaldb_data_locator_row_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    row: &PersonalDbDataLocatorCoreMetaRow,
    principal: &str,
) -> Result<u64> {
    validate_data_locator_row(row)?;
    let tuple = personaldb_data_locator_tuple_key(row.tenant_id, &row.group_id, &row.data_id)?;
    write_personaldb_product_row_mvcc(
        mvcc,
        row.tenant_id,
        &row.group_id,
        principal,
        &row.transaction_id,
        TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        tuple,
        encode_data_locator_row(row)?,
    )
    .await
}

pub async fn write_personaldb_group_row_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    row: &PersonalDbGroupCoreMetaRow,
    principal: &str,
) -> Result<u64> {
    validate_group_row(row)?;
    let tuple = personaldb_group_tuple_key(row.tenant_id, &row.group_id, row.generation)?;
    write_personaldb_product_row_mvcc(
        mvcc,
        row.tenant_id,
        &row.group_id,
        principal,
        &row.transaction_id,
        TABLE_PERSONALDB_GROUP_ROW,
        tuple,
        encode_group_row(row)?,
    )
    .await
}

pub(crate) async fn write_personaldb_product_row_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    group_id: &str,
    principal: &str,
    idempotency_key: &str,
    table_id: u16,
    tuple_key: Vec<u8>,
    payload: Vec<u8>,
) -> Result<u64> {
    let mut plan =
        PersonalDbWritePlan::new(tenant_id, group_id, principal, idempotency_key)?;
    plan.stage_coremeta_row(table_id, tuple_key, payload)?;
    plan.commit(mvcc).await
}

pub fn read_personaldb_group_row_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    group_id: &str,
    generation: u64,
    snapshot_version: u64,
) -> Result<Option<PersonalDbGroupCoreMetaRow>> {
    let tuple = personaldb_group_tuple_key(tenant_id, group_id, generation)?;
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_PERSONALDB,
        TABLE_PERSONALDB_GROUP_ROW,
        &tuple,
    )?;
    mvcc.runtime
        .read_at(&key, snapshot_version)?
        .map(|row| decode_group_row(&row.value))
        .transpose()
}

pub fn list_personaldb_group_rows_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    group_id: &str,
    snapshot_version: u64,
) -> Result<Vec<PersonalDbGroupCoreMetaRow>> {
    validate_personaldb_scope(tenant_id, group_id)?;
    let tuple_prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8(group_id),
    ])?;
    let prefix = crate::mvcc_product::coremeta_application_prefix(CF_PERSONALDB, &tuple_prefix)?;
    mvcc.runtime
        .scan_table_prefix_at(TABLE_PERSONALDB_GROUP_ROW, &prefix, snapshot_version)?
        .into_iter()
        .map(|(_, row)| decode_group_row(&row.value))
        .collect()
}
