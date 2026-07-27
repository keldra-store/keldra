use crate::core_store::{
    CF_MESH, CoreMetaTuplePart, CoreMutationOperation, TABLE_CONTROL_CURRENT_ROW,
    core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::PartitionWritePermit;
use crate::persistence::{App, AppDetails, Tenant};
use crate::storage::Storage;
use anyhow::{Result, anyhow, bail};
use prost::{Message, Oneof};
use std::collections::BTreeSet;

const CONTROL_EVENT_SCHEMA: &str = "anvil.control.event.v1";
const CONTROL_CURRENT_SCHEMA: &str = "anvil.control.current.v1";
const CONTROL_CURRENT_TARGET_MAX_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ControlEventBody {
    RegionUpsert {
        name: String,
    },
    TenantUpsert {
        id: i64,
        name: String,
    },
    AppCreate {
        id: i64,
        tenant_id: i64,
        name: String,
        client_id: String,
        client_secret_encrypted: Vec<u8>,
    },
    AppSecretUpdate {
        app_id: i64,
        client_secret_encrypted: Vec<u8>,
    },
    AppDelete {
        app_id: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ControlCurrentRecord {
    Revision {
        revision: u64,
    },
    IdAllocator {
        max_allocated_id: i64,
    },
    Region {
        name: String,
        active: bool,
    },
    Tenant {
        id: i64,
        name: String,
        active: bool,
    },
    App {
        id: i64,
        tenant_id: i64,
        name: String,
        client_id: String,
        client_secret_encrypted: Vec<u8>,
        active: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredControlApp {
    id: i64,
    tenant_id: i64,
    name: String,
    client_id: String,
    client_secret_encrypted: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionAppDetails {
    pub app: App,
    pub tenant_id: i64,
    pub client_secret_encrypted: Vec<u8>,
}

mod current;

pub use current::{CurrentAppPage, CurrentRegionPage, CurrentTenantPage};

#[derive(Clone, PartialEq, Message)]
struct ControlEventProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    emitted_at: String,
    #[prost(uint64, tag = "3")]
    fence_token: u64,
    #[prost(string, tag = "4")]
    mutation_id: String,
    #[prost(oneof = "control_event_proto::Event", tags = "10, 11, 12, 13, 14")]
    event: Option<control_event_proto::Event>,
}

mod control_event_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Event {
        #[prost(message, tag = "10")]
        RegionUpsert(super::RegionUpsertProto),
        #[prost(message, tag = "11")]
        TenantUpsert(super::TenantUpsertProto),
        #[prost(message, tag = "12")]
        AppCreate(super::AppCreateProto),
        #[prost(message, tag = "13")]
        AppSecretUpdate(super::AppSecretUpdateProto),
        #[prost(message, tag = "14")]
        AppDelete(super::AppDeleteProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ControlCurrentProto {
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(oneof = "control_current_proto::Record", tags = "10, 11, 12, 13, 14")]
    record: Option<control_current_proto::Record>,
}

mod control_current_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Record {
        #[prost(message, tag = "10")]
        IdAllocator(super::IdAllocatorCurrentProto),
        #[prost(message, tag = "11")]
        Region(super::RegionCurrentProto),
        #[prost(message, tag = "12")]
        Tenant(super::TenantCurrentProto),
        #[prost(message, tag = "13")]
        App(super::AppCurrentProto),
        #[prost(message, tag = "14")]
        Revision(super::RevisionCurrentProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct RevisionCurrentProto {
    #[prost(uint64, tag = "1")]
    revision: u64,
}

#[derive(Clone, PartialEq, Message)]
struct RegionUpsertProto {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct TenantUpsertProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct AppCreateProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    name: String,
    #[prost(string, tag = "4")]
    client_id: String,
    #[prost(bytes, tag = "5")]
    client_secret_encrypted: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct AppSecretUpdateProto {
    #[prost(int64, tag = "1")]
    app_id: i64,
    #[prost(bytes, tag = "2")]
    client_secret_encrypted: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct AppDeleteProto {
    #[prost(int64, tag = "1")]
    app_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct IdAllocatorCurrentProto {
    #[prost(int64, tag = "1")]
    max_allocated_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct RegionCurrentProto {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(bool, tag = "2")]
    active: bool,
}

#[derive(Clone, PartialEq, Message)]
struct TenantCurrentProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(bool, tag = "3")]
    active: bool,
}

#[derive(Clone, PartialEq, Message)]
struct AppCurrentProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    name: String,
    #[prost(string, tag = "4")]
    client_id: String,
    #[prost(bytes, tag = "5")]
    client_secret_encrypted: Vec<u8>,
    #[prost(bool, tag = "6")]
    active: bool,
}

pub(crate) async fn create_region_with_permit_mvcc(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    name: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<bool> {
    require_control_permit(permit)?;
    if matches!(
        read_control_current_mvcc(mvcc, &region_tuple_key(name)?)?,
        Some(ControlCurrentRecord::Region { active: true, .. })
    ) {
        return Ok(false);
    }
    append_control_event_mvcc(
        mvcc,
        ControlEventBody::RegionUpsert {
            name: name.to_string(),
        },
        vec![ControlCurrentRecord::Region {
            name: name.to_string(),
            active: true,
        }],
        permit.fence_token,
        None,
        None,
    )
    .await?;
    Ok(true)
}

pub fn current_control_collection_revision_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
) -> Result<String> {
    match read_control_current_mvcc(mvcc, &control_revision_tuple_key()?)? {
        Some(ControlCurrentRecord::Revision { revision }) => Ok(revision.to_string()),
        Some(_) => bail!("control revision key contains a different record type"),
        None => Ok("0".to_string()),
    }
}

pub fn page_regions_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    expected_revision: &str,
    after_application_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentRegionPage> {
    if page_size == 0 || page_size > 1_000 {
        bail!("region page size must be between 1 and 1000");
    }
    let current_revision = current_control_collection_revision_mvcc(mvcc)?;
    if current_revision != expected_revision {
        bail!(
            "control region collection revision changed: expected {expected_revision}, actual {current_revision}"
        );
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let tuple_prefix = region_tuple_prefix()?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_MESH, &tuple_prefix)?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_CONTROL_CURRENT_ROW,
        &application_prefix,
        snapshot,
    )?;
    if let Some(after) = after_application_key {
        rows.retain(|(key, _)| key.application_key.as_slice() > after);
    }
    let has_more = rows.len() > page_size;
    rows.truncate(page_size);
    let next_tuple_key = has_more
        .then(|| rows.last().map(|(key, _)| key.application_key.clone()))
        .flatten();
    let mut regions = Vec::new();
    for (_, row) in rows {
        match decode_control_current_row(&row.value)? {
            ControlCurrentRecord::Region { name, active: true } => regions.push(name),
            ControlCurrentRecord::Region { active: false, .. } => {}
            _ => bail!("control region collection contains a different record type"),
        }
    }
    let final_revision = current_control_collection_revision_mvcc(mvcc)?;
    if final_revision != expected_revision {
        bail!(
            "control region collection revision changed: expected {expected_revision}, actual {final_revision}"
        );
    }
    Ok(CurrentRegionPage {
        regions,
        next_tuple_key,
    })
}

pub(crate) async fn create_tenant_with_permit_mvcc(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    name: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
    admin_audit_event: Option<&crate::admin_audit::AdminAuditEvent>,
) -> Result<Tenant> {
    require_control_permit(permit)?;
    if let Some(existing) = read_tenant_by_name_mvcc(mvcc, name)? {
        return Ok(existing);
    }
    let max_allocated_id = match read_control_current_mvcc(mvcc, &id_allocator_tuple_key()?)? {
        Some(ControlCurrentRecord::IdAllocator { max_allocated_id }) => max_allocated_id,
        Some(_) => bail!("control ID allocator row contains a different record type"),
        None => 0,
    };
    let tenant = Tenant {
        id: max_allocated_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("control ID allocator overflow"))?,
        name: name.to_string(),
    };
    append_control_event_mvcc(
        mvcc,
        ControlEventBody::TenantUpsert {
            id: tenant.id,
            name: tenant.name.clone(),
        },
        vec![
            ControlCurrentRecord::IdAllocator {
                max_allocated_id: tenant.id,
            },
            ControlCurrentRecord::Tenant {
                id: tenant.id,
                name: tenant.name.clone(),
                active: true,
            },
        ],
        permit.fence_token,
        None,
        admin_audit_event,
    )
    .await?;
    Ok(tenant)
}

#[derive(Debug)]
pub(crate) struct ControlTenantMutationPlan {
    pub tenant: Tenant,
    mutations: Vec<crate::mvcc_product::ProductMutation>,
    predicates: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    outbox_events: Vec<crate::mvcc_outbox::StreamOutboxEvent>,
}

impl ControlTenantMutationPlan {
    pub(crate) async fn stage(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<Tenant> {
        mvcc.stage_product_mutations(transaction_id, principal, self.mutations, now_unix_ms)?;
        for (key, predicate) in self.predicates {
            mvcc.stage_predicate(transaction_id, principal, key, predicate, now_unix_ms)?;
        }
        for event in self.outbox_events {
            mvcc.open_transactions
                .add_stream_event(transaction_id, event, now_unix_ms)?;
        }
        let assignment = mvcc
            .reconcile_work_assignment("control-plane", mvcc.cluster_id())
            .await?
            .ok_or_else(|| anyhow!("this node does not own the cluster control-plane assignment"))?;
        mvcc.stage_assignment_guard(transaction_id, principal, &assignment, now_unix_ms)?;
        Ok(self.tenant)
    }
}

pub(crate) fn plan_create_tenant_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    name: &str,
    admin_audit_event: &crate::admin_audit::AdminAuditEvent,
) -> Result<ControlTenantMutationPlan> {
    use crate::mvcc_transaction::PredicateKind;

    if read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &tenant_name_tuple_key(name)?,
    )?
    .is_some()
    {
        bail!("tenant already exists");
    }
    let max_allocated_id = match read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &id_allocator_tuple_key()?,
    )? {
        Some(ControlCurrentRecord::IdAllocator { max_allocated_id }) => max_allocated_id,
        Some(_) => bail!("control ID allocator row contains a different record type"),
        None => 0,
    };
    let tenant = Tenant {
        id: max_allocated_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("control ID allocator overflow"))?,
        name: name.to_string(),
    };
    let revision_tuple_key = control_revision_tuple_key()?;
    let revision = match read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &revision_tuple_key,
    )? {
        Some(ControlCurrentRecord::Revision { revision }) => revision,
        Some(_) => bail!("control revision key contains a different record type"),
        None => 0,
    };
    let next_revision = revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("control journal revision overflow"))?;
    let mutation_id = deterministic_control_mutation_id(transaction_id, "tenant-create");
    let mutation_id_string = mutation_id.to_string();
    let mut operations = Vec::new();
    for record in [
        ControlCurrentRecord::IdAllocator {
            max_allocated_id: tenant.id,
        },
        ControlCurrentRecord::Tenant {
            id: tenant.id,
            name: tenant.name.clone(),
            active: true,
        },
        ControlCurrentRecord::Revision {
            revision: next_revision,
        },
    ] {
        operations.extend(control_current_updates(record)?);
    }
    operations.push(CoreMutationOperation::StreamAppend {
        partition_id: hex::encode(control_partition_id()),
        stream_id: control_plane_stream_id(),
        record_kind: "control_plane".to_string(),
        payload: encode_control_event_body(
            &ControlEventBody::TenantUpsert {
                id: tenant.id,
                name: tenant.name.clone(),
            },
            0,
            mutation_id,
        )?,
        idempotency_key: Some(format!("control-plane:{mutation_id_string}")),
    });
    let mut predicates = Vec::new();
    let mut predicate_keys = BTreeSet::new();
    for operation in &operations {
        let (cf, table_id, tuple_key) = match operation {
            CoreMutationOperation::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                ..
            }
            | CoreMutationOperation::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
            } => (cf, *table_id, tuple_key),
            CoreMutationOperation::StreamAppend { .. } => continue,
        };
        let key = crate::mvcc_product::coremeta_logical_key(cf, table_id, tuple_key)?;
        if predicate_keys.insert(key.clone()) {
            predicates.push((
                key.clone(),
                mvcc.read_latest_value(&key)?
                    .map(|payload| PredicateKind::ValueHash(*blake3::hash(&payload).as_bytes()))
                    .unwrap_or(PredicateKind::Absent),
            ));
        }
    }
    let mut product =
        crate::mvcc_product::product_mutations_and_outbox_from_operations(operations)?;
    let audit = crate::admin_audit::admin_audit_mvcc_plan(
        admin_audit_event,
        next_revision,
        &mutation_id_string,
    )?;
    product.mutations.extend(audit.mutations);
    product.outbox_events.extend(audit.outbox_events);
    predicates.extend(product.predicates);
    Ok(ControlTenantMutationPlan {
        tenant,
        mutations: product.mutations,
        predicates,
        outbox_events: product.outbox_events,
    })
}

pub fn read_tenant_by_name_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    name: &str,
) -> Result<Option<Tenant>> {
    match read_control_current_mvcc(mvcc, &tenant_name_tuple_key(name)?)? {
        Some(ControlCurrentRecord::Tenant {
            id,
            name: stored_name,
            active,
        }) if stored_name == name => Ok(active.then_some(Tenant {
            id,
            name: stored_name,
        })),
        Some(ControlCurrentRecord::Tenant { .. }) => {
            bail!("control tenant-name row does not match its key")
        }
        Some(_) => bail!("control tenant-name row contains a different record type"),
        None => Ok(None),
    }
}

pub fn read_app_by_id_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    app_id: i64,
) -> Result<Option<App>> {
    let Some(app) = read_stored_app_mvcc(mvcc, &app_id_tuple_key(app_id)?)? else {
        return Ok(None);
    };
    if app.id != app_id {
        bail!("control app-id row does not match its key");
    }
    Ok(Some(App {
        id: app.id,
        name: app.name,
        client_id: app.client_id,
    }))
}

pub fn read_app_by_tenant_name_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    name: &str,
) -> Result<Option<App>> {
    let Some(app) = read_stored_app_mvcc(mvcc, &app_tenant_name_tuple_key(tenant_id, name)?)?
    else {
        return Ok(None);
    };
    if app.tenant_id != tenant_id || app.name != name {
        bail!("control tenant-app row does not match its key");
    }
    Ok(Some(App {
        id: app.id,
        name: app.name,
        client_id: app.client_id,
    }))
}

pub fn read_app_details_by_client_id_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    client_id: &str,
) -> Result<Option<AppDetails>> {
    let Some(app) = read_stored_app_mvcc(mvcc, &app_client_id_tuple_key(client_id)?)? else {
        return Ok(None);
    };
    if app.client_id != client_id {
        bail!("control app-client row does not match its key");
    }
    Ok(Some(AppDetails {
        id: app.id,
        tenant_id: app.tenant_id,
        client_secret_encrypted: app.client_secret_encrypted,
    }))
}

fn read_stored_app_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tuple_key: &[u8],
) -> Result<Option<StoredControlApp>> {
    match read_control_current_mvcc(mvcc, tuple_key)? {
        Some(ControlCurrentRecord::App {
            id,
            tenant_id,
            name,
            client_id,
            client_secret_encrypted,
            active: true,
        }) => Ok(Some(StoredControlApp {
            id,
            tenant_id,
            name,
            client_id,
            client_secret_encrypted,
        })),
        Some(ControlCurrentRecord::App { active: false, .. }) | None => Ok(None),
        Some(_) => bail!("control application row contains a different record type"),
    }
}

pub fn page_apps_for_tenant_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    expected_revision: &str,
    after_application_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentAppPage> {
    if page_size == 0 || page_size > 1_000 {
        bail!("application page size must be between 1 and 1000");
    }
    if current_control_collection_revision_mvcc(mvcc)? != expected_revision {
        bail!("control application collection revision changed");
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let tuple_prefix = app_tenant_name_tuple_prefix(tenant_id)?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_MESH, &tuple_prefix)?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_CONTROL_CURRENT_ROW,
        &application_prefix,
        snapshot,
    )?;
    if let Some(after) = after_application_key {
        rows.retain(|(key, _)| key.application_key.as_slice() > after);
    }
    let has_more = rows.len() > page_size;
    rows.truncate(page_size);
    let next_tuple_key = has_more
        .then(|| rows.last().map(|(key, _)| key.application_key.clone()))
        .flatten();
    let mut apps = Vec::new();
    for (_, row) in rows {
        match decode_control_current_row(&row.value)? {
            ControlCurrentRecord::App {
                id,
                tenant_id: row_tenant_id,
                name,
                client_id,
                active: true,
                ..
            } if row_tenant_id == tenant_id => apps.push(App {
                id,
                name,
                client_id,
            }),
            ControlCurrentRecord::App { .. } => {
                bail!("tenant application collection contains an invalid row")
            }
            _ => bail!("tenant application collection contains a different record type"),
        }
    }
    if current_control_collection_revision_mvcc(mvcc)? != expected_revision {
        bail!("control application collection revision changed");
    }
    Ok(CurrentAppPage {
        apps,
        next_tuple_key,
    })
}

pub fn page_tenants_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    expected_revision: &str,
    after_application_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentTenantPage> {
    if page_size == 0 || page_size > 1_000 {
        bail!("tenant page size must be between 1 and 1000");
    }
    if current_control_collection_revision_mvcc(mvcc)? != expected_revision {
        bail!("control tenant collection revision changed");
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let tuple_prefix = tenant_id_tuple_prefix()?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_MESH, &tuple_prefix)?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_CONTROL_CURRENT_ROW,
        &application_prefix,
        snapshot,
    )?;
    if let Some(after) = after_application_key {
        rows.retain(|(key, _)| key.application_key.as_slice() > after);
    }
    let has_more = rows.len() > page_size;
    rows.truncate(page_size);
    let next_tuple_key = has_more
        .then(|| rows.last().map(|(key, _)| key.application_key.clone()))
        .flatten();
    let mut tenants = Vec::new();
    for (_, row) in rows {
        match decode_control_current_row(&row.value)? {
            ControlCurrentRecord::Tenant {
                id,
                name,
                active: true,
            } => tenants.push(Tenant { id, name }),
            ControlCurrentRecord::Tenant { active: false, .. } => {}
            _ => bail!("control tenant collection contains a different record type"),
        }
    }
    if current_control_collection_revision_mvcc(mvcc)? != expected_revision {
        bail!("control tenant collection revision changed");
    }
    Ok(CurrentTenantPage {
        tenants,
        next_tuple_key,
    })
}

pub(crate) fn read_tenant_by_name_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    name: &str,
) -> Result<Option<Tenant>> {
    match read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &tenant_name_tuple_key(name)?,
    )? {
        Some(ControlCurrentRecord::Tenant {
            id,
            name: stored_name,
            active,
        }) if stored_name == name => Ok(active.then_some(Tenant {
            id,
            name: stored_name,
        })),
        Some(ControlCurrentRecord::Tenant { .. }) => {
            bail!("control tenant-name row does not match its key")
        }
        Some(_) => bail!("control tenant-name row contains a different record type"),
        None => Ok(None),
    }
}

fn read_control_current_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tuple_key: &[u8],
) -> Result<Option<ControlCurrentRecord>> {
    let key =
        crate::mvcc_product::coremeta_logical_key(CF_MESH, TABLE_CONTROL_CURRENT_ROW, tuple_key)?;
    mvcc.read_latest_value(&key)?
        .as_deref()
        .map(decode_control_current_row)
        .transpose()
}

fn read_control_current_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tuple_key: &[u8],
) -> Result<Option<ControlCurrentRecord>> {
    let key =
        crate::mvcc_product::coremeta_logical_key(CF_MESH, TABLE_CONTROL_CURRENT_ROW, tuple_key)?;
    mvcc.read_transaction_value(transaction_id, principal, &key)?
        .as_deref()
        .map(decode_control_current_row)
        .transpose()
}

pub(crate) fn read_app_by_tenant_name_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tenant_id: i64,
    name: &str,
) -> Result<Option<TransactionAppDetails>> {
    match read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &app_tenant_name_tuple_key(tenant_id, name)?,
    )? {
        Some(ControlCurrentRecord::App {
            id,
            tenant_id: stored_tenant_id,
            name: stored_name,
            client_id,
            client_secret_encrypted,
            active: true,
        }) if stored_tenant_id == tenant_id && stored_name == name => {
            Ok(Some(TransactionAppDetails {
                app: App {
                    id,
                    name: stored_name,
                    client_id,
                },
                tenant_id: stored_tenant_id,
                client_secret_encrypted,
            }))
        }
        Some(ControlCurrentRecord::App { active: false, .. }) | None => Ok(None),
        Some(ControlCurrentRecord::App { .. }) => {
            bail!("control tenant-app row does not match its key")
        }
        Some(_) => bail!("control tenant-app row contains a different record type"),
    }
}

#[derive(Debug)]
pub(crate) struct ControlAppMutationPlan {
    pub app: App,
    control_revision: u64,
    mutations: Vec<crate::mvcc_product::ProductMutation>,
    predicates: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    outbox_events: Vec<crate::mvcc_outbox::StreamOutboxEvent>,
}

impl ControlAppMutationPlan {
    pub(crate) fn with_admin_audit(
        mut self,
        event: &crate::admin_audit::AdminAuditEvent,
        transaction_id: &str,
    ) -> Result<Self> {
        let audit = crate::admin_audit::admin_audit_mvcc_plan(
            event,
            self.control_revision,
            transaction_id,
        )?;
        self.mutations.extend(audit.mutations);
        self.outbox_events.extend(audit.outbox_events);
        Ok(self)
    }

    pub(crate) async fn stage(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<App> {
        mvcc.stage_product_mutations(
            transaction_id,
            principal,
            self.mutations,
            now_unix_ms,
        )?;
        for (key, predicate) in self.predicates {
            mvcc.stage_predicate(transaction_id, principal, key, predicate, now_unix_ms)?;
        }
        for event in self.outbox_events {
            mvcc.open_transactions
                .add_stream_event(transaction_id, event, now_unix_ms)?;
        }
        let assignment = mvcc
            .reconcile_work_assignment("control-plane", mvcc.cluster_id())
            .await?
            .ok_or_else(|| anyhow!("this node does not own the cluster control-plane assignment"))?;
        mvcc.stage_assignment_guard(transaction_id, principal, &assignment, now_unix_ms)?;
        Ok(self.app)
    }
}

fn deterministic_control_mutation_id(transaction_id: &str, operation: &str) -> uuid::Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.control.transaction-mutation.v1");
    hasher.update(transaction_id.as_bytes());
    hasher.update(&[0]);
    hasher.update(operation.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    uuid::Uuid::from_bytes(bytes)
}

fn plan_control_app_mutation(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    event: ControlEventBody,
    mut current_updates: Vec<ControlCurrentRecord>,
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
    app: App,
    operation: &str,
) -> Result<ControlAppMutationPlan> {
    use crate::mvcc_transaction::PredicateKind;

    let revision_tuple_key = control_revision_tuple_key()?;
    let revision_key = crate::mvcc_product::coremeta_logical_key(
        CF_MESH,
        TABLE_CONTROL_CURRENT_ROW,
        &revision_tuple_key,
    )?;
    let revision = match read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &revision_tuple_key,
    )? {
        Some(ControlCurrentRecord::Revision { revision }) => revision,
        Some(_) => bail!("control revision key contains a different record type"),
        None => 0,
    };
    let next_revision = revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("control journal revision overflow"))?;
    current_updates.push(ControlCurrentRecord::Revision {
        revision: next_revision,
    });

    let mutation_id = deterministic_control_mutation_id(transaction_id, operation);
    let mutation_id_string = mutation_id.to_string();
    let mut operations = Vec::new();
    for record in current_updates {
        operations.extend(control_current_updates(record)?);
    }
    operations.push(CoreMutationOperation::StreamAppend {
        partition_id: hex::encode(control_partition_id()),
        stream_id: control_plane_stream_id(),
        record_kind: "control_plane".to_string(),
        payload: encode_control_event_body(&event, 0, mutation_id)?,
        idempotency_key: Some(format!("control-plane:{mutation_id_string}")),
    });

    let mut predicate_keys = BTreeSet::new();
    let mut predicates = Vec::new();
    for operation in &operations {
        let (cf, table_id, tuple_key) = match operation {
            CoreMutationOperation::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                ..
            }
            | CoreMutationOperation::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } => (cf, *table_id, tuple_key),
            CoreMutationOperation::StreamAppend { .. } => continue,
        };
        let key = crate::mvcc_product::coremeta_logical_key(cf, table_id, tuple_key)?;
        if !predicate_keys.insert(key.clone()) {
            continue;
        }
        let kind = mvcc
            .read_latest_value(&key)?
            .map(|payload| PredicateKind::ValueHash(*blake3::hash(&payload).as_bytes()))
            .unwrap_or(PredicateKind::Absent);
        predicates.push((key, kind));
    }
    let mut product =
        crate::mvcc_product::product_mutations_and_outbox_from_operations(operations)?;
    if let Some(audit_event) = audit_event {
        let audit = crate::tenant_audit::tenant_audit_mvcc_plan(
            audit_event,
            next_revision,
            &mutation_id_string,
        )?;
        product.mutations.extend(audit.mutations);
        product.predicates.extend(audit.predicates);
        product.outbox_events.extend(audit.outbox_events);
    }
    predicates.extend(product.predicates);
    Ok(ControlAppMutationPlan {
        app,
        control_revision: next_revision,
        mutations: product.mutations,
        predicates,
        outbox_events: product.outbox_events,
    })
}

pub(crate) fn plan_create_app_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tenant_id: i64,
    name: &str,
    client_id: &str,
    encrypted_secret: &[u8],
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
) -> Result<ControlAppMutationPlan> {
    if read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &app_tenant_name_tuple_key(tenant_id, name)?,
    )?
    .is_some()
    {
        bail!("app already exists");
    }
    if read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &app_client_id_tuple_key(client_id)?,
    )?
    .is_some()
    {
        bail!("client_id already exists");
    }
    let max_allocated_id = match read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &id_allocator_tuple_key()?,
    )? {
        Some(ControlCurrentRecord::IdAllocator { max_allocated_id }) => max_allocated_id,
        Some(_) => bail!("control ID allocator row contains a different record type"),
        None => 0,
    };
    let id = max_allocated_id
        .checked_add(1)
        .ok_or_else(|| anyhow!("control ID allocator overflow"))?;
    let app = App {
        id,
        name: name.to_string(),
        client_id: client_id.to_string(),
    };
    let record = ControlCurrentRecord::App {
        id,
        tenant_id,
        name: name.to_string(),
        client_id: client_id.to_string(),
        client_secret_encrypted: encrypted_secret.to_vec(),
        active: true,
    };
    plan_control_app_mutation(
        mvcc,
        transaction_id,
        principal,
        ControlEventBody::AppCreate {
            id,
            tenant_id,
            name: name.to_string(),
            client_id: client_id.to_string(),
            client_secret_encrypted: encrypted_secret.to_vec(),
        },
        vec![
            ControlCurrentRecord::IdAllocator {
                max_allocated_id: id,
            },
            record,
        ],
        audit_event,
        app,
        "app-create",
    )
}

pub(crate) fn plan_update_app_secret_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    app_id: i64,
    encrypted_secret: &[u8],
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
) -> Result<ControlAppMutationPlan> {
    let existing = match read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &app_id_tuple_key(app_id)?,
    )? {
        Some(ControlCurrentRecord::App {
            id,
            tenant_id,
            name,
            client_id,
            client_secret_encrypted,
            active: true,
        }) => StoredControlApp {
            id,
            tenant_id,
            name,
            client_id,
            client_secret_encrypted,
        },
        _ => bail!("app not found"),
    };
    let app = App {
        id: existing.id,
        name: existing.name.clone(),
        client_id: existing.client_id.clone(),
    };
    plan_control_app_mutation(
        mvcc,
        transaction_id,
        principal,
        ControlEventBody::AppSecretUpdate {
            app_id,
            client_secret_encrypted: encrypted_secret.to_vec(),
        },
        vec![ControlCurrentRecord::App {
            id: existing.id,
            tenant_id: existing.tenant_id,
            name: existing.name,
            client_id: existing.client_id,
            client_secret_encrypted: encrypted_secret.to_vec(),
            active: true,
        }],
        audit_event,
        app,
        "app-rotate-secret",
    )
}

pub(crate) fn plan_delete_app_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    app_id: i64,
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
) -> Result<ControlAppMutationPlan> {
    let existing = match read_control_current_in_transaction(
        mvcc,
        transaction_id,
        principal,
        &app_id_tuple_key(app_id)?,
    )? {
        Some(ControlCurrentRecord::App {
            id,
            tenant_id,
            name,
            client_id,
            client_secret_encrypted,
            active: true,
        }) => StoredControlApp {
            id,
            tenant_id,
            name,
            client_id,
            client_secret_encrypted,
        },
        _ => bail!("app not found"),
    };
    let app = App {
        id: existing.id,
        name: existing.name.clone(),
        client_id: existing.client_id.clone(),
    };
    plan_control_app_mutation(
        mvcc,
        transaction_id,
        principal,
        ControlEventBody::AppDelete { app_id },
        vec![ControlCurrentRecord::App {
            id: existing.id,
            tenant_id: existing.tenant_id,
            name: existing.name,
            client_id: existing.client_id,
            client_secret_encrypted: existing.client_secret_encrypted,
            active: false,
        }],
        audit_event,
        app,
        "app-delete",
    )
}

async fn append_control_event_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    event: ControlEventBody,
    mut current_updates: Vec<ControlCurrentRecord>,
    fence_token: u64,
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
    admin_audit_event: Option<&crate::admin_audit::AdminAuditEvent>,
) -> Result<()> {
    use crate::mvcc_transaction::{DurabilityLevel, PredicateKind};

    let revision_tuple_key = control_revision_tuple_key()?;
    let revision_key = crate::mvcc_product::coremeta_logical_key(
        CF_MESH,
        TABLE_CONTROL_CURRENT_ROW,
        &revision_tuple_key,
    )?;
    let revision_payload = mvcc.read_latest_value(&revision_key)?;
    let revision = match revision_payload.as_deref() {
        Some(payload) => match decode_control_current_row(payload)? {
            ControlCurrentRecord::Revision { revision } => revision,
            _ => bail!("control revision key contains a different record type"),
        },
        None => 0,
    };
    let next_revision = revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("control journal revision overflow"))?;
    current_updates.push(ControlCurrentRecord::Revision {
        revision: next_revision,
    });

    let mutation_id = uuid::Uuid::new_v4();
    let mutation_id_string = mutation_id.to_string();
    let mut operations = Vec::new();
    for record in current_updates {
        operations.extend(control_current_updates(record)?);
    }
    operations.push(CoreMutationOperation::StreamAppend {
        partition_id: hex::encode(control_partition_id()),
        stream_id: control_plane_stream_id(),
        record_kind: "control_plane".to_string(),
        payload: encode_control_event_body(&event, fence_token, mutation_id)?,
        idempotency_key: Some(format!("control-plane:{mutation_id_string}")),
    });

    let mut predicate_keys = BTreeSet::new();
    let mut predicates = Vec::new();
    for operation in &operations {
        let (cf, table_id, tuple_key) = match operation {
            CoreMutationOperation::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                ..
            }
            | CoreMutationOperation::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } => (cf, *table_id, tuple_key),
            CoreMutationOperation::StreamAppend { .. } => continue,
        };
        let key = crate::mvcc_product::coremeta_logical_key(cf, table_id, tuple_key)?;
        if !predicate_keys.insert(key.clone()) {
            continue;
        }
        let kind = mvcc
            .read_latest_value(&key)?
            .map(|payload| PredicateKind::ValueHash(*blake3::hash(&payload).as_bytes()))
            .unwrap_or(PredicateKind::Absent);
        predicates.push((key, kind));
    }
    let mut plan = crate::mvcc_product::product_mutations_and_outbox_from_operations(operations)?;
    if let Some(audit_event) = audit_event {
        let audit_plan = crate::tenant_audit::tenant_audit_mvcc_plan(
            audit_event,
            next_revision,
            &mutation_id_string,
        )?;
        plan.mutations.extend(audit_plan.mutations);
        plan.outbox_events.extend(audit_plan.outbox_events);
    }
    if let Some(audit_event) = admin_audit_event {
        let audit_plan = crate::admin_audit::admin_audit_mvcc_plan(
            audit_event,
            next_revision,
            &mutation_id_string,
        )?;
        plan.mutations.extend(audit_plan.mutations);
        plan.outbox_events.extend(audit_plan.outbox_events);
    }
    predicates.extend(plan.predicates);
    let principal = control_partition_principal();
    let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            &principal,
            &format!("control-plane:{mutation_id_string}"),
            std::time::Duration::from_secs(30),
            DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now,
        )
        .await?;
    mvcc.stage_product_mutations(&handle.transaction_id, &principal, plan.mutations, now)?;
    for (key, kind) in predicates {
        mvcc.stage_predicate(&handle.transaction_id, &principal, key, kind, now)?;
    }
    for event in plan.outbox_events {
        mvcc.open_transactions
            .add_stream_event(&handle.transaction_id, event, now)?;
    }
    let assignment = mvcc
        .reconcile_work_assignment("control-plane", mvcc.cluster_id())
        .await?
        .ok_or_else(|| anyhow!("this node does not own the cluster control-plane assignment"))?;
    mvcc.stage_assignment_guard(&handle.transaction_id, &principal, &assignment, now)?;
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            &principal,
            u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
        )
        .await?;
    if let crate::mvcc_transaction::CertificationResult::Aborted { reason } = outcome.certification
    {
        bail!("control-plane transaction aborted: {reason:?}");
    }
    Ok(())
}

fn control_current_updates(record: ControlCurrentRecord) -> Result<Vec<CoreMutationOperation>> {
    let payload = encode_control_current_row(&record)?;
    let mut operations = Vec::new();
    match &record {
        ControlCurrentRecord::Revision { .. } => {
            operations.push(control_current_put(control_revision_tuple_key()?, payload));
        }
        ControlCurrentRecord::IdAllocator { .. } => {
            operations.push(control_current_put(id_allocator_tuple_key()?, payload));
        }
        ControlCurrentRecord::Region { name, .. } => {
            operations.push(control_current_put(region_tuple_key(name)?, payload));
        }
        ControlCurrentRecord::Tenant { id, name, active } => {
            operations.push(control_current_put(
                tenant_id_tuple_key(*id)?,
                payload.clone(),
            ));
            if *active {
                operations.push(control_current_put(tenant_name_tuple_key(name)?, payload));
            } else {
                operations.push(control_current_delete(tenant_name_tuple_key(name)?));
            }
        }
        ControlCurrentRecord::App {
            id,
            tenant_id,
            name,
            client_id,
            active,
            ..
        } => {
            operations.push(control_current_put(app_id_tuple_key(*id)?, payload.clone()));
            if *active {
                operations.push(control_current_put(
                    app_tenant_name_tuple_key(*tenant_id, name)?,
                    payload.clone(),
                ));
                operations.push(control_current_put(
                    app_client_id_tuple_key(client_id)?,
                    payload,
                ));
            } else {
                operations.push(control_current_delete(app_tenant_name_tuple_key(
                    *tenant_id, name,
                )?));
                operations.push(control_current_delete(app_client_id_tuple_key(client_id)?));
            }
        }
    }
    Ok(operations)
}

fn control_current_put(tuple_key: Vec<u8>, payload: Vec<u8>) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaPut {
        partition_id: hex::encode(control_partition_id()),
        cf: CF_MESH.to_string(),
        table_id: TABLE_CONTROL_CURRENT_ROW,
        tuple_key,
        payload,
    }
}

fn control_current_delete(tuple_key: Vec<u8>) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaDelete {
        partition_id: hex::encode(control_partition_id()),
        cf: CF_MESH.to_string(),
        table_id: TABLE_CONTROL_CURRENT_ROW,
        tuple_key,
    }
}

pub(crate) async fn create_app_with_permit_mvcc(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    name: &str,
    client_id: &str,
    encrypted_secret: &[u8],
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
    admin_audit_event: Option<&crate::admin_audit::AdminAuditEvent>,
) -> Result<App> {
    require_control_permit(permit)?;
    if read_app_by_tenant_name_mvcc(mvcc, tenant_id, name)?.is_some() {
        bail!("app already exists");
    }
    if read_app_details_by_client_id_mvcc(mvcc, client_id)?.is_some() {
        bail!("client_id already exists");
    }
    let max_allocated_id = match read_control_current_mvcc(mvcc, &id_allocator_tuple_key()?)? {
        Some(ControlCurrentRecord::IdAllocator { max_allocated_id }) => max_allocated_id,
        Some(_) => bail!("control ID allocator row contains a different record type"),
        None => 0,
    };
    let id = max_allocated_id
        .checked_add(1)
        .ok_or_else(|| anyhow!("control ID allocator overflow"))?;
    let record = ControlCurrentRecord::App {
        id,
        tenant_id,
        name: name.to_string(),
        client_id: client_id.to_string(),
        client_secret_encrypted: encrypted_secret.to_vec(),
        active: true,
    };
    append_control_event_mvcc(
        mvcc,
        ControlEventBody::AppCreate {
            id,
            tenant_id,
            name: name.to_string(),
            client_id: client_id.to_string(),
            client_secret_encrypted: encrypted_secret.to_vec(),
        },
        vec![
            ControlCurrentRecord::IdAllocator {
                max_allocated_id: id,
            },
            record,
        ],
        permit.fence_token,
        audit_event,
        admin_audit_event,
    )
    .await?;
    Ok(App {
        id,
        name: name.to_string(),
        client_id: client_id.to_string(),
    })
}

pub(crate) async fn update_app_secret_with_permit_mvcc(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    app_id: i64,
    encrypted_secret: &[u8],
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
    admin_audit_event: Option<&crate::admin_audit::AdminAuditEvent>,
) -> Result<()> {
    require_control_permit(permit)?;
    let existing = read_stored_app_mvcc(mvcc, &app_id_tuple_key(app_id)?)?
        .ok_or_else(|| anyhow!("app not found"))?;
    append_control_event_mvcc(
        mvcc,
        ControlEventBody::AppSecretUpdate {
            app_id,
            client_secret_encrypted: encrypted_secret.to_vec(),
        },
        vec![ControlCurrentRecord::App {
            id: existing.id,
            tenant_id: existing.tenant_id,
            name: existing.name,
            client_id: existing.client_id,
            client_secret_encrypted: encrypted_secret.to_vec(),
            active: true,
        }],
        permit.fence_token,
        audit_event,
        admin_audit_event,
    )
    .await
}

pub(crate) async fn delete_app_with_permit_mvcc(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    app_id: i64,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
    admin_audit_event: Option<&crate::admin_audit::AdminAuditEvent>,
) -> Result<()> {
    require_control_permit(permit)?;
    let existing = read_stored_app_mvcc(mvcc, &app_id_tuple_key(app_id)?)?
        .ok_or_else(|| anyhow!("app not found"))?;
    append_control_event_mvcc(
        mvcc,
        ControlEventBody::AppDelete { app_id },
        vec![ControlCurrentRecord::App {
            id: existing.id,
            tenant_id: existing.tenant_id,
            name: existing.name,
            client_id: existing.client_id,
            client_secret_encrypted: existing.client_secret_encrypted,
            active: false,
        }],
        permit.fence_token,
        audit_event,
        admin_audit_event,
    )
    .await
}

fn encode_control_event_body(
    event: &ControlEventBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let proto = ControlEventProto {
        schema: CONTROL_EVENT_SCHEMA.to_string(),
        emitted_at: chrono::Utc::now().to_rfc3339(),
        fence_token,
        mutation_id: mutation_id.to_string(),
        event: Some(match event {
            ControlEventBody::RegionUpsert { name } => {
                control_event_proto::Event::RegionUpsert(RegionUpsertProto { name: name.clone() })
            }
            ControlEventBody::TenantUpsert { id, name } => {
                control_event_proto::Event::TenantUpsert(TenantUpsertProto {
                    id: *id,
                    name: name.clone(),
                })
            }
            ControlEventBody::AppCreate {
                id,
                tenant_id,
                name,
                client_id,
                client_secret_encrypted,
            } => control_event_proto::Event::AppCreate(AppCreateProto {
                id: *id,
                tenant_id: *tenant_id,
                name: name.clone(),
                client_id: client_id.clone(),
                client_secret_encrypted: client_secret_encrypted.clone(),
            }),
            ControlEventBody::AppSecretUpdate {
                app_id,
                client_secret_encrypted,
            } => control_event_proto::Event::AppSecretUpdate(AppSecretUpdateProto {
                app_id: *app_id,
                client_secret_encrypted: client_secret_encrypted.clone(),
            }),
            ControlEventBody::AppDelete { app_id } => {
                control_event_proto::Event::AppDelete(AppDeleteProto { app_id: *app_id })
            }
        }),
    };
    let mut bytes = Vec::new();
    proto.encode(&mut bytes)?;
    ensure_deterministic_control_proto(&proto, &bytes, "control event")?;
    if bytes.len() > CONTROL_CURRENT_TARGET_MAX_BYTES {
        bail!(
            "control event protobuf is {} bytes, exceeding {} bytes",
            bytes.len(),
            CONTROL_CURRENT_TARGET_MAX_BYTES
        );
    }
    Ok(bytes)
}

fn ensure_deterministic_control_proto(
    message: &impl Message,
    bytes: &[u8],
    label: &str,
) -> Result<()> {
    let mut canonical = Vec::with_capacity(message.encoded_len());
    message.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("{label} protobuf is not deterministic");
    }
    Ok(())
}

fn encode_control_current_row(record: &ControlCurrentRecord) -> Result<Vec<u8>> {
    let proto = ControlCurrentProto {
        schema: CONTROL_CURRENT_SCHEMA.to_string(),
        record: Some(match record {
            ControlCurrentRecord::Revision { revision } => {
                control_current_proto::Record::Revision(RevisionCurrentProto {
                    revision: *revision,
                })
            }
            ControlCurrentRecord::IdAllocator { max_allocated_id } => {
                control_current_proto::Record::IdAllocator(IdAllocatorCurrentProto {
                    max_allocated_id: *max_allocated_id,
                })
            }
            ControlCurrentRecord::Region { name, active } => {
                control_current_proto::Record::Region(RegionCurrentProto {
                    name: name.clone(),
                    active: *active,
                })
            }
            ControlCurrentRecord::Tenant { id, name, active } => {
                control_current_proto::Record::Tenant(TenantCurrentProto {
                    id: *id,
                    name: name.clone(),
                    active: *active,
                })
            }
            ControlCurrentRecord::App {
                id,
                tenant_id,
                name,
                client_id,
                client_secret_encrypted,
                active,
            } => control_current_proto::Record::App(AppCurrentProto {
                id: *id,
                tenant_id: *tenant_id,
                name: name.clone(),
                client_id: client_id.clone(),
                client_secret_encrypted: client_secret_encrypted.clone(),
                active: *active,
            }),
        }),
    };
    let mut bytes = Vec::new();
    proto.encode(&mut bytes)?;
    ensure_deterministic_control_proto(&proto, &bytes, "control current")?;
    if bytes.len() > CONTROL_CURRENT_TARGET_MAX_BYTES {
        bail!(
            "control current protobuf is {} bytes, exceeding {} bytes",
            bytes.len(),
            CONTROL_CURRENT_TARGET_MAX_BYTES
        );
    }
    Ok(bytes)
}

fn decode_control_current_row(bytes: &[u8]) -> Result<ControlCurrentRecord> {
    if bytes.len() > CONTROL_CURRENT_TARGET_MAX_BYTES {
        bail!(
            "control current protobuf is {} bytes, exceeding {} bytes",
            bytes.len(),
            CONTROL_CURRENT_TARGET_MAX_BYTES
        );
    }
    let proto = ControlCurrentProto::decode(bytes)?;
    ensure_deterministic_control_proto(&proto, bytes, "control current")?;
    if proto.schema != CONTROL_CURRENT_SCHEMA {
        bail!("control current protobuf has invalid schema");
    }
    match proto
        .record
        .ok_or_else(|| anyhow!("control current protobuf is missing record"))?
    {
        control_current_proto::Record::Revision(value) => Ok(ControlCurrentRecord::Revision {
            revision: value.revision,
        }),
        control_current_proto::Record::IdAllocator(value) => {
            Ok(ControlCurrentRecord::IdAllocator {
                max_allocated_id: value.max_allocated_id,
            })
        }
        control_current_proto::Record::Region(value) => Ok(ControlCurrentRecord::Region {
            name: value.name,
            active: value.active,
        }),
        control_current_proto::Record::Tenant(value) => Ok(ControlCurrentRecord::Tenant {
            id: value.id,
            name: value.name,
            active: value.active,
        }),
        control_current_proto::Record::App(value) => Ok(ControlCurrentRecord::App {
            id: value.id,
            tenant_id: value.tenant_id,
            name: value.name,
            client_id: value.client_id,
            client_secret_encrypted: value.client_secret_encrypted,
            active: value.active,
        }),
    }
}

fn control_revision_tuple_key() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("revision")])
}

fn id_allocator_tuple_key() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("id-allocator")])
}

fn region_tuple_key(name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("region"),
        CoreMetaTuplePart::Utf8(name),
    ])
}

fn region_tuple_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("region")])
}

fn tenant_id_tuple_key(id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("tenant-id"),
        CoreMetaTuplePart::I64(id),
    ])
}

fn tenant_id_tuple_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("tenant-id")])
}

fn tenant_name_tuple_key(name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("tenant-name"),
        CoreMetaTuplePart::Utf8(name),
    ])
}

fn app_id_tuple_key(id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("app-id"),
        CoreMetaTuplePart::I64(id),
    ])
}

fn app_id_tuple_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("app-id")])
}

fn app_tenant_name_tuple_key(tenant_id: i64, name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("app-tenant"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(name),
    ])
}

fn app_tenant_name_tuple_prefix(tenant_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("app-tenant"),
        CoreMetaTuplePart::I64(tenant_id),
    ])
}

fn app_client_id_tuple_key(client_id: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("app-client"),
        CoreMetaTuplePart::Utf8(client_id),
    ])
}

pub fn control_partition_id() -> Hash32 {
    hash32(b"control_plane/global")
}

fn control_plane_stream_id() -> String {
    "control_plane:global".to_string()
}

fn control_partition_principal() -> String {
    "partition-owner:control_plane:global".to_string()
}

fn require_control_permit(permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "control_plane"
        || permit.partition_id != hex::encode(control_partition_id())
    {
        anyhow::bail!("control-plane write permit targets a different partition");
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}
