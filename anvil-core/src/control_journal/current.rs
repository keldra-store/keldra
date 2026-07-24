use super::*;

const CONTROL_PAGE_MAX_ROWS: usize = 1_000;

#[derive(Debug, Clone)]
pub struct CurrentAppPage {
    pub apps: Vec<App>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct CurrentRegionPage {
    pub regions: Vec<String>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct CurrentTenantPage {
    pub tenants: Vec<Tenant>,
    pub next_tuple_key: Option<Vec<u8>>,
}

pub async fn read_control_state(storage: &Storage) -> Result<ControlState> {
    let revision = current_control_collection_revision(storage).await?;
    let mut state = ControlState {
        next_id: read_id_allocator(storage).await?,
        ..ControlState::default()
    };

    let mut cursor = None;
    loop {
        let page =
            page_regions(storage, &revision, cursor.as_deref(), CONTROL_PAGE_MAX_ROWS).await?;
        state.regions.extend(page.regions);
        let Some(next) = page.next_tuple_key else {
            break;
        };
        cursor = Some(next);
    }

    cursor = None;
    loop {
        let page =
            page_tenants(storage, &revision, cursor.as_deref(), CONTROL_PAGE_MAX_ROWS).await?;
        for tenant in page.tenants {
            state.next_id = state.next_id.max(tenant.id);
            state.tenants.insert(tenant.id, tenant);
        }
        let Some(next) = page.next_tuple_key else {
            break;
        };
        cursor = Some(next);
    }

    let core_store = CoreStore::new(storage.clone()).await?;
    cursor = None;
    let prefix = app_id_tuple_prefix()?;
    loop {
        ensure_collection_revision(storage, &revision, "control state").await?;
        let (rows, next) = scan_current_page(
            &core_store,
            &prefix,
            cursor.as_deref(),
            CONTROL_PAGE_MAX_ROWS,
        )?;
        for row in rows {
            match decode_control_current_row(&row.payload)? {
                ControlCurrentRecord::App {
                    id,
                    tenant_id,
                    name,
                    client_id,
                    client_secret_encrypted,
                    active: true,
                } => {
                    state.next_id = state.next_id.max(id);
                    state.apps.insert(
                        id,
                        StoredControlApp {
                            id,
                            tenant_id,
                            name,
                            client_id,
                            client_secret_encrypted,
                        },
                    );
                }
                ControlCurrentRecord::App { active: false, .. } => {}
                _ => bail!("control app collection contains a different record type"),
            }
        }
        let Some(next) = next else {
            break;
        };
        cursor = Some(next);
    }
    ensure_collection_revision(storage, &revision, "control state").await?;
    Ok(state)
}

pub async fn read_tenant_by_name(storage: &Storage, name: &str) -> Result<Option<Tenant>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = core_store.read_coremeta_row(
        CF_MESH,
        TABLE_CONTROL_CURRENT_ROW,
        &tenant_name_tuple_key(name)?,
    )?
    else {
        return Ok(None);
    };
    match decode_control_current_row(&payload)? {
        ControlCurrentRecord::Tenant {
            id,
            name: stored_name,
            active,
        } if stored_name == name => Ok(active.then_some(Tenant {
            id,
            name: stored_name,
        })),
        ControlCurrentRecord::Tenant { .. } => {
            bail!("control tenant-name row does not match its key")
        }
        _ => bail!("control tenant-name row contains a different record type"),
    }
}

pub async fn read_app_by_id(storage: &Storage, app_id: i64) -> Result<Option<App>> {
    let Some(app) = read_stored_app(storage, &app_id_tuple_key(app_id)?).await? else {
        return Ok(None);
    };
    if app.id != app_id {
        bail!("control app-id row does not match its key");
    }
    Ok(Some(app_record(&app)))
}

pub async fn read_app_by_tenant_name(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
) -> Result<Option<App>> {
    let Some(app) = read_stored_app(storage, &app_tenant_name_tuple_key(tenant_id, name)?).await?
    else {
        return Ok(None);
    };
    if app.tenant_id != tenant_id || app.name != name {
        bail!("control tenant-app row does not match its key");
    }
    Ok(Some(app_record(&app)))
}

pub async fn read_app_details_by_client_id(
    storage: &Storage,
    client_id: &str,
) -> Result<Option<AppDetails>> {
    let Some(app) = read_stored_app(storage, &app_client_id_tuple_key(client_id)?).await? else {
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

pub async fn current_control_collection_revision(storage: &Storage) -> Result<String> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = core_store.read_coremeta_row(
        CF_MESH,
        TABLE_CONTROL_CURRENT_ROW,
        &control_revision_tuple_key()?,
    )?
    else {
        return Ok("0".to_string());
    };
    match decode_control_current_row(&payload)? {
        ControlCurrentRecord::Revision { revision } => Ok(revision.to_string()),
        _ => bail!("control revision key contains a different record type"),
    }
}

pub async fn page_apps_for_tenant(
    storage: &Storage,
    tenant_id: i64,
    expected_revision: &str,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentAppPage> {
    ensure_page_size(page_size, "application")?;
    ensure_collection_revision(storage, expected_revision, "application").await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let (rows, next_tuple_key) = scan_current_page(
        &core_store,
        &app_tenant_name_tuple_prefix(tenant_id)?,
        after_tuple_key,
        page_size,
    )?;
    let mut apps = Vec::with_capacity(rows.len());
    for row in rows {
        match decode_control_current_row(&row.payload)? {
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
    ensure_collection_revision(storage, expected_revision, "application").await?;
    Ok(CurrentAppPage {
        apps,
        next_tuple_key,
    })
}

pub async fn page_regions(
    storage: &Storage,
    expected_revision: &str,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentRegionPage> {
    ensure_page_size(page_size, "region")?;
    ensure_collection_revision(storage, expected_revision, "region").await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let (rows, next_tuple_key) = scan_current_page(
        &core_store,
        &region_tuple_prefix()?,
        after_tuple_key,
        page_size,
    )?;
    let mut regions = Vec::with_capacity(rows.len());
    for row in rows {
        match decode_control_current_row(&row.payload)? {
            ControlCurrentRecord::Region { name, active: true } => regions.push(name),
            ControlCurrentRecord::Region { active: false, .. } => {}
            _ => bail!("control region collection contains a different record type"),
        }
    }
    ensure_collection_revision(storage, expected_revision, "region").await?;
    Ok(CurrentRegionPage {
        regions,
        next_tuple_key,
    })
}

pub async fn page_tenants(
    storage: &Storage,
    expected_revision: &str,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentTenantPage> {
    ensure_page_size(page_size, "tenant")?;
    ensure_collection_revision(storage, expected_revision, "tenant").await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let (rows, next_tuple_key) = scan_current_page(
        &core_store,
        &tenant_id_tuple_prefix()?,
        after_tuple_key,
        page_size,
    )?;
    let mut tenants = Vec::with_capacity(rows.len());
    for row in rows {
        match decode_control_current_row(&row.payload)? {
            ControlCurrentRecord::Tenant {
                id,
                name,
                active: true,
            } => tenants.push(Tenant { id, name }),
            ControlCurrentRecord::Tenant { active: false, .. } => {}
            _ => bail!("control tenant collection contains a different record type"),
        }
    }
    ensure_collection_revision(storage, expected_revision, "tenant").await?;
    Ok(CurrentTenantPage {
        tenants,
        next_tuple_key,
    })
}

pub(super) async fn read_id_allocator(storage: &Storage) -> Result<i64> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = core_store.read_coremeta_row(
        CF_MESH,
        TABLE_CONTROL_CURRENT_ROW,
        &id_allocator_tuple_key()?,
    )?
    else {
        return Ok(0);
    };
    match decode_control_current_row(&payload)? {
        ControlCurrentRecord::IdAllocator { max_allocated_id } => Ok(max_allocated_id),
        _ => bail!("control id allocator row contains a different record type"),
    }
}

pub(super) async fn read_region_active(storage: &Storage, name: &str) -> Result<bool> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = core_store.read_coremeta_row(
        CF_MESH,
        TABLE_CONTROL_CURRENT_ROW,
        &region_tuple_key(name)?,
    )?
    else {
        return Ok(false);
    };
    match decode_control_current_row(&payload)? {
        ControlCurrentRecord::Region {
            name: stored_name,
            active,
        } if stored_name == name => Ok(active),
        ControlCurrentRecord::Region { .. } => {
            bail!("control region row does not match its key")
        }
        _ => bail!("control region row contains a different record type"),
    }
}

pub(super) async fn read_stored_app(
    storage: &Storage,
    tuple_key: &[u8],
) -> Result<Option<StoredControlApp>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(payload) =
        core_store.read_coremeta_row(CF_MESH, TABLE_CONTROL_CURRENT_ROW, tuple_key)?
    else {
        return Ok(None);
    };
    match decode_control_current_row(&payload)? {
        ControlCurrentRecord::App {
            id,
            tenant_id,
            name,
            client_id,
            client_secret_encrypted,
            active: true,
        } => Ok(Some(StoredControlApp {
            id,
            tenant_id,
            name,
            client_id,
            client_secret_encrypted,
        })),
        ControlCurrentRecord::App { active: false, .. } => Ok(None),
        _ => bail!("control application row contains a different record type"),
    }
}

fn scan_current_page(
    core_store: &CoreStore,
    prefix: &[u8],
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<(Vec<crate::core_store::CoreMetaRecord>, Option<Vec<u8>>)> {
    let mut rows = core_store.scan_coremeta_prefix_page(
        CF_MESH,
        TABLE_CONTROL_CURRENT_ROW,
        prefix,
        after_tuple_key,
        page_size + 1,
    )?;
    let has_more = rows.len() > page_size;
    if has_more {
        rows.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            core_meta_record_tuple_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("control page continuation has no last row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    Ok((rows, next_tuple_key))
}

fn ensure_page_size(page_size: usize, collection: &str) -> Result<()> {
    if !(1..=CONTROL_PAGE_MAX_ROWS).contains(&page_size) {
        bail!("{collection} page size must be between 1 and {CONTROL_PAGE_MAX_ROWS}");
    }
    Ok(())
}

async fn ensure_collection_revision(
    storage: &Storage,
    expected_revision: &str,
    collection: &str,
) -> Result<()> {
    if current_control_collection_revision(storage).await? != expected_revision {
        bail!("{collection} collection revision changed");
    }
    Ok(())
}
