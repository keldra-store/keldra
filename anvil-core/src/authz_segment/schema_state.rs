use crate::{authz_realm_schema, storage::Storage};
use anyhow::{Result, anyhow, bail};
use std::collections::BTreeMap;

const AUTHZ_SCHEMA_SOURCE_PAGE_SIZE: usize = 256;
const MAX_AUTHZ_SCHEMA_SOURCE_ROWS: usize = 16_384;

pub(super) async fn collect_latest_schema_revisions(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<authz_realm_schema::StoredAuthzSchemaRevision>> {
    let mut latest = BTreeMap::new();
    let mut after_tuple_key = None;
    let mut visited = 0_usize;
    loop {
        let page = authz_realm_schema::page_schema_revisions(
            storage,
            tenant_id,
            after_tuple_key.as_deref(),
            AUTHZ_SCHEMA_SOURCE_PAGE_SIZE,
        )
        .await?;
        visited = visited
            .checked_add(page.records.len())
            .ok_or_else(|| anyhow!("authorization schema source row count overflow"))?;
        if visited > MAX_AUTHZ_SCHEMA_SOURCE_ROWS {
            bail!(
                "AuthzMaterializationTooBroad: schema source exceeds {MAX_AUTHZ_SCHEMA_SOURCE_ROWS} rows"
            );
        }
        for revision in page.records {
            latest
                .entry(revision.schema_ref.schema_id.clone())
                .and_modify(
                    |current: &mut authz_realm_schema::StoredAuthzSchemaRevision| {
                        if revision.schema_ref.schema_revision > current.schema_ref.schema_revision
                        {
                            *current = revision.clone();
                        }
                    },
                )
                .or_insert(revision);
        }
        let Some(next_tuple_key) = page.next_tuple_key else {
            break;
        };
        if after_tuple_key.as_ref() == Some(&next_tuple_key) {
            bail!("authorization schema source page did not advance its continuation");
        }
        after_tuple_key = Some(next_tuple_key);
    }
    Ok(latest.into_values().collect())
}

pub(super) async fn collect_schema_bindings(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<authz_realm_schema::StoredAuthzSchemaBinding>> {
    let mut bindings = Vec::new();
    let mut after_tuple_key = None;
    loop {
        let page = authz_realm_schema::page_schema_bindings(
            storage,
            tenant_id,
            after_tuple_key.as_deref(),
            AUTHZ_SCHEMA_SOURCE_PAGE_SIZE,
        )
        .await?;
        if bindings.len().saturating_add(page.records.len()) > MAX_AUTHZ_SCHEMA_SOURCE_ROWS {
            bail!(
                "AuthzMaterializationTooBroad: schema binding source exceeds {MAX_AUTHZ_SCHEMA_SOURCE_ROWS} rows"
            );
        }
        bindings.extend(page.records);
        let Some(next_tuple_key) = page.next_tuple_key else {
            break;
        };
        if after_tuple_key.as_ref() == Some(&next_tuple_key) {
            bail!("authorization schema binding page did not advance its continuation");
        }
        after_tuple_key = Some(next_tuple_key);
    }
    Ok(bindings)
}
