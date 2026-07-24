use crate::{
    anvil_personaldb_sqlite_changeset::DecodedSqliteChangesetChange,
    formats::hash32,
    personaldb_coremeta::{
        read_personaldb_data_locator_bytes, read_personaldb_data_locator_row,
        write_personaldb_bytes_as_data_locator,
    },
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use rusqlite::Connection;
use std::collections::BTreeSet;

const PERSONALDB_SCHEMA_REF_PREFIX: &str = "personaldb_schema_sql:";

pub async fn write_personaldb_schema_sql(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    schema_sql: &str,
    schema_hash: &str,
) -> Result<()> {
    validate_schema_sql(schema_sql, schema_hash)?;
    let ref_name = personaldb_schema_ref_name(tenant_id, database_id)?;
    write_personaldb_bytes_as_data_locator(
        storage,
        tenant_id,
        database_id,
        &ref_name,
        "schema_sql",
        1,
        schema_sql.as_bytes().to_vec(),
        schema_hash.to_string(),
        vec![format!("schema_hash:{schema_hash}")],
        format!("personaldb-schema:{tenant_id}:{database_id}:{schema_hash}"),
    )
    .await?;
    Ok(())
}

pub async fn read_personaldb_schema_sql(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    schema_hash: &str,
) -> Result<Option<String>> {
    let ref_name = personaldb_schema_ref_name(tenant_id, database_id)?;
    let Some(row) =
        read_personaldb_data_locator_row(storage, tenant_id, database_id, &ref_name).await?
    else {
        return Ok(None);
    };
    if row.data_kind != "schema_sql" {
        return Err(anyhow!("PersonalDB schema locator has wrong data kind"));
    }
    let bytes = read_personaldb_data_locator_bytes(storage, &row).await?;
    if hex::encode(hash32(&bytes)) != schema_hash {
        return Err(anyhow!("PersonalDB schema hash mismatch"));
    }
    let schema_sql = String::from_utf8(bytes).context("PersonalDB schema must be UTF-8")?;
    validate_schema_sql(&schema_sql, schema_hash)?;
    Ok(Some(schema_sql))
}

fn personaldb_schema_ref_name(tenant_id: i64, database_id: &str) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!("PersonalDB tenant id must be nonnegative"));
    }
    validate_safe_component(database_id, "database_id")?;
    Ok(format!(
        "{PERSONALDB_SCHEMA_REF_PREFIX}tenant:{tenant_id}:database:{database_id}"
    ))
}

fn validate_safe_component(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!("{field} is not a safe component"));
    }
    Ok(())
}

pub fn validate_schema_sql(schema_sql: &str, schema_hash: &str) -> Result<()> {
    if schema_sql.trim().is_empty() {
        return Err(anyhow!("PersonalDB schema SQL must not be empty"));
    }
    if hex::encode(hash32(schema_sql.as_bytes())) != schema_hash {
        return Err(anyhow!("PersonalDB schema hash mismatch"));
    }
    let tables = registered_schema_tables(schema_sql)?;
    if tables.is_empty() {
        return Err(anyhow!("PersonalDB schema must define at least one table"));
    }
    Ok(())
}

pub fn validate_changeset_tables_registered(
    changes: &[DecodedSqliteChangesetChange],
    schema_sql: &str,
) -> Result<()> {
    let tables = registered_schema_tables(schema_sql)?;
    for change in changes {
        if !tables.contains(&change.table_name) {
            return Err(anyhow!(
                "SQLite changeset targets table absent from registered schema: {}",
                change.table_name
            ));
        }
    }
    Ok(())
}

pub fn registered_schema_tables(schema_sql: &str) -> Result<BTreeSet<String>> {
    let connection = Connection::open_in_memory()?;
    connection
        .execute_batch(schema_sql)
        .context("execute PersonalDB schema SQL")?;
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
         ORDER BY name",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut tables = BTreeSet::new();
    for row in rows {
        let name = row?;
        if name.contains('/') || name.contains("..") || name.is_empty() {
            return Err(anyhow!("PersonalDB schema table name is unsafe"));
        }
        tables.insert(name);
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anvil_personaldb_sqlite_changeset::iterate_changeset;
    use rusqlite::{Connection, session::Session};

    const SCHEMA_SQL: &str =
        "CREATE TABLE items(id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL);";

    #[test]
    fn schema_tables_are_extracted_and_hash_checked() {
        let schema_hash = hex::encode(hash32(SCHEMA_SQL.as_bytes()));
        validate_schema_sql(SCHEMA_SQL, &schema_hash).unwrap();
        let tables = registered_schema_tables(SCHEMA_SQL).unwrap();
        assert!(tables.contains("items"));
        assert!(validate_schema_sql(SCHEMA_SQL, &hex::encode([9; 32])).is_err());
    }

    #[test]
    fn changeset_tables_must_exist_in_registered_schema() {
        let changeset = insert_changeset("items");
        let changes = iterate_changeset(&changeset).unwrap();
        validate_changeset_tables_registered(&changes, SCHEMA_SQL).unwrap();

        let other = insert_changeset("other_items");
        let changes = iterate_changeset(&other).unwrap();
        let err = validate_changeset_tables_registered(&changes, SCHEMA_SQL).unwrap_err();
        assert!(err.to_string().contains("absent from registered schema"));
    }

    fn insert_changeset(table: &str) -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(&format!(
            "CREATE TABLE {table}(id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL);"
        ))
        .unwrap();
        let mut session = Session::new(&db).unwrap();
        session.attach::<&str>(None).unwrap();
        db.execute(&format!("INSERT INTO {table} VALUES (1, 'alpha')"), [])
            .unwrap();
        let mut output = Vec::new();
        session.changeset_strm(&mut output).unwrap();
        output
    }
}
