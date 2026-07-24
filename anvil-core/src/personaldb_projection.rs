use crate::{
    core_store::{decode_deterministic_proto, encode_deterministic_proto},
    formats::hash32,
    personaldb_coremeta::{
        PERSONALDB_DATA_LOCATOR_PAGE_MAX, list_personaldb_data_locator_rows,
        list_personaldb_data_locator_rows_for_tenant, personaldb_payload_hash,
        read_personaldb_data_locator_bytes, read_personaldb_data_locator_row,
        write_personaldb_bytes_as_data_locator,
    },
    storage::Storage,
};
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

const PERSONALDB_PROJECTION_DEFINITION_PREFIX: &str = "personaldb_projection_definition:";
const PERSONALDB_PROJECTION_DEFINITION_KIND: &str = "projection_definition";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionDefinition {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub projection_id: String,
    pub source_database_ids: Vec<String>,
    pub target_database_id: String,
    pub target_actor_or_scope: String,
    pub table_mappings: Vec<TableMapping>,
    pub column_mappings: Vec<ColumnMapping>,
    pub row_filters: Vec<RowFilter>,
    pub resource_bindings: Vec<ProjectionResourceBinding>,
    pub writeback_policy: WriteBackPolicy,
    pub definition_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableMapping {
    pub source_database_id: String,
    pub source_table: String,
    pub target_table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnMapping {
    pub source_table: String,
    pub source_column: String,
    pub target_table: String,
    pub target_column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowFilter {
    FieldEqualsLiteral {
        table: String,
        field: String,
        literal: String,
    },
    FieldInAuthorizedResourceSet {
        table: String,
        field: String,
        resource_set: String,
    },
    ResourceRelationAllows {
        table: String,
        resource_id_field: String,
        relation: String,
    },
    ParentRelationAllows {
        table: String,
        parent_resource_id_field: String,
        relation: String,
    },
    NotDeleted {
        table: String,
        deleted_field: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProjectionResourceBinding {
    pub source_table: String,
    pub primary_key_column: String,
    pub resource_type: String,
    pub resource_id_column: String,
    pub parent_resource_id_column: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WriteBackPolicy {
    Deny,
    AllowMappedColumns {
        protected_columns: Vec<String>,
        allowed_columns: Vec<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum RowFilterKindProto {
    Unspecified = 0,
    FieldEqualsLiteral = 1,
    FieldInAuthorizedResourceSet = 2,
    ResourceRelationAllows = 3,
    ParentRelationAllows = 4,
    NotDeleted = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum WriteBackPolicyKindProto {
    Unspecified = 0,
    Deny = 1,
    AllowMappedColumns = 2,
}

#[derive(Clone, PartialEq, Message)]
struct ProjectionDefinitionProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(string, tag = "4")]
    projection_id: String,
    #[prost(string, repeated, tag = "5")]
    source_database_ids: Vec<String>,
    #[prost(string, tag = "6")]
    target_database_id: String,
    #[prost(string, tag = "7")]
    target_actor_or_scope: String,
    #[prost(message, repeated, tag = "8")]
    table_mappings: Vec<TableMappingProto>,
    #[prost(message, repeated, tag = "9")]
    column_mappings: Vec<ColumnMappingProto>,
    #[prost(message, repeated, tag = "10")]
    row_filters: Vec<RowFilterProto>,
    #[prost(message, repeated, tag = "11")]
    resource_bindings: Vec<ProjectionResourceBindingProto>,
    #[prost(message, optional, tag = "12")]
    writeback_policy: Option<WriteBackPolicyProto>,
    #[prost(string, optional, tag = "13")]
    definition_hash: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct TableMappingProto {
    #[prost(string, tag = "1")]
    source_database_id: String,
    #[prost(string, tag = "2")]
    source_table: String,
    #[prost(string, tag = "3")]
    target_table: String,
}

#[derive(Clone, PartialEq, Message)]
struct ColumnMappingProto {
    #[prost(string, tag = "1")]
    source_table: String,
    #[prost(string, tag = "2")]
    source_column: String,
    #[prost(string, tag = "3")]
    target_table: String,
    #[prost(string, tag = "4")]
    target_column: String,
}

#[derive(Clone, PartialEq, Message)]
struct RowFilterProto {
    #[prost(enumeration = "RowFilterKindProto", tag = "1")]
    kind: i32,
    #[prost(string, tag = "2")]
    table: String,
    #[prost(string, tag = "3")]
    field: String,
    #[prost(string, tag = "4")]
    literal: String,
    #[prost(string, tag = "5")]
    resource_set: String,
    #[prost(string, tag = "6")]
    resource_id_field: String,
    #[prost(string, tag = "7")]
    relation: String,
    #[prost(string, tag = "8")]
    parent_resource_id_field: String,
    #[prost(string, tag = "9")]
    deleted_field: String,
}

#[derive(Clone, PartialEq, Message)]
struct ProjectionResourceBindingProto {
    #[prost(string, tag = "1")]
    source_table: String,
    #[prost(string, tag = "2")]
    primary_key_column: String,
    #[prost(string, tag = "3")]
    resource_type: String,
    #[prost(string, tag = "4")]
    resource_id_column: String,
    #[prost(string, optional, tag = "5")]
    parent_resource_id_column: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct WriteBackPolicyProto {
    #[prost(enumeration = "WriteBackPolicyKindProto", tag = "1")]
    kind: i32,
    #[prost(string, repeated, tag = "2")]
    protected_columns: Vec<String>,
    #[prost(string, repeated, tag = "3")]
    allowed_columns: Vec<String>,
}

impl ProjectionDefinition {
    pub fn seal(mut self) -> Result<Self> {
        canonicalize_projection_definition(&mut self);
        validate_projection_definition_unsigned(&self)?;
        self.definition_hash = Some(hash_projection_definition(&self)?);
        Ok(self)
    }

    pub fn verify(&self) -> Result<()> {
        let mut canonical = self.clone();
        canonicalize_projection_definition(&mut canonical);
        validate_projection_definition_unsigned(&canonical)?;
        if canonical != *self {
            return Err(anyhow!("projection definition is not canonical"));
        }
        let expected = hash_projection_definition(self)?;
        if self.definition_hash.as_deref() != Some(expected.as_str()) {
            return Err(anyhow!("projection definition hash mismatch"));
        }
        Ok(())
    }
}

pub fn hash_projection_definition(definition: &ProjectionDefinition) -> Result<String> {
    let mut unsigned = definition.clone();
    unsigned.definition_hash = None;
    Ok(hex::encode(hash32(&encode_projection_definition(
        &unsigned,
    )?)))
}

pub async fn write_projection_definition(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    definition: &ProjectionDefinition,
) -> Result<()> {
    definition.verify()?;
    ensure_scope(tenant_id, database_id, definition)?;
    let data_id = projection_definition_data_id(tenant_id, database_id, &definition.projection_id)?;
    let bytes = encode_projection_definition(definition)?;
    let generation = read_personaldb_data_locator_row(storage, tenant_id, database_id, &data_id)
        .await?
        .map(|row| row.generation.saturating_add(1))
        .unwrap_or(1);
    write_personaldb_bytes_as_data_locator(
        storage,
        tenant_id,
        database_id,
        &data_id,
        PERSONALDB_PROJECTION_DEFINITION_KIND,
        generation,
        bytes.clone(),
        personaldb_payload_hash(&bytes),
        definition.source_database_ids.clone(),
        format!(
            "personaldb-projection-definition:{tenant_id}:{database_id}:{}",
            definition.projection_id
        ),
    )
    .await?;
    Ok(())
}

pub async fn read_projection_definition(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
) -> Result<Option<ProjectionDefinition>> {
    let data_id = projection_definition_data_id(tenant_id, database_id, projection_id)?;
    let Some(definition) =
        read_projection_definition_row(storage, tenant_id, database_id, &data_id).await?
    else {
        return Ok(None);
    };
    ensure_scope(tenant_id, database_id, &definition)?;
    if definition.projection_id != projection_id {
        return Err(anyhow!("projection definition data scope mismatch"));
    }
    Ok(Some(definition))
}

pub async fn list_projection_definitions_for_source(
    storage: &Storage,
    tenant_id: i64,
    source_database_id: &str,
) -> Result<Vec<ProjectionDefinition>> {
    let mut definitions = Vec::new();
    let mut after_tuple_key = None;
    loop {
        let page = list_personaldb_data_locator_rows_for_tenant(
            storage,
            tenant_id,
            after_tuple_key.as_deref(),
            PERSONALDB_DATA_LOCATOR_PAGE_MAX,
        )
        .await?;
        for row in page.rows {
            if row.data_kind != PERSONALDB_PROJECTION_DEFINITION_KIND {
                continue;
            }
            let Some(definition) =
                read_projection_definition_row(storage, tenant_id, &row.group_id, &row.data_id)
                    .await?
            else {
                continue;
            };
            if definition
                .source_database_ids
                .iter()
                .any(|source| source == source_database_id)
            {
                definitions.push(definition);
            }
        }
        let Some(next_tuple_key) = page.next_tuple_key else {
            break;
        };
        after_tuple_key = Some(next_tuple_key);
    }
    definitions.sort_by(|left, right| {
        left.database_id
            .cmp(&right.database_id)
            .then_with(|| left.projection_id.cmp(&right.projection_id))
    });
    Ok(definitions)
}

pub async fn list_projection_definitions_for_database(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
) -> Result<Vec<ProjectionDefinition>> {
    let mut definitions = Vec::new();
    let mut after_tuple_key = None;
    loop {
        let page = list_personaldb_data_locator_rows(
            storage,
            tenant_id,
            database_id,
            after_tuple_key.as_deref(),
            PERSONALDB_DATA_LOCATOR_PAGE_MAX,
        )
        .await?;
        for row in page.rows {
            if row.data_kind != PERSONALDB_PROJECTION_DEFINITION_KIND {
                continue;
            }
            if let Some(definition) =
                read_projection_definition_row(storage, tenant_id, database_id, &row.data_id)
                    .await?
            {
                definitions.push(definition);
            };
        }
        let Some(next_tuple_key) = page.next_tuple_key else {
            break;
        };
        after_tuple_key = Some(next_tuple_key);
    }
    definitions.sort_by(|left, right| left.projection_id.cmp(&right.projection_id));
    Ok(definitions)
}

async fn read_projection_definition_row(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    data_id: &str,
) -> Result<Option<ProjectionDefinition>> {
    let Some(row) =
        read_personaldb_data_locator_row(storage, tenant_id, database_id, data_id).await?
    else {
        return Ok(None);
    };
    if row.data_kind != PERSONALDB_PROJECTION_DEFINITION_KIND {
        return Err(anyhow!("projection definition CoreMeta row kind mismatch"));
    }
    let bytes = read_personaldb_data_locator_bytes(storage, &row).await?;
    let definition = decode_projection_definition(&bytes)?;
    definition.verify()?;
    Ok(Some(definition))
}

fn encode_projection_definition(definition: &ProjectionDefinition) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&projection_to_proto(definition)))
}

fn decode_projection_definition(bytes: &[u8]) -> Result<ProjectionDefinition> {
    projection_from_proto(decode_deterministic_proto::<ProjectionDefinitionProto>(
        bytes,
        "personaldb projection definition",
    )?)
}

fn projection_to_proto(definition: &ProjectionDefinition) -> ProjectionDefinitionProto {
    ProjectionDefinitionProto {
        format_version: u32::from(definition.format_version),
        tenant_id: definition.tenant_id.clone(),
        database_id: definition.database_id.clone(),
        projection_id: definition.projection_id.clone(),
        source_database_ids: definition.source_database_ids.clone(),
        target_database_id: definition.target_database_id.clone(),
        target_actor_or_scope: definition.target_actor_or_scope.clone(),
        table_mappings: definition
            .table_mappings
            .iter()
            .map(table_mapping_to_proto)
            .collect(),
        column_mappings: definition
            .column_mappings
            .iter()
            .map(column_mapping_to_proto)
            .collect(),
        row_filters: definition
            .row_filters
            .iter()
            .map(row_filter_to_proto)
            .collect(),
        resource_bindings: definition
            .resource_bindings
            .iter()
            .map(resource_binding_to_proto)
            .collect(),
        writeback_policy: Some(writeback_policy_to_proto(&definition.writeback_policy)),
        definition_hash: definition.definition_hash.clone(),
    }
}

fn projection_from_proto(proto: ProjectionDefinitionProto) -> Result<ProjectionDefinition> {
    Ok(ProjectionDefinition {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("projection definition version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        database_id: proto.database_id,
        projection_id: proto.projection_id,
        source_database_ids: proto.source_database_ids,
        target_database_id: proto.target_database_id,
        target_actor_or_scope: proto.target_actor_or_scope,
        table_mappings: proto
            .table_mappings
            .into_iter()
            .map(table_mapping_from_proto)
            .collect(),
        column_mappings: proto
            .column_mappings
            .into_iter()
            .map(column_mapping_from_proto)
            .collect(),
        row_filters: proto
            .row_filters
            .into_iter()
            .map(row_filter_from_proto)
            .collect::<Result<Vec<_>>>()?,
        resource_bindings: proto
            .resource_bindings
            .into_iter()
            .map(resource_binding_from_proto)
            .collect(),
        writeback_policy: writeback_policy_from_proto(
            proto
                .writeback_policy
                .ok_or_else(|| anyhow!("projection definition missing writeback policy"))?,
        )?,
        definition_hash: proto.definition_hash,
    })
}

fn table_mapping_to_proto(mapping: &TableMapping) -> TableMappingProto {
    TableMappingProto {
        source_database_id: mapping.source_database_id.clone(),
        source_table: mapping.source_table.clone(),
        target_table: mapping.target_table.clone(),
    }
}

fn table_mapping_from_proto(proto: TableMappingProto) -> TableMapping {
    TableMapping {
        source_database_id: proto.source_database_id,
        source_table: proto.source_table,
        target_table: proto.target_table,
    }
}

fn column_mapping_to_proto(mapping: &ColumnMapping) -> ColumnMappingProto {
    ColumnMappingProto {
        source_table: mapping.source_table.clone(),
        source_column: mapping.source_column.clone(),
        target_table: mapping.target_table.clone(),
        target_column: mapping.target_column.clone(),
    }
}

fn column_mapping_from_proto(proto: ColumnMappingProto) -> ColumnMapping {
    ColumnMapping {
        source_table: proto.source_table,
        source_column: proto.source_column,
        target_table: proto.target_table,
        target_column: proto.target_column,
    }
}

fn row_filter_to_proto(filter: &RowFilter) -> RowFilterProto {
    match filter {
        RowFilter::FieldEqualsLiteral {
            table,
            field,
            literal,
        } => RowFilterProto {
            kind: RowFilterKindProto::FieldEqualsLiteral as i32,
            table: table.clone(),
            field: field.clone(),
            literal: literal.clone(),
            resource_set: String::new(),
            resource_id_field: String::new(),
            relation: String::new(),
            parent_resource_id_field: String::new(),
            deleted_field: String::new(),
        },
        RowFilter::FieldInAuthorizedResourceSet {
            table,
            field,
            resource_set,
        } => RowFilterProto {
            kind: RowFilterKindProto::FieldInAuthorizedResourceSet as i32,
            table: table.clone(),
            field: field.clone(),
            resource_set: resource_set.clone(),
            literal: String::new(),
            resource_id_field: String::new(),
            relation: String::new(),
            parent_resource_id_field: String::new(),
            deleted_field: String::new(),
        },
        RowFilter::ResourceRelationAllows {
            table,
            resource_id_field,
            relation,
        } => RowFilterProto {
            kind: RowFilterKindProto::ResourceRelationAllows as i32,
            table: table.clone(),
            resource_id_field: resource_id_field.clone(),
            relation: relation.clone(),
            field: String::new(),
            literal: String::new(),
            resource_set: String::new(),
            parent_resource_id_field: String::new(),
            deleted_field: String::new(),
        },
        RowFilter::ParentRelationAllows {
            table,
            parent_resource_id_field,
            relation,
        } => RowFilterProto {
            kind: RowFilterKindProto::ParentRelationAllows as i32,
            table: table.clone(),
            parent_resource_id_field: parent_resource_id_field.clone(),
            relation: relation.clone(),
            field: String::new(),
            literal: String::new(),
            resource_set: String::new(),
            resource_id_field: String::new(),
            deleted_field: String::new(),
        },
        RowFilter::NotDeleted {
            table,
            deleted_field,
        } => RowFilterProto {
            kind: RowFilterKindProto::NotDeleted as i32,
            table: table.clone(),
            deleted_field: deleted_field.clone(),
            field: String::new(),
            literal: String::new(),
            resource_set: String::new(),
            resource_id_field: String::new(),
            relation: String::new(),
            parent_resource_id_field: String::new(),
        },
    }
}

fn row_filter_from_proto(proto: RowFilterProto) -> Result<RowFilter> {
    match RowFilterKindProto::try_from(proto.kind)
        .map_err(|_| anyhow!("projection row filter kind is invalid"))?
    {
        RowFilterKindProto::FieldEqualsLiteral => Ok(RowFilter::FieldEqualsLiteral {
            table: proto.table,
            field: proto.field,
            literal: proto.literal,
        }),
        RowFilterKindProto::FieldInAuthorizedResourceSet => {
            Ok(RowFilter::FieldInAuthorizedResourceSet {
                table: proto.table,
                field: proto.field,
                resource_set: proto.resource_set,
            })
        }
        RowFilterKindProto::ResourceRelationAllows => Ok(RowFilter::ResourceRelationAllows {
            table: proto.table,
            resource_id_field: proto.resource_id_field,
            relation: proto.relation,
        }),
        RowFilterKindProto::ParentRelationAllows => Ok(RowFilter::ParentRelationAllows {
            table: proto.table,
            parent_resource_id_field: proto.parent_resource_id_field,
            relation: proto.relation,
        }),
        RowFilterKindProto::NotDeleted => Ok(RowFilter::NotDeleted {
            table: proto.table,
            deleted_field: proto.deleted_field,
        }),
        RowFilterKindProto::Unspecified => {
            Err(anyhow!("projection row filter kind is unspecified"))
        }
    }
}

fn resource_binding_to_proto(
    binding: &ProjectionResourceBinding,
) -> ProjectionResourceBindingProto {
    ProjectionResourceBindingProto {
        source_table: binding.source_table.clone(),
        primary_key_column: binding.primary_key_column.clone(),
        resource_type: binding.resource_type.clone(),
        resource_id_column: binding.resource_id_column.clone(),
        parent_resource_id_column: binding.parent_resource_id_column.clone(),
    }
}

fn resource_binding_from_proto(proto: ProjectionResourceBindingProto) -> ProjectionResourceBinding {
    ProjectionResourceBinding {
        source_table: proto.source_table,
        primary_key_column: proto.primary_key_column,
        resource_type: proto.resource_type,
        resource_id_column: proto.resource_id_column,
        parent_resource_id_column: proto.parent_resource_id_column,
    }
}

fn writeback_policy_to_proto(policy: &WriteBackPolicy) -> WriteBackPolicyProto {
    match policy {
        WriteBackPolicy::Deny => WriteBackPolicyProto {
            kind: WriteBackPolicyKindProto::Deny as i32,
            protected_columns: Vec::new(),
            allowed_columns: Vec::new(),
        },
        WriteBackPolicy::AllowMappedColumns {
            protected_columns,
            allowed_columns,
        } => WriteBackPolicyProto {
            kind: WriteBackPolicyKindProto::AllowMappedColumns as i32,
            protected_columns: protected_columns.clone(),
            allowed_columns: allowed_columns.clone(),
        },
    }
}

fn writeback_policy_from_proto(proto: WriteBackPolicyProto) -> Result<WriteBackPolicy> {
    match WriteBackPolicyKindProto::try_from(proto.kind)
        .map_err(|_| anyhow!("projection writeback policy kind is invalid"))?
    {
        WriteBackPolicyKindProto::Deny => Ok(WriteBackPolicy::Deny),
        WriteBackPolicyKindProto::AllowMappedColumns => Ok(WriteBackPolicy::AllowMappedColumns {
            protected_columns: proto.protected_columns,
            allowed_columns: proto.allowed_columns,
        }),
        WriteBackPolicyKindProto::Unspecified => {
            Err(anyhow!("projection writeback policy kind is unspecified"))
        }
    }
}

fn canonicalize_projection_definition(definition: &mut ProjectionDefinition) {
    definition.source_database_ids.sort();
    definition.table_mappings.sort();
    definition.column_mappings.sort();
    definition.row_filters.sort();
    definition.resource_bindings.sort();
    if let WriteBackPolicy::AllowMappedColumns {
        protected_columns,
        allowed_columns,
    } = &mut definition.writeback_policy
    {
        protected_columns.sort();
        allowed_columns.sort();
    }
}

fn validate_projection_definition_unsigned(definition: &ProjectionDefinition) -> Result<()> {
    if definition.format_version != 1 {
        return Err(anyhow!("unsupported projection definition version"));
    }
    require_nonempty(&definition.tenant_id, "tenant_id")?;
    require_nonempty(&definition.database_id, "database_id")?;
    require_nonempty(&definition.projection_id, "projection_id")?;
    require_nonempty(&definition.target_database_id, "target_database_id")?;
    require_nonempty(&definition.target_actor_or_scope, "target_actor_or_scope")?;
    validate_nonempty_unique_strings(&definition.source_database_ids, "source_database_ids")?;
    validate_unique(&definition.table_mappings, "table_mappings")?;
    validate_unique(&definition.column_mappings, "column_mappings")?;
    validate_unique(&definition.row_filters, "row_filters")?;
    validate_unique(&definition.resource_bindings, "resource_bindings")?;
    for mapping in &definition.table_mappings {
        validate_table_mapping(mapping)?;
        if !definition
            .source_database_ids
            .contains(&mapping.source_database_id)
        {
            return Err(anyhow!(
                "table mapping source database is not admitted by projection"
            ));
        }
    }
    for mapping in &definition.column_mappings {
        validate_column_mapping(mapping)?;
    }
    for filter in &definition.row_filters {
        validate_row_filter(filter)?;
    }
    for binding in &definition.resource_bindings {
        validate_resource_binding(binding)?;
    }
    validate_writeback_policy(&definition.writeback_policy)?;
    Ok(())
}

fn validate_table_mapping(mapping: &TableMapping) -> Result<()> {
    require_nonempty(&mapping.source_database_id, "source_database_id")?;
    require_nonempty(&mapping.source_table, "source_table")?;
    require_nonempty(&mapping.target_table, "target_table")?;
    Ok(())
}

fn validate_column_mapping(mapping: &ColumnMapping) -> Result<()> {
    require_nonempty(&mapping.source_table, "source_table")?;
    require_nonempty(&mapping.source_column, "source_column")?;
    require_nonempty(&mapping.target_table, "target_table")?;
    require_nonempty(&mapping.target_column, "target_column")?;
    Ok(())
}

fn validate_row_filter(filter: &RowFilter) -> Result<()> {
    match filter {
        RowFilter::FieldEqualsLiteral {
            table,
            field,
            literal,
        } => {
            require_nonempty(table, "row_filter.table")?;
            require_nonempty(field, "row_filter.field")?;
            require_nonempty(literal, "row_filter.literal")?;
        }
        RowFilter::FieldInAuthorizedResourceSet {
            table,
            field,
            resource_set,
        } => {
            require_nonempty(table, "row_filter.table")?;
            require_nonempty(field, "row_filter.field")?;
            require_nonempty(resource_set, "row_filter.resource_set")?;
        }
        RowFilter::ResourceRelationAllows {
            table,
            resource_id_field,
            relation,
        } => {
            require_nonempty(table, "row_filter.table")?;
            require_nonempty(resource_id_field, "row_filter.resource_id_field")?;
            require_nonempty(relation, "row_filter.relation")?;
        }
        RowFilter::ParentRelationAllows {
            table,
            parent_resource_id_field,
            relation,
        } => {
            require_nonempty(table, "row_filter.table")?;
            require_nonempty(
                parent_resource_id_field,
                "row_filter.parent_resource_id_field",
            )?;
            require_nonempty(relation, "row_filter.relation")?;
        }
        RowFilter::NotDeleted {
            table,
            deleted_field,
        } => {
            require_nonempty(table, "row_filter.table")?;
            require_nonempty(deleted_field, "row_filter.deleted_field")?;
        }
    }
    Ok(())
}

fn validate_resource_binding(binding: &ProjectionResourceBinding) -> Result<()> {
    require_nonempty(&binding.source_table, "source_table")?;
    require_nonempty(&binding.primary_key_column, "primary_key_column")?;
    require_nonempty(&binding.resource_type, "resource_type")?;
    require_nonempty(&binding.resource_id_column, "resource_id_column")?;
    if binding
        .parent_resource_id_column
        .as_deref()
        .is_some_and(str::is_empty)
    {
        return Err(anyhow!("parent_resource_id_column must not be empty"));
    }
    Ok(())
}

fn validate_writeback_policy(policy: &WriteBackPolicy) -> Result<()> {
    match policy {
        WriteBackPolicy::Deny => Ok(()),
        WriteBackPolicy::AllowMappedColumns {
            protected_columns,
            allowed_columns,
        } => {
            validate_nonempty_unique_strings(allowed_columns, "allowed_columns")?;
            let mut protected = std::collections::BTreeSet::new();
            for column in protected_columns {
                require_nonempty(column, "protected_columns")?;
                if !protected.insert(column) {
                    return Err(anyhow!("protected_columns contains duplicates"));
                }
                if allowed_columns.contains(column) {
                    return Err(anyhow!(
                        "writeback column cannot be both protected and allowed"
                    ));
                }
            }
            Ok(())
        }
    }
}

fn validate_nonempty_unique_strings(values: &[String], field: &'static str) -> Result<()> {
    if values.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for value in values {
        require_nonempty(value, field)?;
        if !seen.insert(value) {
            return Err(anyhow!("{field} contains duplicates"));
        }
    }
    Ok(())
}

fn validate_unique<T>(values: &[T], field: &'static str) -> Result<()>
where
    T: Ord,
{
    if values.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    for pair in values.windows(2) {
        if pair[0] == pair[1] {
            return Err(anyhow!("{field} contains duplicates"));
        }
    }
    Ok(())
}

fn ensure_scope(
    expected_tenant_id: i64,
    expected_database_id: &str,
    definition: &ProjectionDefinition,
) -> Result<()> {
    if definition.tenant_id != expected_tenant_id.to_string() {
        return Err(anyhow!("projection definition tenant scope mismatch"));
    }
    if definition.database_id != expected_database_id {
        return Err(anyhow!("projection definition database scope mismatch"));
    }
    Ok(())
}

fn projection_definition_data_prefix(tenant_id: i64, database_id: &str) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!(
            "projection definition tenant id must be nonnegative"
        ));
    }
    require_safe_component(database_id, "database_id")?;
    Ok(format!(
        "{PERSONALDB_PROJECTION_DEFINITION_PREFIX}tenant:{tenant_id}:database:{database_id}:"
    ))
}

fn projection_definition_data_id(
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
) -> Result<String> {
    require_safe_component(projection_id, "projection_id")?;
    Ok(format!(
        "{}projection:{projection_id}",
        projection_definition_data_prefix(tenant_id, database_id)?
    ))
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
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

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn projection_definition_seals_verifies_and_canonicalizes() {
        let mut definition = sample_definition();
        definition.source_database_ids.reverse();
        definition.column_mappings.reverse();
        let definition = definition.seal().unwrap();
        definition.verify().unwrap();
        assert_eq!(definition.definition_hash.as_deref().unwrap().len(), 64);
        assert_eq!(definition.source_database_ids, vec!["db-alpha", "db-beta"]);
    }

    #[test]
    fn projection_definition_rejects_tampering_and_invalid_writeback() {
        let mut definition = sample_definition().seal().unwrap();
        definition.target_actor_or_scope = "scope-b".to_string();
        assert!(definition.verify().is_err());

        let mut invalid = sample_definition();
        invalid.writeback_policy = WriteBackPolicy::AllowMappedColumns {
            protected_columns: vec!["status".to_string()],
            allowed_columns: vec!["status".to_string()],
        };
        assert!(invalid.seal().is_err());
    }

    #[test]
    fn projection_definition_rejects_unadmitted_source_and_empty_filters() {
        let mut invalid_source = sample_definition();
        invalid_source.table_mappings[0].source_database_id = "db-missing".to_string();
        assert!(invalid_source.seal().is_err());

        let mut invalid_filter = sample_definition();
        invalid_filter.row_filters[0] = RowFilter::FieldEqualsLiteral {
            table: "invoice".to_string(),
            field: "status".to_string(),
            literal: String::new(),
        };
        assert!(invalid_filter.seal().is_err());
    }

    #[tokio::test]
    async fn projection_definition_round_trips_at_spec_path() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let definition = sample_definition().seal().unwrap();
        write_projection_definition(&storage, 7, "projection-db", &definition)
            .await
            .unwrap();
        let data_id = projection_definition_data_id(7, "projection-db", "projection-a").unwrap();
        assert!(
            data_id
                .starts_with("personaldb_projection_definition:tenant:7:database:projection-db:")
        );

        let read = read_projection_definition(&storage, 7, "projection-db", "projection-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, definition);
    }

    #[tokio::test]
    async fn projection_definitions_can_be_listed_by_source_group() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = sample_definition().seal().unwrap();
        write_projection_definition(&storage, 7, "projection-db", &first)
            .await
            .unwrap();
        let mut second = sample_definition();
        second.database_id = "projection-db-b".to_string();
        second.projection_id = "projection-b".to_string();
        second.source_database_ids = vec!["db-gamma".to_string()];
        second.table_mappings[0].source_database_id = "db-gamma".to_string();
        let second = second.seal().unwrap();
        write_projection_definition(&storage, 7, "projection-db-b", &second)
            .await
            .unwrap();

        let definitions = list_projection_definitions_for_source(&storage, 7, "db-alpha")
            .await
            .unwrap();
        assert_eq!(definitions, vec![first]);
        let missing = list_projection_definitions_for_source(&storage, 7, "db-missing")
            .await
            .unwrap();
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn projection_definition_rejects_scope_and_unsafe_path() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mut definition = sample_definition().seal().unwrap();
        definition.database_id = "other-db".to_string();
        definition = definition.seal().unwrap();
        assert!(
            write_projection_definition(&storage, 7, "projection-db", &definition)
                .await
                .is_err()
        );
        assert!(projection_definition_data_id(7, "projection-db", "../escape").is_err());
    }

    fn sample_definition() -> ProjectionDefinition {
        ProjectionDefinition {
            format_version: 1,
            tenant_id: "7".to_string(),
            database_id: "projection-db".to_string(),
            projection_id: "projection-a".to_string(),
            source_database_ids: vec!["db-beta".to_string(), "db-alpha".to_string()],
            target_database_id: "projection-db".to_string(),
            target_actor_or_scope: "scope-account-a".to_string(),
            table_mappings: vec![TableMapping {
                source_database_id: "db-alpha".to_string(),
                source_table: "invoice".to_string(),
                target_table: "invoice_projection".to_string(),
            }],
            column_mappings: vec![
                ColumnMapping {
                    source_table: "invoice".to_string(),
                    source_column: "status".to_string(),
                    target_table: "invoice_projection".to_string(),
                    target_column: "status".to_string(),
                },
                ColumnMapping {
                    source_table: "invoice".to_string(),
                    source_column: "total".to_string(),
                    target_table: "invoice_projection".to_string(),
                    target_column: "total".to_string(),
                },
            ],
            row_filters: vec![
                RowFilter::FieldEqualsLiteral {
                    table: "invoice".to_string(),
                    field: "deleted".to_string(),
                    literal: "false".to_string(),
                },
                RowFilter::ResourceRelationAllows {
                    table: "invoice".to_string(),
                    resource_id_field: "resource_id".to_string(),
                    relation: "viewer".to_string(),
                },
                RowFilter::NotDeleted {
                    table: "invoice".to_string(),
                    deleted_field: "deleted".to_string(),
                },
            ],
            resource_bindings: vec![ProjectionResourceBinding {
                source_table: "invoice".to_string(),
                primary_key_column: "id".to_string(),
                resource_type: "invoice".to_string(),
                resource_id_column: "resource_id".to_string(),
                parent_resource_id_column: Some("account_id".to_string()),
            }],
            writeback_policy: WriteBackPolicy::AllowMappedColumns {
                protected_columns: vec!["id".to_string()],
                allowed_columns: vec!["status".to_string(), "total".to_string()],
            },
            definition_hash: None,
        }
    }
}
