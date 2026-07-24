use crate::{
    anvil_api::{
        AuthzAllowedSubject, AuthzNamespaceSchema, AuthzRelationRule, AuthzRelationSchema,
    },
    authz_coremeta_payload::{decode_authz_payload_row, encode_authz_payload_row},
    core_store::{
        CF_AUTHZ, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaRowCommonProto, CoreMetaTuplePart,
        CoreMetaVisibilityState, CoreStore, TABLE_AUTHZ_SCHEMA_ROW,
        commit_coremeta_batch_for_storage, core_meta_committed_row_common,
        core_meta_record_tuple_key, core_meta_root_key_hash, core_meta_row_common_from_payload,
        core_meta_tuple_key, decode_deterministic_proto, encode_deterministic_proto,
    },
    formats::hash32,
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use prost::Message;
use serde::{Deserialize, Serialize};

const AUTHZ_NAMESPACE_SCHEMA_ROW_KIND: &str = "namespace_schema";
pub const AUTHZ_NAMESPACE_SCHEMA_PAGE_MAX: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzNamespaceSchemaRecord {
    pub version: u16,
    pub tenant_id: i64,
    pub namespace: String,
    pub relations: Vec<AuthzRelationSchemaRecord>,
    pub schema_json: String,
    pub schema_hash: String,
    pub schema_version: u64,
    pub authz_revision: u64,
    pub applied_by: String,
    pub reason: String,
    pub applied_at: String,
    pub record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzRelationSchemaRecord {
    pub relation: String,
    pub rules: Vec<AuthzRelationRuleRecord>,
    pub member_kind: i32,
    pub allowed_subjects: Vec<AuthzAllowedSubjectRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzAllowedSubjectRecord {
    pub selector_kind: i32,
    pub subject_kind: String,
    pub subject_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzNamespaceSchemaPage {
    pub records: Vec<AuthzNamespaceSchemaRecord>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzRelationRuleRecord {
    pub kind: String,
    pub relation: String,
    pub tuple_relation: String,
    pub target_relation: String,
}

// Publication common belongs to the outer CoreMeta row, never this hashed domain payload.
#[derive(Clone, PartialEq, Message)]
struct AuthzNamespaceSchemaDomainProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    namespace: String,
    #[prost(message, repeated, tag = "4")]
    relations: Vec<AuthzRelationSchemaRecordProto>,
    #[prost(string, tag = "5")]
    schema_json: String,
    #[prost(string, tag = "6")]
    schema_hash: String,
    #[prost(uint64, tag = "7")]
    schema_version: u64,
    #[prost(uint64, tag = "8")]
    authz_revision: u64,
    #[prost(string, tag = "9")]
    applied_by: String,
    #[prost(string, tag = "10")]
    reason: String,
    #[prost(string, tag = "11")]
    applied_at: String,
    #[prost(string, tag = "12")]
    record_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzRelationSchemaRecordProto {
    #[prost(string, tag = "1")]
    relation: String,
    #[prost(message, repeated, tag = "2")]
    rules: Vec<AuthzRelationRuleRecordProto>,
    #[prost(int32, tag = "3")]
    member_kind: i32,
    #[prost(message, repeated, tag = "4")]
    allowed_subjects: Vec<AuthzAllowedSubjectRecordProto>,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzAllowedSubjectRecordProto {
    #[prost(int32, tag = "1")]
    selector_kind: i32,
    #[prost(string, tag = "2")]
    subject_kind: String,
    #[prost(string, tag = "3")]
    subject_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzRelationRuleRecordProto {
    #[prost(string, tag = "1")]
    kind: String,
    #[prost(string, tag = "2")]
    relation: String,
    #[prost(string, tag = "3")]
    tuple_relation: String,
    #[prost(string, tag = "4")]
    target_relation: String,
}

pub async fn write_authz_namespace_schema(
    storage: &Storage,
    tenant_id: i64,
    mut schema: AuthzNamespaceSchema,
    authz_revision: u64,
    applied_by: &str,
    reason: &str,
) -> Result<AuthzNamespaceSchemaRecord> {
    validate_namespace_schema(&schema)?;
    let previous = read_authz_namespace_schema(storage, tenant_id, &schema.namespace).await?;
    let schema_version = previous
        .as_ref()
        .map(|record| record.schema_version.saturating_add(1))
        .unwrap_or(1);
    let applied_at = Utc::now().to_rfc3339();
    schema.schema_hash = schema_hash(&schema)?;
    schema.schema_version = schema_version;
    schema.authz_revision = authz_revision;
    schema.applied_at = applied_at.clone();
    let mut record = AuthzNamespaceSchemaRecord {
        version: 1,
        tenant_id,
        namespace: schema.namespace,
        relations: schema
            .relations
            .into_iter()
            .map(AuthzRelationSchemaRecord::from)
            .collect(),
        schema_json: schema.schema_json,
        schema_hash: schema.schema_hash,
        schema_version,
        authz_revision,
        applied_by: applied_by.to_string(),
        reason: reason.to_string(),
        applied_at,
        record_hash: String::new(),
    };
    record.record_hash = record_hash(&record)?;
    validate_record(&record, tenant_id, &record.namespace)?;
    write_namespace_schema_row(storage, &record).await?;
    Ok(record)
}

pub async fn read_authz_namespace_schema(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
) -> Result<Option<AuthzNamespaceSchemaRecord>> {
    let Some(record) = read_namespace_schema_row(storage, tenant_id, namespace).await? else {
        return Ok(None);
    };
    validate_record(&record, tenant_id, namespace)?;
    Ok(Some(record))
}

pub async fn page_authz_namespace_schemas(
    storage: &Storage,
    tenant_id: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<AuthzNamespaceSchemaPage> {
    if !(1..=AUTHZ_NAMESPACE_SCHEMA_PAGE_MAX).contains(&page_size) {
        return Err(anyhow!(
            "authorization namespace schema page size must be between 1 and {AUTHZ_NAMESPACE_SCHEMA_PAGE_MAX}"
        ));
    }
    let prefix = namespace_schema_tuple_prefix(tenant_id)?;
    if after_tuple_key
        .is_some_and(|cursor| cursor.len() <= prefix.len() || !cursor.starts_with(&prefix))
    {
        return Err(anyhow!(
            "authorization namespace schema cursor is outside the tenant prefix"
        ));
    }
    let store = CoreStore::new(storage.clone()).await?;
    let mut rows = store.scan_coremeta_prefix_page(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &prefix,
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
                    .ok_or_else(|| anyhow!("authorization namespace schema page is empty"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let record = decode_namespace_schema_row_payload(storage, tenant_id, &row.payload).await?;
        validate_record(&record, tenant_id, &record.namespace)?;
        if core_meta_record_tuple_key(&row.key)?
            != namespace_schema_tuple_key(tenant_id, &record.namespace)?.as_slice()
        {
            return Err(anyhow!(
                "authorization namespace schema physical row scope mismatch"
            ));
        }
        records.push(record);
    }
    Ok(AuthzNamespaceSchemaPage {
        records,
        next_tuple_key,
    })
}

pub fn schema_response(record: &AuthzNamespaceSchemaRecord) -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: record.namespace.clone(),
        relations: record
            .relations
            .iter()
            .map(AuthzRelationSchema::from)
            .collect(),
        schema_json: record.schema_json.clone(),
        schema_hash: record.schema_hash.clone(),
        schema_version: record.schema_version,
        authz_revision: record.authz_revision,
        applied_at: record.applied_at.clone(),
    }
}

fn validate_namespace_schema(schema: &AuthzNamespaceSchema) -> Result<()> {
    crate::authz_schema_contract::validate_namespace_shape(schema)
}

fn validate_record(
    record: &AuthzNamespaceSchemaRecord,
    tenant_id: i64,
    namespace: &str,
) -> Result<()> {
    if record.version != 1 {
        return Err(anyhow!(
            "unsupported authorization namespace schema version"
        ));
    }
    if record.tenant_id != tenant_id || record.namespace != namespace {
        return Err(anyhow!("authorization namespace schema scope mismatch"));
    }
    if record.schema_version == 0 {
        return Err(anyhow!(
            "authorization namespace schema version must be nonzero"
        ));
    }
    let expected_schema_hash = schema_hash(&schema_response(record))?;
    if expected_schema_hash != record.schema_hash {
        return Err(anyhow!("authorization namespace schema hash mismatch"));
    }
    let expected_record_hash = record_hash(record)?;
    if expected_record_hash != record.record_hash {
        return Err(anyhow!(
            "authorization namespace schema record hash mismatch"
        ));
    }
    Ok(())
}

fn validate_component(value: &str, name: &str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{name} must not be empty"));
    }
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!("{name} must be a safe component"));
    }
    Ok(())
}

fn schema_hash(schema: &AuthzNamespaceSchema) -> Result<String> {
    let canonical = canonical_schema(schema);
    Ok(hex::encode(hash32(&encode_authz_schema(&canonical))))
}

fn record_hash(record: &AuthzNamespaceSchemaRecord) -> Result<String> {
    let mut unsigned = record.clone();
    unsigned.record_hash.clear();
    Ok(hex::encode(hash32(&encode_namespace_schema_record(
        &unsigned,
    )?)))
}

fn canonical_schema(schema: &AuthzNamespaceSchema) -> AuthzNamespaceSchema {
    let mut schema = schema.clone();
    schema.schema_hash.clear();
    schema.schema_version = 0;
    schema.authz_revision = 0;
    schema.applied_at.clear();
    schema
        .relations
        .sort_by(|left, right| left.relation.cmp(&right.relation));
    for relation in &mut schema.relations {
        relation.rules.sort_by(|left, right| {
            (
                &left.kind,
                &left.relation,
                &left.tuple_relation,
                &left.target_relation,
            )
                .cmp(&(
                    &right.kind,
                    &right.relation,
                    &right.tuple_relation,
                    &right.target_relation,
                ))
        });
        relation.allowed_subjects.sort_by(|left, right| {
            (left.selector_kind, &left.subject_kind, &left.subject_id).cmp(&(
                right.selector_kind,
                &right.subject_kind,
                &right.subject_id,
            ))
        });
    }
    schema
}

async fn write_namespace_schema_row(
    storage: &Storage,
    record: &AuthzNamespaceSchemaRecord,
) -> Result<()> {
    validate_record(record, record.tenant_id, &record.namespace)?;
    let tuple_key = namespace_schema_tuple_key(record.tenant_id, &record.namespace)?;
    let record_payload = encode_namespace_schema_record(record)?;
    let payload = encode_authz_payload_row(
        storage,
        namespace_schema_publication_common(record.tenant_id),
        AUTHZ_NAMESPACE_SCHEMA_ROW_KIND,
        &format!("tenant/{}/namespace/{}", record.tenant_id, record.namespace),
        record.schema_version,
        &record.record_hash,
        record_payload,
    )
    .await?;
    let op = CoreMetaBatchOp {
        cf: CF_AUTHZ,
        table_id: TABLE_AUTHZ_SCHEMA_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    commit_coremeta_batch_for_storage(
        storage,
        &format!(
            "authz-namespace-schema:{}:{}:{}",
            record.tenant_id, record.namespace, record.schema_version
        ),
        &[op],
        &[crate::core_store::CoreMetaRootPublication::new(
            namespace_schema_root_anchor_key(record.tenant_id),
            crate::formats::writer::WriterFamily::Authz,
        )],
    )
    .await?;
    Ok(())
}

async fn read_namespace_schema_row(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
) -> Result<Option<AuthzNamespaceSchemaRecord>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(bytes) = store.read_coremeta_row(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &namespace_schema_tuple_key(tenant_id, namespace)?,
    )?
    else {
        return Ok(None);
    };
    let record = decode_namespace_schema_row_payload(storage, tenant_id, &bytes)
        .await
        .with_context(|| format!("decode authorization namespace schema {namespace}"))?;
    validate_record(&record, tenant_id, namespace)?;
    Ok(Some(record))
}

async fn decode_namespace_schema_row_payload(
    storage: &Storage,
    tenant_id: i64,
    row_payload: &[u8],
) -> Result<AuthzNamespaceSchemaRecord> {
    let common = core_meta_row_common_from_payload(row_payload)?;
    validate_namespace_schema_publication_common(tenant_id, &common)?;
    let record_payload = decode_authz_payload_row(
        storage,
        tenant_id,
        row_payload,
        AUTHZ_NAMESPACE_SCHEMA_ROW_KIND,
    )
    .await?;
    decode_namespace_schema_record(&record_payload)
}

fn encode_namespace_schema_record(record: &AuthzNamespaceSchemaRecord) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&namespace_record_to_proto(
        record,
    )))
}

fn decode_namespace_schema_record(bytes: &[u8]) -> Result<AuthzNamespaceSchemaRecord> {
    namespace_record_from_proto(
        decode_deterministic_proto::<AuthzNamespaceSchemaDomainProto>(
            bytes,
            "authorization namespace schema record",
        )?,
    )
}

fn encode_authz_schema(schema: &AuthzNamespaceSchema) -> Vec<u8> {
    encode_deterministic_proto(&authz_schema_to_proto(schema))
}

fn namespace_record_to_proto(
    record: &AuthzNamespaceSchemaRecord,
) -> AuthzNamespaceSchemaDomainProto {
    AuthzNamespaceSchemaDomainProto {
        version: u32::from(record.version),
        tenant_id: record.tenant_id,
        namespace: record.namespace.clone(),
        relations: record
            .relations
            .iter()
            .map(relation_record_to_proto)
            .collect(),
        schema_json: record.schema_json.clone(),
        schema_hash: record.schema_hash.clone(),
        schema_version: record.schema_version,
        authz_revision: record.authz_revision,
        applied_by: record.applied_by.clone(),
        reason: record.reason.clone(),
        applied_at: record.applied_at.clone(),
        record_hash: record.record_hash.clone(),
    }
}

fn namespace_schema_publication_common(tenant_id: i64) -> CoreMetaRowCommonProto {
    // CoreStore replaces the publication placeholders while committing the root.
    core_meta_committed_row_common(
        format!("tenant/{tenant_id}"),
        core_meta_root_key_hash(&namespace_schema_root_anchor_key(tenant_id)),
        1,
        String::new(),
        0,
    )
}

fn validate_namespace_schema_publication_common(
    tenant_id: i64,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    let expected = namespace_schema_publication_common(tenant_id);
    if common.realm_id != expected.realm_id {
        return Err(anyhow!(
            "authorization namespace schema CoreMeta realm mismatch"
        ));
    }
    if common.root_key_hash != expected.root_key_hash {
        return Err(anyhow!(
            "authorization namespace schema CoreMeta root mismatch"
        ));
    }
    if common.root_generation == 0 {
        return Err(anyhow!(
            "authorization namespace schema CoreMeta publication generation must be nonzero"
        ));
    }
    if common.transaction_id.is_empty() {
        return Err(anyhow!(
            "authorization namespace schema CoreMeta transaction must not be empty"
        ));
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        return Err(anyhow!(
            "authorization namespace schema CoreMeta row is not committed"
        ));
    }
    if common.payload_schema_version != expected.payload_schema_version {
        return Err(anyhow!(
            "authorization namespace schema CoreMeta payload version mismatch"
        ));
    }
    Ok(())
}

fn namespace_schema_root_anchor_key(tenant_id: i64) -> String {
    format!("authz-schema/{tenant_id}")
}

fn namespace_record_from_proto(
    proto: AuthzNamespaceSchemaDomainProto,
) -> Result<AuthzNamespaceSchemaRecord> {
    Ok(AuthzNamespaceSchemaRecord {
        version: u16::try_from(proto.version)
            .map_err(|_| anyhow!("authorization namespace schema version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        namespace: proto.namespace,
        relations: proto
            .relations
            .into_iter()
            .map(relation_record_from_proto)
            .collect(),
        schema_json: proto.schema_json,
        schema_hash: proto.schema_hash,
        schema_version: proto.schema_version,
        authz_revision: proto.authz_revision,
        applied_by: proto.applied_by,
        reason: proto.reason,
        applied_at: proto.applied_at,
        record_hash: proto.record_hash,
    })
}

fn authz_schema_to_proto(schema: &AuthzNamespaceSchema) -> AuthzNamespaceSchemaDomainProto {
    AuthzNamespaceSchemaDomainProto {
        version: 1,
        tenant_id: 0,
        namespace: schema.namespace.clone(),
        relations: schema
            .relations
            .iter()
            .map(|relation| AuthzRelationSchemaRecordProto {
                relation: relation.relation.clone(),
                rules: relation
                    .rules
                    .iter()
                    .map(|rule| AuthzRelationRuleRecordProto {
                        kind: rule.kind.clone(),
                        relation: rule.relation.clone(),
                        tuple_relation: rule.tuple_relation.clone(),
                        target_relation: rule.target_relation.clone(),
                    })
                    .collect(),
                member_kind: relation.member_kind,
                allowed_subjects: relation
                    .allowed_subjects
                    .iter()
                    .map(|selector| AuthzAllowedSubjectRecordProto {
                        selector_kind: selector.selector_kind,
                        subject_kind: selector.subject_kind.clone(),
                        subject_id: selector.subject_id.clone(),
                    })
                    .collect(),
            })
            .collect(),
        schema_json: schema.schema_json.clone(),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_by: String::new(),
        reason: String::new(),
        applied_at: String::new(),
        record_hash: String::new(),
    }
}

fn relation_record_to_proto(
    relation: &AuthzRelationSchemaRecord,
) -> AuthzRelationSchemaRecordProto {
    AuthzRelationSchemaRecordProto {
        relation: relation.relation.clone(),
        rules: relation.rules.iter().map(rule_record_to_proto).collect(),
        member_kind: relation.member_kind,
        allowed_subjects: relation
            .allowed_subjects
            .iter()
            .map(allowed_subject_record_to_proto)
            .collect(),
    }
}

fn relation_record_from_proto(proto: AuthzRelationSchemaRecordProto) -> AuthzRelationSchemaRecord {
    AuthzRelationSchemaRecord {
        relation: proto.relation,
        rules: proto
            .rules
            .into_iter()
            .map(rule_record_from_proto)
            .collect(),
        member_kind: proto.member_kind,
        allowed_subjects: proto
            .allowed_subjects
            .into_iter()
            .map(allowed_subject_record_from_proto)
            .collect(),
    }
}

fn allowed_subject_record_to_proto(
    selector: &AuthzAllowedSubjectRecord,
) -> AuthzAllowedSubjectRecordProto {
    AuthzAllowedSubjectRecordProto {
        selector_kind: selector.selector_kind,
        subject_kind: selector.subject_kind.clone(),
        subject_id: selector.subject_id.clone(),
    }
}

fn allowed_subject_record_from_proto(
    proto: AuthzAllowedSubjectRecordProto,
) -> AuthzAllowedSubjectRecord {
    AuthzAllowedSubjectRecord {
        selector_kind: proto.selector_kind,
        subject_kind: proto.subject_kind,
        subject_id: proto.subject_id,
    }
}

fn rule_record_to_proto(rule: &AuthzRelationRuleRecord) -> AuthzRelationRuleRecordProto {
    AuthzRelationRuleRecordProto {
        kind: rule.kind.clone(),
        relation: rule.relation.clone(),
        tuple_relation: rule.tuple_relation.clone(),
        target_relation: rule.target_relation.clone(),
    }
}

fn rule_record_from_proto(proto: AuthzRelationRuleRecordProto) -> AuthzRelationRuleRecord {
    AuthzRelationRuleRecord {
        kind: proto.kind,
        relation: proto.relation,
        tuple_relation: proto.tuple_relation,
        target_relation: proto.target_relation,
    }
}

fn namespace_schema_tuple_prefix(tenant_id: i64) -> Result<Vec<u8>> {
    if tenant_id < 0 {
        return Err(anyhow!(
            "authorization schema tenant id must be nonnegative"
        ));
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_NAMESPACE_SCHEMA_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
    ])
}

fn namespace_schema_tuple_key(tenant_id: i64, namespace: &str) -> Result<Vec<u8>> {
    if tenant_id < 0 {
        return Err(anyhow!(
            "authorization schema tenant id must be nonnegative"
        ));
    }
    validate_component(namespace, "namespace")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_NAMESPACE_SCHEMA_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(namespace),
    ])
}

impl From<AuthzRelationRule> for AuthzRelationRuleRecord {
    fn from(rule: AuthzRelationRule) -> Self {
        Self {
            kind: rule.kind,
            relation: rule.relation,
            tuple_relation: rule.tuple_relation,
            target_relation: rule.target_relation,
        }
    }
}

impl From<AuthzRelationSchema> for AuthzRelationSchemaRecord {
    fn from(schema: AuthzRelationSchema) -> Self {
        Self {
            relation: schema.relation,
            rules: schema
                .rules
                .into_iter()
                .map(AuthzRelationRuleRecord::from)
                .collect(),
            member_kind: schema.member_kind,
            allowed_subjects: schema
                .allowed_subjects
                .into_iter()
                .map(AuthzAllowedSubjectRecord::from)
                .collect(),
        }
    }
}

impl From<AuthzAllowedSubject> for AuthzAllowedSubjectRecord {
    fn from(selector: AuthzAllowedSubject) -> Self {
        Self {
            selector_kind: selector.selector_kind,
            subject_kind: selector.subject_kind,
            subject_id: selector.subject_id,
        }
    }
}

impl From<&AuthzRelationRuleRecord> for AuthzRelationRule {
    fn from(rule: &AuthzRelationRuleRecord) -> Self {
        Self {
            kind: rule.kind.clone(),
            relation: rule.relation.clone(),
            tuple_relation: rule.tuple_relation.clone(),
            target_relation: rule.target_relation.clone(),
        }
    }
}

impl From<&AuthzRelationSchemaRecord> for AuthzRelationSchema {
    fn from(schema: &AuthzRelationSchemaRecord) -> Self {
        Self {
            relation: schema.relation.clone(),
            rules: schema.rules.iter().map(AuthzRelationRule::from).collect(),
            member_kind: schema.member_kind,
            allowed_subjects: schema
                .allowed_subjects
                .iter()
                .map(AuthzAllowedSubject::from)
                .collect(),
        }
    }
}

impl From<&AuthzAllowedSubjectRecord> for AuthzAllowedSubject {
    fn from(selector: &AuthzAllowedSubjectRecord) -> Self {
        Self {
            selector_kind: selector.selector_kind,
            subject_kind: selector.subject_kind.clone(),
            subject_id: selector.subject_id.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anvil_api::{AuthzSchemaMemberKind, AuthzSubjectSelectorKind};
    use tempfile::tempdir;

    fn schema(namespace: &str) -> AuthzNamespaceSchema {
        AuthzNamespaceSchema {
            namespace: namespace.to_string(),
            relations: vec![
                AuthzRelationSchema {
                    relation: "viewer".to_string(),
                    rules: vec![AuthzRelationRule {
                        kind: "inherit".to_string(),
                        relation: "editor".to_string(),
                        tuple_relation: String::new(),
                        target_relation: String::new(),
                    }],
                    member_kind: AuthzSchemaMemberKind::Permission as i32,
                    allowed_subjects: Vec::new(),
                },
                AuthzRelationSchema {
                    relation: "editor".to_string(),
                    rules: Vec::new(),
                    member_kind: AuthzSchemaMemberKind::DirectRelation as i32,
                    allowed_subjects: vec![AuthzAllowedSubject {
                        selector_kind: AuthzSubjectSelectorKind::AnyCanonicalId as i32,
                        subject_kind: "user".to_string(),
                        subject_id: String::new(),
                    }],
                },
            ],
            schema_json: r#"{"namespace":"document"}"#.to_string(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        }
    }

    #[tokio::test]
    async fn namespace_schema_persists_versions_and_hashes() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first =
            write_authz_namespace_schema(&storage, 7, schema("document"), 10, "tester", "initial")
                .await
                .unwrap();
        assert_eq!(first.schema_version, 1);
        assert_eq!(first.authz_revision, 10);

        let second =
            write_authz_namespace_schema(&storage, 7, schema("document"), 11, "tester", "update")
                .await
                .unwrap();
        assert_eq!(second.schema_version, 2);

        let read = read_authz_namespace_schema(&storage, 7, "document")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.schema_version, 2);
        let page = page_authz_namespace_schemas(&storage, 7, None, 1)
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        assert!(page.next_tuple_key.is_none());
    }

    #[tokio::test]
    async fn namespace_schema_separates_logical_and_physical_generations() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        write_authz_namespace_schema(&storage, 7, schema("document"), 10, "tester", "first root")
            .await
            .unwrap();
        let record = write_authz_namespace_schema(
            &storage,
            7,
            schema("folder"),
            11,
            "tester",
            "second root",
        )
        .await
        .unwrap();

        let store = CoreStore::new(storage.clone()).await.unwrap();
        let row_payload = store
            .read_coremeta_row(
                CF_AUTHZ,
                TABLE_AUTHZ_SCHEMA_ROW,
                &namespace_schema_tuple_key(7, "folder").unwrap(),
            )
            .unwrap()
            .unwrap();
        let common = core_meta_row_common_from_payload(&row_payload).unwrap();

        assert_eq!(record.schema_version, 1);
        assert_eq!(common.root_generation, 2);
        assert_ne!(common.root_generation, record.schema_version);
        assert_eq!(common.transaction_id, "authz-namespace-schema:7:folder:1");
        assert_eq!(
            common.visibility_state_enum(),
            CoreMetaVisibilityState::Committed
        );
        assert_eq!(
            read_authz_namespace_schema(&storage, 7, "folder")
                .await
                .unwrap(),
            Some(record.clone())
        );

        let domain_payload = encode_namespace_schema_record(&record).unwrap();
        assert!(core_meta_row_common_from_payload(&domain_payload).is_err());
        assert_eq!(record_hash(&record).unwrap(), record.record_hash);
    }

    #[test]
    fn namespace_schema_publication_keeps_scope_and_visibility_strict() {
        let mut common = namespace_schema_publication_common(7);
        common.root_generation = 91;
        common.transaction_id = "corestore-assigned-transaction".to_string();
        assert!(validate_namespace_schema_publication_common(7, &common).is_ok());

        let mut wrong_realm = common.clone();
        wrong_realm.realm_id = "tenant/8".to_string();
        assert!(validate_namespace_schema_publication_common(7, &wrong_realm).is_err());

        let mut wrong_root = common.clone();
        wrong_root.root_key_hash = core_meta_root_key_hash("authz-schema/8");
        assert!(validate_namespace_schema_publication_common(7, &wrong_root).is_err());

        let mut pending = common;
        pending.visibility_state = CoreMetaVisibilityState::Pending as i32;
        assert!(validate_namespace_schema_publication_common(7, &pending).is_err());
    }

    #[tokio::test]
    async fn namespace_schema_rejects_unsafe_names_and_bad_rules() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        assert!(
            write_authz_namespace_schema(&storage, 7, schema("../bad"), 1, "tester", "bad")
                .await
                .is_err()
        );

        let mut bad = schema("document");
        bad.relations[0].rules[0].kind = "made_up".to_string();
        assert!(
            write_authz_namespace_schema(&storage, 7, bad, 1, "tester", "bad")
                .await
                .is_err()
        );
    }
}
