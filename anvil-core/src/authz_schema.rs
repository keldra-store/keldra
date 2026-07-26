use crate::{
    anvil_api::{
        AuthzAllowedSubject, AuthzNamespaceSchema, AuthzRelationRule, AuthzRelationSchema,
    },
    core_store::{
        CF_AUTHZ, CoreMetaTuplePart, TABLE_AUTHZ_SCHEMA_ROW, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
    },
    formats::hash32,
};
use anyhow::{Result, anyhow, bail};
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

// Canonical domain payload stored directly as the MVCC row value.
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    mut schema: AuthzNamespaceSchema,
    authz_revision: u64,
    applied_by: &str,
    reason: &str,
) -> Result<AuthzNamespaceSchemaRecord> {
    validate_namespace_schema(&schema)?;
    let assignment = if tenant_id == crate::system_realm::SYSTEM_STORAGE_TENANT_ID {
        None
    } else {
        Some(
            mvcc.reconcile_authz_tuple_assignment(tenant_id)
                .await?
                .ok_or_else(|| {
                    anyhow!("this node is not the assigned authorization schema writer")
                })?,
        )
    };
    let principal = crate::authz_head::transaction_principal(tenant_id);
    let requested_hash = schema_hash(&schema)?;
    let operation_hash = hex::encode(hash32(
        format!(
            "{tenant_id}\0{}\0{requested_hash}\0{authz_revision}\0{applied_by}\0{reason}",
            schema.namespace
        )
        .as_bytes(),
    ));
    let idempotency_key = format!(
        "authz-namespace-schema:{tenant_id}:{}:{operation_hash}",
        schema.namespace,
    );
    let now_unix_ms = current_unix_ms();
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            &principal,
            idempotency_key,
            std::time::Duration::from_secs(30),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now_unix_ms,
        )
        .await?;
    let key = namespace_schema_logical_key(tenant_id, &schema.namespace)?;
    let previous_payload = mvcc.read_transaction_value(&handle.transaction_id, &principal, &key)?;
    let previous = previous_payload
        .as_deref()
        .map(decode_namespace_schema_record)
        .transpose()?;
    let schema_version = previous
        .as_ref()
        .map(|record| record.schema_version.saturating_add(1))
        .unwrap_or(1);
    let applied_at = Utc::now().to_rfc3339();
    schema.schema_hash = requested_hash;
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
    let status = mvcc
        .open_transactions
        .status(&handle.transaction_id, &principal, now_unix_ms)?;
    if status.state == "open" {
        mvcc.stage_product_mutations(
            &handle.transaction_id,
            &principal,
            vec![crate::mvcc_product::ProductMutation::put(
                key.clone(),
                encode_namespace_schema_record(&record)?,
            )],
            now_unix_ms,
        )?;
        let predicate = previous_payload
            .as_ref()
            .map(|payload| {
                crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(payload).as_bytes())
            })
            .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent);
        mvcc.stage_predicate(
            &handle.transaction_id,
            &principal,
            key.clone(),
            predicate,
            now_unix_ms,
        )?;
        if let Some(assignment) = &assignment {
            mvcc.stage_assignment_guard(
                &handle.transaction_id,
                &principal,
                assignment,
                now_unix_ms,
            )?;
        }
    }
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            &principal,
            current_unix_ms(),
        )
        .await?;
    let commit_version = match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { commit_version } => {
            commit_version
        }
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
            bail!("authorization namespace schema transaction aborted: {reason:?}")
        }
    };
    let payload = mvcc
        .runtime
        .read_at(&key, commit_version)?
        .ok_or_else(|| anyhow!("committed authorization namespace schema is not readable"))?;
    let committed = decode_namespace_schema_record(&payload.value)?;
    validate_record(&committed, tenant_id, &committed.namespace)?;
    Ok(committed)
}

pub async fn read_authz_namespace_schema(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    namespace: &str,
) -> Result<Option<AuthzNamespaceSchemaRecord>> {
    let snapshot = mvcc.runtime.applied_version()?;
    let key = namespace_schema_logical_key(tenant_id, namespace)?;
    let Some(payload) = mvcc.runtime.read_at(&key, snapshot)?.map(|row| row.value) else {
        return Ok(None);
    };
    let record = decode_namespace_schema_record(&payload)?;
    validate_record(&record, tenant_id, namespace)?;
    Ok(Some(record))
}

pub async fn page_authz_namespace_schemas(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
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
    let snapshot = mvcc.runtime.applied_version()?;
    let application_prefix = crate::mvcc_product::coremeta_application_prefix(CF_AUTHZ, &prefix)?;
    let mut rows = mvcc
        .runtime
        .scan_table_prefix_at(TABLE_AUTHZ_SCHEMA_ROW, &application_prefix, snapshot)?
        .into_iter()
        .map(|(key, row)| {
            Ok((
                crate::mvcc_product::coremeta_tuple_from_logical_key(&key, CF_AUTHZ)?.to_vec(),
                row.value,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    rows.retain(|(tuple_key, _)| after_tuple_key.is_none_or(|after| tuple_key.as_slice() > after));
    rows.truncate(page_size.saturating_add(1));
    let has_more = rows.len() > page_size;
    if has_more {
        rows.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            rows.last()
                .ok_or_else(|| anyhow!("authorization namespace schema page is empty"))?
                .0
                .clone(),
        )
    } else {
        None
    };
    let mut records = Vec::with_capacity(rows.len());
    for (tuple_key, payload) in rows {
        let record = decode_namespace_schema_record(&payload)?;
        validate_record(&record, tenant_id, &record.namespace)?;
        if tuple_key != namespace_schema_tuple_key(tenant_id, &record.namespace)? {
            return Err(anyhow!(
                "authorization namespace schema MVCC row scope mismatch"
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

fn namespace_schema_logical_key(
    tenant_id: i64,
    namespace: &str,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &namespace_schema_tuple_key(tenant_id, namespace)?,
    )
}

fn current_unix_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
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
