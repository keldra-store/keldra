use crate::{
    core_store::{
        CF_MESH, CoreMetaRowCommonProto, CoreMetaTuplePart, TABLE_REPAIR_FINDING_HEAD_ROW,
        TABLE_REPAIR_FINDING_ID_ROW, TABLE_REPAIR_FINDING_ROW, core_meta_committed_row_common,
        core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::hash32,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex as StdMutex, Weak},
};
use tokio::sync::Mutex;

type HmacSha256 = Hmac<Sha256>;
const REPAIR_FINDING_HEAD_SCHEMA: &str = "anvil.repair.finding_head.v1";
const REPAIR_FINDING_ID_SCHEMA: &str = "anvil.repair.finding_id.v1";
const REPAIR_FINDING_PAGE_MAX: usize = 1000;

static REPAIR_FINDING_WRITE_LOCKS: LazyLock<StdMutex<HashMap<String, Weak<Mutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairFindingSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairFindingStatus {
    Open,
    RebuiltDerivedIndex,
    RepairedManifest,
    RequiresOperatorReview,
    Irreparable,
    RepairedObjectShards,
    VerifiedHealthy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairActionKind {
    VerifyOnly,
    RebuildDerivedIndex,
    RebuildDirectoryIndex,
    RepairManifestFromSegments,
    SynthesizeCommittedObjectVersion,
    SynthesizePersonalDbCommit,
    RepairObjectShards,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairSubjectRef {
    pub subject_kind: String,
    pub subject_id: String,
    pub generation: Option<u64>,
    pub cursor: Option<u128>,
    pub expected_hash: Option<String>,
    pub actual_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairFinding {
    pub format_version: u16,
    pub finding_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub repair_task_id: String,
    pub lease_fence_token: u64,
    pub severity: RepairFindingSeverity,
    pub status: RepairFindingStatus,
    pub code: String,
    pub message: String,
    pub subjects: Vec<RepairSubjectRef>,
    pub proposed_action: RepairActionKind,
    pub evidence: serde_json::Value,
    pub created_at_nanos: i64,
    pub scope_revision: u64,
    pub finding_hash: Option<String>,
    pub finding_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairFindingWrite {
    pub finding_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub repair_task_id: String,
    pub lease_fence_token: u64,
    pub severity: RepairFindingSeverity,
    pub status: RepairFindingStatus,
    pub code: String,
    pub message: String,
    pub subjects: Vec<RepairSubjectRef>,
    pub proposed_action: RepairActionKind,
    pub evidence: serde_json::Value,
    pub created_at_nanos: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum RepairFindingSeverityProto {
    Unspecified = 0,
    Info = 1,
    Warning = 2,
    Error = 3,
    Critical = 4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum RepairFindingStatusProto {
    Unspecified = 0,
    Open = 1,
    RebuiltDerivedIndex = 2,
    RepairedManifest = 3,
    RequiresOperatorReview = 4,
    Irreparable = 5,
    RepairedObjectShards = 6,
    VerifiedHealthy = 7,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum RepairActionKindProto {
    Unspecified = 0,
    VerifyOnly = 1,
    RebuildDerivedIndex = 2,
    RebuildDirectoryIndex = 3,
    RepairManifestFromSegments = 4,
    SynthesizeCommittedObjectVersion = 5,
    SynthesizePersonalDbCommit = 6,
    RepairObjectShards = 7,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum RepairJsonKindProto {
    Unspecified = 0,
    Null = 1,
    Bool = 2,
    Number = 3,
    String = 4,
    Array = 5,
    Object = 6,
}

#[derive(Clone, PartialEq, Message)]
struct RepairJsonValueProto {
    #[prost(enumeration = "RepairJsonKindProto", tag = "1")]
    kind: i32,
    #[prost(bool, tag = "2")]
    bool_value: bool,
    #[prost(string, tag = "3")]
    number_value: String,
    #[prost(string, tag = "4")]
    string_value: String,
    #[prost(message, repeated, tag = "5")]
    array_values: Vec<RepairJsonValueProto>,
    #[prost(string, repeated, tag = "6")]
    object_keys: Vec<String>,
    #[prost(message, repeated, tag = "7")]
    object_values: Vec<RepairJsonValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct RepairFindingRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    body: Option<RepairFindingBodyProto>,
}

#[derive(Clone, PartialEq, Message)]
struct RepairFindingBodyProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    finding_id: String,
    #[prost(string, tag = "3")]
    scope_kind: String,
    #[prost(string, tag = "4")]
    scope_id: String,
    #[prost(string, tag = "5")]
    repair_task_id: String,
    #[prost(uint64, tag = "6")]
    lease_fence_token: u64,
    #[prost(enumeration = "RepairFindingSeverityProto", tag = "7")]
    severity: i32,
    #[prost(enumeration = "RepairFindingStatusProto", tag = "8")]
    status: i32,
    #[prost(string, tag = "9")]
    code: String,
    #[prost(string, tag = "10")]
    message: String,
    #[prost(message, repeated, tag = "11")]
    subjects: Vec<RepairSubjectRefProto>,
    #[prost(enumeration = "RepairActionKindProto", tag = "12")]
    proposed_action: i32,
    #[prost(message, optional, tag = "13")]
    evidence: Option<RepairJsonValueProto>,
    #[prost(int64, tag = "14")]
    created_at_nanos: i64,
    #[prost(string, optional, tag = "15")]
    finding_hash: Option<String>,
    #[prost(string, optional, tag = "16")]
    finding_signature: Option<String>,
    #[prost(uint64, tag = "17")]
    scope_revision: u64,
}

#[derive(Clone, PartialEq, Message)]
struct RepairFindingHeadProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    scope_kind: String,
    #[prost(string, tag = "4")]
    scope_id: String,
    #[prost(uint64, tag = "5")]
    revision: u64,
    #[prost(uint64, tag = "6")]
    finding_count: u64,
    #[prost(string, tag = "7")]
    last_finding_id: String,
    #[prost(string, tag = "8")]
    last_finding_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct RepairFindingIdProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    scope_kind: String,
    #[prost(string, tag = "4")]
    scope_id: String,
    #[prost(string, tag = "5")]
    finding_id: String,
    #[prost(uint64, tag = "6")]
    revision: u64,
}

#[derive(Clone, PartialEq, Message)]
struct RepairSubjectRefProto {
    #[prost(string, tag = "1")]
    subject_kind: String,
    #[prost(string, tag = "2")]
    subject_id: String,
    #[prost(uint64, optional, tag = "3")]
    generation: Option<u64>,
    #[prost(string, optional, tag = "4")]
    cursor: Option<String>,
    #[prost(string, optional, tag = "5")]
    expected_hash: Option<String>,
    #[prost(string, optional, tag = "6")]
    actual_hash: Option<String>,
}

impl RepairFinding {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_finding(&self)?;
        let hash = hash_repair_finding(&self)?;
        let signature = sign_finding_hash(
            signing_key,
            &hash,
            &[
                &self.scope_kind,
                &self.scope_id,
                &self.repair_task_id,
                &self.finding_id,
            ],
        )?;
        self.finding_hash = Some(hash);
        self.finding_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_finding(self)?;
        let expected_hash = hash_repair_finding(self)?;
        if self.finding_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("repair finding hash mismatch"));
        }
        let expected_signature = sign_finding_hash(
            signing_key,
            &expected_hash,
            &[
                &self.scope_kind,
                &self.scope_id,
                &self.repair_task_id,
                &self.finding_id,
            ],
        )?;
        if self.finding_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("repair finding signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_repair_finding(finding: &RepairFinding) -> Result<String> {
    let mut unsigned = finding.clone();
    unsigned.finding_hash = None;
    unsigned.finding_signature = None;
    Ok(hex::encode(hash32(&encode_repair_finding_body(&unsigned)?)))
}

pub async fn write_repair_finding(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    finding: RepairFindingWrite,
    signing_key: &[u8],
) -> Result<RepairFinding> {
    write_repair_finding_inner(mvcc, finding, signing_key, Vec::new()).await
}

pub async fn write_repair_finding_with_lease(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    finding: RepairFindingWrite,
    signing_key: &[u8],
    lease_precondition: (
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    ),
) -> Result<RepairFinding> {
    write_repair_finding_inner(mvcc, finding, signing_key, vec![lease_precondition]).await
}

async fn write_repair_finding_inner(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    finding: RepairFindingWrite,
    signing_key: &[u8],
    publication_preconditions: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
) -> Result<RepairFinding> {
    let repair_started_at = std::time::Instant::now();
    validate_write(&finding)?;
    let metric_scope_kind = finding.scope_kind.clone();
    let metric_status = repair_finding_status_name(finding.status);
    let metric_severity = repair_finding_severity_name(finding.severity);
    let write_lock = repair_finding_write_lock(&finding.scope_kind, &finding.scope_id);
    let _guard = write_lock.lock().await;
    if let Some(existing) = read_repair_finding(
        mvcc,
        &finding.scope_kind,
        &finding.scope_id,
        &finding.finding_id,
        signing_key,
    )
    .await?
    {
        if finding_matches_write(&existing, &finding) {
            return Ok(existing);
        }
        return Err(anyhow!(
            "repair finding id already names different immutable content"
        ));
    }
    let head_key = repair_finding_mvcc_key(
        TABLE_REPAIR_FINDING_HEAD_ROW,
        repair_finding_head_tuple_key(&finding.scope_kind, &finding.scope_id)?,
    )?;
    let current_head_payload = mvcc.read_latest_value(&head_key)?;
    let current_head = current_head_payload
        .as_deref()
        .map(|payload| decode_repair_finding_head(payload, &finding.scope_kind, &finding.scope_id))
        .transpose()?;
    let scope_revision = current_head
        .as_ref()
        .map(|head| head.revision)
        .unwrap_or_default()
        .checked_add(1)
        .ok_or_else(|| anyhow!("repair finding scope revision overflow"))?;
    let sealed = RepairFinding {
        format_version: 1,
        finding_id: finding.finding_id,
        scope_kind: finding.scope_kind,
        scope_id: finding.scope_id,
        repair_task_id: finding.repair_task_id,
        lease_fence_token: finding.lease_fence_token,
        severity: finding.severity,
        status: finding.status,
        code: finding.code,
        message: finding.message,
        subjects: finding.subjects,
        proposed_action: finding.proposed_action,
        evidence: finding.evidence,
        created_at_nanos: finding.created_at_nanos,
        scope_revision,
        finding_hash: None,
        finding_signature: None,
    }
    .seal(signing_key)?;
    write_repair_finding_records_mvcc(
        mvcc,
        &sealed,
        current_head.as_ref(),
        current_head_payload.as_deref(),
        publication_preconditions,
    )
    .await?;
    crate::perf::record_repair_duration(
        sealed.code.as_str(),
        sealed.scope_kind.as_str(),
        metric_status,
        repair_started_at.elapsed(),
    );
    crate::perf::record_anti_entropy_findings_total(
        sealed.code.as_str(),
        metric_scope_kind.as_str(),
        metric_severity,
        1,
    );
    Ok(sealed)
}

pub async fn read_repair_finding(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    scope_kind: &str,
    scope_id: &str,
    finding_id: &str,
    signing_key: &[u8],
) -> Result<Option<RepairFinding>> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    require_safe_component(finding_id, "finding_id")?;
    let id_key = repair_finding_mvcc_key(
        TABLE_REPAIR_FINDING_ID_ROW,
        repair_finding_id_tuple_key(scope_kind, scope_id, finding_id)?,
    )?;
    let snapshot = mvcc.runtime.applied_version()?;
    let Some(id_bytes) = mvcc
        .runtime
        .read_at(&id_key, snapshot)?
        .map(|row| row.value)
    else {
        return Ok(None);
    };
    let id_row = decode_repair_finding_id(&id_bytes, scope_kind, scope_id, finding_id)?;
    let tuple_key = repair_finding_mvcc_key(
        TABLE_REPAIR_FINDING_ROW,
        repair_finding_tuple_key(scope_kind, scope_id, id_row.revision)?,
    )?;
    let bytes = mvcc
        .runtime
        .read_at(&tuple_key, snapshot)?
        .map(|row| row.value)
        .ok_or_else(|| anyhow!("repair finding id row points to a missing revision"))?;
    let finding = decode_repair_finding(&bytes)?;
    finding.verify(signing_key)?;
    if finding.scope_kind != scope_kind
        || finding.scope_id != scope_id
        || finding.finding_id != finding_id
    {
        return Err(anyhow!("repair finding ref scope mismatch"));
    }
    Ok(Some(finding))
}

pub async fn repair_finding_scope_revision(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    scope_kind: &str,
    scope_id: &str,
) -> Result<u64> {
    Ok(read_repair_finding_head_mvcc(mvcc, scope_kind, scope_id)?
        .map(|head| head.revision)
        .unwrap_or_default())
}

pub async fn page_repair_findings(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    scope_kind: &str,
    scope_id: &str,
    after_revision: u64,
    through_revision: u64,
    limit: usize,
    signing_key: &[u8],
) -> Result<Vec<RepairFinding>> {
    if !(1..=REPAIR_FINDING_PAGE_MAX + 1).contains(&limit) {
        return Err(anyhow!(
            "repair finding page limit must be between 1 and {}",
            REPAIR_FINDING_PAGE_MAX + 1
        ));
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let head_before = read_repair_finding_head_at(mvcc, scope_kind, scope_id, snapshot)?
        .map(|(_, head)| head.revision)
        .unwrap_or_default();
    if head_before != through_revision {
        return Err(anyhow!("repair finding collection revision changed"));
    }
    if after_revision >= through_revision || through_revision == 0 {
        return Ok(Vec::new());
    }

    let start_revision = after_revision + 1;
    let prefix = crate::mvcc_product::coremeta_application_prefix(
        CF_MESH,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8(scope_kind),
            CoreMetaTuplePart::Utf8(scope_id),
        ])?,
    )?;
    let mut findings = mvcc
        .runtime
        .scan_table_prefix_at(TABLE_REPAIR_FINDING_ROW, &prefix, snapshot)?
        .into_iter()
        .map(|(_, row)| decode_repair_finding(&row.value))
        .collect::<Result<Vec<_>>>()?;
    findings.retain(|finding| {
        finding.scope_revision >= start_revision && finding.scope_revision <= through_revision
    });
    findings.sort_by_key(|finding| finding.scope_revision);
    findings.truncate(limit);
    for finding in &findings {
        finding.verify(signing_key)?;
        if finding.scope_kind != scope_kind
            || finding.scope_id != scope_id
            || finding.scope_revision <= after_revision
            || finding.scope_revision > through_revision
        {
            return Err(anyhow!("repair finding page scope mismatch"));
        }
    }
    Ok(findings)
}

pub fn validate_repair_action(action: RepairActionKind) -> Result<()> {
    match action {
        RepairActionKind::SynthesizeCommittedObjectVersion
        | RepairActionKind::SynthesizePersonalDbCommit => Err(anyhow!(
            "repair action cannot synthesize committed object versions or PersonalDB commits"
        )),
        RepairActionKind::VerifyOnly
        | RepairActionKind::RebuildDerivedIndex
        | RepairActionKind::RebuildDirectoryIndex
        | RepairActionKind::RepairManifestFromSegments
        | RepairActionKind::RepairObjectShards => Ok(()),
    }
}

fn validate_write(finding: &RepairFindingWrite) -> Result<()> {
    let unsigned = RepairFinding {
        format_version: 1,
        finding_id: finding.finding_id.clone(),
        scope_kind: finding.scope_kind.clone(),
        scope_id: finding.scope_id.clone(),
        repair_task_id: finding.repair_task_id.clone(),
        lease_fence_token: finding.lease_fence_token,
        severity: finding.severity,
        status: finding.status,
        code: finding.code.clone(),
        message: finding.message.clone(),
        subjects: finding.subjects.clone(),
        proposed_action: finding.proposed_action,
        evidence: finding.evidence.clone(),
        created_at_nanos: finding.created_at_nanos,
        scope_revision: 1,
        finding_hash: None,
        finding_signature: None,
    };
    validate_unsigned_finding(&unsigned)
}

fn validate_unsigned_finding(finding: &RepairFinding) -> Result<()> {
    if finding.format_version != 1 {
        return Err(anyhow!("unsupported repair finding version"));
    }
    require_safe_component(&finding.finding_id, "finding_id")?;
    require_safe_component(&finding.scope_kind, "scope_kind")?;
    require_safe_component(&finding.scope_id, "scope_id")?;
    require_safe_component(&finding.repair_task_id, "repair_task_id")?;
    if finding.lease_fence_token == 0 {
        return Err(anyhow!("repair finding lease fence token must be nonzero"));
    }
    require_nonempty(&finding.code, "code")?;
    require_nonempty(&finding.message, "message")?;
    if finding.subjects.is_empty() {
        return Err(anyhow!("repair finding must include at least one subject"));
    }
    for subject in &finding.subjects {
        validate_subject(subject)?;
    }
    validate_repair_action(finding.proposed_action)?;
    if finding.created_at_nanos < 0 {
        return Err(anyhow!("repair finding timestamp must be nonnegative"));
    }
    if finding.scope_revision == 0 {
        return Err(anyhow!("repair finding scope revision must be nonzero"));
    }
    Ok(())
}

fn validate_subject(subject: &RepairSubjectRef) -> Result<()> {
    require_safe_component(&subject.subject_kind, "subject_kind")?;
    require_nonempty(&subject.subject_id, "subject_id")?;
    if let Some(expected_hash) = subject.expected_hash.as_ref() {
        validate_hex32(expected_hash, "expected_hash")?;
    }
    if let Some(actual_hash) = subject.actual_hash.as_ref() {
        validate_hex32(actual_hash, "actual_hash")?;
    }
    Ok(())
}

fn repair_finding_severity_name(severity: RepairFindingSeverity) -> &'static str {
    match severity {
        RepairFindingSeverity::Info => "info",
        RepairFindingSeverity::Warning => "warning",
        RepairFindingSeverity::Error => "error",
        RepairFindingSeverity::Critical => "critical",
    }
}

fn repair_finding_status_name(status: RepairFindingStatus) -> &'static str {
    match status {
        RepairFindingStatus::Open => "open",
        RepairFindingStatus::RebuiltDerivedIndex => "rebuilt_derived_index",
        RepairFindingStatus::RepairedManifest => "repaired_manifest",
        RepairFindingStatus::RequiresOperatorReview => "requires_operator_review",
        RepairFindingStatus::Irreparable => "irreparable",
        RepairFindingStatus::RepairedObjectShards => "repaired_object_shards",
        RepairFindingStatus::VerifiedHealthy => "verified_healthy",
    }
}

fn sign_finding_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("repair finding signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"repair_finding");
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{field} is not a safe path component"));
    }
    Ok(())
}

async fn write_repair_finding_records_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    finding: &RepairFinding,
    current_head: Option<&RepairFindingHeadProto>,
    current_head_payload: Option<&[u8]>,
    mut preconditions: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
) -> Result<()> {
    let finding_key = repair_finding_tuple_key(
        &finding.scope_kind,
        &finding.scope_id,
        finding.scope_revision,
    )?;
    let id_key =
        repair_finding_id_tuple_key(&finding.scope_kind, &finding.scope_id, &finding.finding_id)?;
    let head_key = repair_finding_head_tuple_key(&finding.scope_kind, &finding.scope_id)?;
    let finding_payload = encode_repair_finding(finding)?;
    let common = repair_finding_common(finding)?;
    let id_payload = encode_deterministic_proto(&RepairFindingIdProto {
        common: Some(common.clone()),
        schema: REPAIR_FINDING_ID_SCHEMA.to_string(),
        scope_kind: finding.scope_kind.clone(),
        scope_id: finding.scope_id.clone(),
        finding_id: finding.finding_id.clone(),
        revision: finding.scope_revision,
    });
    let head_payload = encode_deterministic_proto(&RepairFindingHeadProto {
        common: Some(common),
        schema: REPAIR_FINDING_HEAD_SCHEMA.to_string(),
        scope_kind: finding.scope_kind.clone(),
        scope_id: finding.scope_id.clone(),
        revision: finding.scope_revision,
        finding_count: current_head
            .map(|head| head.finding_count)
            .unwrap_or_default()
            .checked_add(1)
            .ok_or_else(|| anyhow!("repair finding count overflow"))?,
        last_finding_id: finding.finding_id.clone(),
        last_finding_hash: finding
            .finding_hash
            .clone()
            .ok_or_else(|| anyhow!("sealed repair finding is missing hash"))?,
    });
    let finding_key = repair_finding_mvcc_key(TABLE_REPAIR_FINDING_ROW, finding_key)?;
    let id_key = repair_finding_mvcc_key(TABLE_REPAIR_FINDING_ID_ROW, id_key)?;
    let head_key = repair_finding_mvcc_key(TABLE_REPAIR_FINDING_HEAD_ROW, head_key)?;
    preconditions.extend([
        (
            finding_key.clone(),
            crate::mvcc_transaction::PredicateKind::Absent,
        ),
        (
            id_key.clone(),
            crate::mvcc_transaction::PredicateKind::Absent,
        ),
        (
            head_key.clone(),
            match current_head_payload {
                Some(payload) => crate::mvcc_transaction::PredicateKind::ValueHash(
                    *blake3::hash(payload).as_bytes(),
                ),
                None => crate::mvcc_transaction::PredicateKind::Absent,
            },
        ),
    ]);
    mvcc.autocommit_product_mutations_with_predicates(
        "repair-finding",
        &format!(
            "repair-finding:{}:{}:{}",
            finding.scope_kind, finding.scope_id, finding.finding_id
        ),
        vec![
            crate::mvcc_product::ProductMutation::put(finding_key, finding_payload),
            crate::mvcc_product::ProductMutation::put(id_key, id_payload),
            crate::mvcc_product::ProductMutation::put(head_key, head_payload),
        ],
        preconditions,
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| anyhow!("repair finding timestamp predates Unix epoch"))?,
    )
    .await?;
    Ok(())
}

fn read_repair_finding_head_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    scope_kind: &str,
    scope_id: &str,
    snapshot: u64,
) -> Result<Option<(Vec<u8>, RepairFindingHeadProto)>> {
    let key = repair_finding_mvcc_key(
        TABLE_REPAIR_FINDING_HEAD_ROW,
        repair_finding_head_tuple_key(scope_kind, scope_id)?,
    )?;
    let Some(row) = mvcc.runtime.read_at(&key, snapshot)? else {
        return Ok(None);
    };
    let head = decode_repair_finding_head(&row.value, scope_kind, scope_id)?;
    Ok(Some((row.value, head)))
}

fn read_repair_finding_head_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    scope_kind: &str,
    scope_id: &str,
) -> Result<Option<RepairFindingHeadProto>> {
    Ok(
        read_repair_finding_head_at(mvcc, scope_kind, scope_id, mvcc.runtime.applied_version()?)?
            .map(|(_, head)| head),
    )
}

fn decode_repair_finding_head(
    bytes: &[u8],
    scope_kind: &str,
    scope_id: &str,
) -> Result<RepairFindingHeadProto> {
    let head = decode_deterministic_proto::<RepairFindingHeadProto>(bytes, "repair finding head")?;
    validate_repair_finding_head(&head, scope_kind, scope_id)?;
    Ok(head)
}

fn decode_repair_finding_id(
    bytes: &[u8],
    scope_kind: &str,
    scope_id: &str,
    finding_id: &str,
) -> Result<RepairFindingIdProto> {
    let row = decode_deterministic_proto::<RepairFindingIdProto>(bytes, "repair finding id")?;
    if row.schema != REPAIR_FINDING_ID_SCHEMA
        || row.scope_kind != scope_kind
        || row.scope_id != scope_id
        || row.finding_id != finding_id
        || row.revision == 0
    {
        return Err(anyhow!("repair finding id row scope mismatch"));
    }
    validate_repair_common(
        row.common
            .as_ref()
            .ok_or_else(|| anyhow!("repair finding id row missing CoreMeta common fields"))?,
        scope_kind,
        scope_id,
    )?;
    Ok(row)
}

fn validate_repair_finding_head(
    head: &RepairFindingHeadProto,
    scope_kind: &str,
    scope_id: &str,
) -> Result<()> {
    if head.schema != REPAIR_FINDING_HEAD_SCHEMA
        || head.scope_kind != scope_kind
        || head.scope_id != scope_id
        || head.revision == 0
        || head.finding_count == 0
        || head.finding_count != head.revision
    {
        return Err(anyhow!("repair finding head row is invalid"));
    }
    require_safe_component(&head.last_finding_id, "last_finding_id")?;
    validate_hex32(&head.last_finding_hash, "last_finding_hash")?;
    validate_repair_common(
        head.common
            .as_ref()
            .ok_or_else(|| anyhow!("repair finding head missing CoreMeta common fields"))?,
        scope_kind,
        scope_id,
    )?;
    Ok(())
}

fn encode_repair_finding(finding: &RepairFinding) -> Result<Vec<u8>> {
    encode_repair_finding_with_common(finding, repair_finding_common(finding)?)
}

fn encode_repair_finding_with_common(
    finding: &RepairFinding,
    common: CoreMetaRowCommonProto,
) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&RepairFindingRowProto {
        common: Some(common),
        body: Some(repair_finding_to_proto(finding)),
    }))
}

fn encode_repair_finding_body(finding: &RepairFinding) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&repair_finding_to_proto(
        finding,
    )))
}

fn decode_repair_finding(bytes: &[u8]) -> Result<RepairFinding> {
    let row = decode_deterministic_proto::<RepairFindingRowProto>(bytes, "repair finding")?;
    let common = row
        .common
        .ok_or_else(|| anyhow!("repair finding missing CoreMeta common row fields"))?;
    let finding = repair_finding_from_proto(
        row.body
            .ok_or_else(|| anyhow!("repair finding missing domain body"))?,
    )?;
    validate_repair_finding_common(&finding, &common)?;
    Ok(finding)
}

fn repair_finding_to_proto(finding: &RepairFinding) -> RepairFindingBodyProto {
    RepairFindingBodyProto {
        format_version: u32::from(finding.format_version),
        finding_id: finding.finding_id.clone(),
        scope_kind: finding.scope_kind.clone(),
        scope_id: finding.scope_id.clone(),
        repair_task_id: finding.repair_task_id.clone(),
        lease_fence_token: finding.lease_fence_token,
        severity: severity_to_proto(finding.severity) as i32,
        status: status_to_proto(finding.status) as i32,
        code: finding.code.clone(),
        message: finding.message.clone(),
        subjects: finding.subjects.iter().map(subject_to_proto).collect(),
        proposed_action: action_to_proto(finding.proposed_action) as i32,
        evidence: Some(json_value_to_proto(&finding.evidence)),
        created_at_nanos: finding.created_at_nanos,
        finding_hash: finding.finding_hash.clone(),
        finding_signature: finding.finding_signature.clone(),
        scope_revision: finding.scope_revision,
    }
}

fn repair_finding_from_proto(proto: RepairFindingBodyProto) -> Result<RepairFinding> {
    Ok(RepairFinding {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("repair finding version exceeds u16"))?,
        finding_id: proto.finding_id,
        scope_kind: proto.scope_kind,
        scope_id: proto.scope_id,
        repair_task_id: proto.repair_task_id,
        lease_fence_token: proto.lease_fence_token,
        severity: severity_from_proto(proto.severity)?,
        status: status_from_proto(proto.status)?,
        code: proto.code,
        message: proto.message,
        subjects: proto
            .subjects
            .into_iter()
            .map(subject_from_proto)
            .collect::<Result<Vec<_>>>()?,
        proposed_action: action_from_proto(proto.proposed_action)?,
        evidence: json_value_from_proto(
            proto
                .evidence
                .ok_or_else(|| anyhow!("repair finding missing evidence"))?,
        )?,
        created_at_nanos: proto.created_at_nanos,
        scope_revision: proto.scope_revision,
        finding_hash: proto.finding_hash,
        finding_signature: proto.finding_signature,
    })
}

fn repair_finding_common(finding: &RepairFinding) -> Result<CoreMetaRowCommonProto> {
    let created_at_unix_nanos = u64::try_from(finding.created_at_nanos)
        .map_err(|_| anyhow!("repair finding timestamp must be nonnegative"))?;
    Ok(core_meta_committed_row_common(
        format!("repair/{}/{}", finding.scope_kind, finding.scope_id),
        repair_finding_root_key_hash(&finding.scope_kind, &finding.scope_id),
        1,
        format!("{}/{}", finding.repair_task_id, finding.finding_id),
        created_at_unix_nanos,
    ))
}

fn validate_repair_finding_common(
    finding: &RepairFinding,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    validate_repair_common(common, &finding.scope_kind, &finding.scope_id)
}

fn validate_repair_common(
    common: &CoreMetaRowCommonProto,
    scope_kind: &str,
    scope_id: &str,
) -> Result<()> {
    if common.realm_id != format!("repair/{scope_kind}/{scope_id}") {
        return Err(anyhow!("repair finding CoreMeta realm mismatch"));
    }
    if common.root_key_hash != repair_finding_root_key_hash(scope_kind, scope_id) {
        return Err(anyhow!("repair finding CoreMeta root mismatch"));
    }
    if common.root_generation == 0 {
        return Err(anyhow!(
            "repair finding CoreMeta root generation must be nonzero"
        ));
    }
    if common.visibility_state_enum() != crate::core_store::CoreMetaVisibilityState::Committed {
        return Err(anyhow!("repair finding CoreMeta row is not committed"));
    }
    Ok(())
}

fn repair_finding_root_key_hash(scope_kind: &str, scope_id: &str) -> String {
    core_meta_root_key_hash(&format!("repair/{scope_kind}/{scope_id}"))
}

fn subject_to_proto(subject: &RepairSubjectRef) -> RepairSubjectRefProto {
    RepairSubjectRefProto {
        subject_kind: subject.subject_kind.clone(),
        subject_id: subject.subject_id.clone(),
        generation: subject.generation,
        cursor: subject.cursor.map(|value| value.to_string()),
        expected_hash: subject.expected_hash.clone(),
        actual_hash: subject.actual_hash.clone(),
    }
}

fn subject_from_proto(proto: RepairSubjectRefProto) -> Result<RepairSubjectRef> {
    Ok(RepairSubjectRef {
        subject_kind: proto.subject_kind,
        subject_id: proto.subject_id,
        generation: proto.generation,
        cursor: proto
            .cursor
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| anyhow!("repair finding subject cursor is not u128"))
            })
            .transpose()?,
        expected_hash: proto.expected_hash,
        actual_hash: proto.actual_hash,
    })
}

fn severity_to_proto(severity: RepairFindingSeverity) -> RepairFindingSeverityProto {
    match severity {
        RepairFindingSeverity::Info => RepairFindingSeverityProto::Info,
        RepairFindingSeverity::Warning => RepairFindingSeverityProto::Warning,
        RepairFindingSeverity::Error => RepairFindingSeverityProto::Error,
        RepairFindingSeverity::Critical => RepairFindingSeverityProto::Critical,
    }
}

fn severity_from_proto(severity: i32) -> Result<RepairFindingSeverity> {
    match RepairFindingSeverityProto::try_from(severity)
        .map_err(|_| anyhow!("repair finding severity is invalid"))?
    {
        RepairFindingSeverityProto::Info => Ok(RepairFindingSeverity::Info),
        RepairFindingSeverityProto::Warning => Ok(RepairFindingSeverity::Warning),
        RepairFindingSeverityProto::Error => Ok(RepairFindingSeverity::Error),
        RepairFindingSeverityProto::Critical => Ok(RepairFindingSeverity::Critical),
        RepairFindingSeverityProto::Unspecified => {
            Err(anyhow!("repair finding severity is unspecified"))
        }
    }
}

fn status_to_proto(status: RepairFindingStatus) -> RepairFindingStatusProto {
    match status {
        RepairFindingStatus::Open => RepairFindingStatusProto::Open,
        RepairFindingStatus::RebuiltDerivedIndex => RepairFindingStatusProto::RebuiltDerivedIndex,
        RepairFindingStatus::RepairedManifest => RepairFindingStatusProto::RepairedManifest,
        RepairFindingStatus::RequiresOperatorReview => {
            RepairFindingStatusProto::RequiresOperatorReview
        }
        RepairFindingStatus::Irreparable => RepairFindingStatusProto::Irreparable,
        RepairFindingStatus::RepairedObjectShards => RepairFindingStatusProto::RepairedObjectShards,
        RepairFindingStatus::VerifiedHealthy => RepairFindingStatusProto::VerifiedHealthy,
    }
}

fn status_from_proto(status: i32) -> Result<RepairFindingStatus> {
    match RepairFindingStatusProto::try_from(status)
        .map_err(|_| anyhow!("repair finding status is invalid"))?
    {
        RepairFindingStatusProto::Open => Ok(RepairFindingStatus::Open),
        RepairFindingStatusProto::RebuiltDerivedIndex => {
            Ok(RepairFindingStatus::RebuiltDerivedIndex)
        }
        RepairFindingStatusProto::RepairedManifest => Ok(RepairFindingStatus::RepairedManifest),
        RepairFindingStatusProto::RequiresOperatorReview => {
            Ok(RepairFindingStatus::RequiresOperatorReview)
        }
        RepairFindingStatusProto::Irreparable => Ok(RepairFindingStatus::Irreparable),
        RepairFindingStatusProto::RepairedObjectShards => {
            Ok(RepairFindingStatus::RepairedObjectShards)
        }
        RepairFindingStatusProto::VerifiedHealthy => Ok(RepairFindingStatus::VerifiedHealthy),
        RepairFindingStatusProto::Unspecified => {
            Err(anyhow!("repair finding status is unspecified"))
        }
    }
}

fn action_to_proto(action: RepairActionKind) -> RepairActionKindProto {
    match action {
        RepairActionKind::VerifyOnly => RepairActionKindProto::VerifyOnly,
        RepairActionKind::RebuildDerivedIndex => RepairActionKindProto::RebuildDerivedIndex,
        RepairActionKind::RebuildDirectoryIndex => RepairActionKindProto::RebuildDirectoryIndex,
        RepairActionKind::RepairManifestFromSegments => {
            RepairActionKindProto::RepairManifestFromSegments
        }
        RepairActionKind::SynthesizeCommittedObjectVersion => {
            RepairActionKindProto::SynthesizeCommittedObjectVersion
        }
        RepairActionKind::SynthesizePersonalDbCommit => {
            RepairActionKindProto::SynthesizePersonalDbCommit
        }
        RepairActionKind::RepairObjectShards => RepairActionKindProto::RepairObjectShards,
    }
}

fn action_from_proto(action: i32) -> Result<RepairActionKind> {
    match RepairActionKindProto::try_from(action)
        .map_err(|_| anyhow!("repair action kind is invalid"))?
    {
        RepairActionKindProto::VerifyOnly => Ok(RepairActionKind::VerifyOnly),
        RepairActionKindProto::RebuildDerivedIndex => Ok(RepairActionKind::RebuildDerivedIndex),
        RepairActionKindProto::RebuildDirectoryIndex => Ok(RepairActionKind::RebuildDirectoryIndex),
        RepairActionKindProto::RepairManifestFromSegments => {
            Ok(RepairActionKind::RepairManifestFromSegments)
        }
        RepairActionKindProto::SynthesizeCommittedObjectVersion => {
            Ok(RepairActionKind::SynthesizeCommittedObjectVersion)
        }
        RepairActionKindProto::SynthesizePersonalDbCommit => {
            Ok(RepairActionKind::SynthesizePersonalDbCommit)
        }
        RepairActionKindProto::RepairObjectShards => Ok(RepairActionKind::RepairObjectShards),
        RepairActionKindProto::Unspecified => Err(anyhow!("repair action kind is unspecified")),
    }
}

fn json_value_to_proto(value: &serde_json::Value) -> RepairJsonValueProto {
    match value {
        serde_json::Value::Null => RepairJsonValueProto {
            kind: RepairJsonKindProto::Null as i32,
            bool_value: false,
            number_value: String::new(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Bool(value) => RepairJsonValueProto {
            kind: RepairJsonKindProto::Bool as i32,
            bool_value: *value,
            number_value: String::new(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Number(value) => RepairJsonValueProto {
            kind: RepairJsonKindProto::Number as i32,
            bool_value: false,
            number_value: value.to_string(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::String(value) => RepairJsonValueProto {
            kind: RepairJsonKindProto::String as i32,
            bool_value: false,
            number_value: String::new(),
            string_value: value.clone(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Array(values) => RepairJsonValueProto {
            kind: RepairJsonKindProto::Array as i32,
            bool_value: false,
            number_value: String::new(),
            string_value: String::new(),
            array_values: values.iter().map(json_value_to_proto).collect(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            RepairJsonValueProto {
                kind: RepairJsonKindProto::Object as i32,
                bool_value: false,
                number_value: String::new(),
                string_value: String::new(),
                array_values: Vec::new(),
                object_keys: entries.iter().map(|(key, _)| (*key).clone()).collect(),
                object_values: entries
                    .into_iter()
                    .map(|(_, value)| json_value_to_proto(value))
                    .collect(),
            }
        }
    }
}

fn json_value_from_proto(proto: RepairJsonValueProto) -> Result<serde_json::Value> {
    match RepairJsonKindProto::try_from(proto.kind)
        .map_err(|_| anyhow!("repair finding evidence value kind is invalid"))?
    {
        RepairJsonKindProto::Null => Ok(serde_json::Value::Null),
        RepairJsonKindProto::Bool => Ok(serde_json::Value::Bool(proto.bool_value)),
        RepairJsonKindProto::Number => {
            let parsed = serde_json::from_str::<serde_json::Value>(&proto.number_value)?;
            if !parsed.is_number() {
                return Err(anyhow!("repair finding evidence number is invalid"));
            }
            Ok(parsed)
        }
        RepairJsonKindProto::String => Ok(serde_json::Value::String(proto.string_value)),
        RepairJsonKindProto::Array => Ok(serde_json::Value::Array(
            proto
                .array_values
                .into_iter()
                .map(json_value_from_proto)
                .collect::<Result<Vec<_>>>()?,
        )),
        RepairJsonKindProto::Object => {
            if proto.object_keys.len() != proto.object_values.len() {
                return Err(anyhow!("repair finding evidence object key/value mismatch"));
            }
            if proto
                .object_keys
                .windows(2)
                .any(|window| window[0] >= window[1])
            {
                return Err(anyhow!(
                    "repair finding evidence object keys are not canonical"
                ));
            }
            let mut object = serde_json::Map::new();
            for (key, value) in proto.object_keys.into_iter().zip(proto.object_values) {
                object.insert(key, json_value_from_proto(value)?);
            }
            Ok(serde_json::Value::Object(object))
        }
        RepairJsonKindProto::Unspecified => {
            Err(anyhow!("repair finding evidence value kind is unspecified"))
        }
    }
}

fn repair_finding_write_lock(scope_kind: &str, scope_id: &str) -> Arc<Mutex<()>> {
    let key = format!("{scope_kind}\0{scope_id}");
    let mut locks = REPAIR_FINDING_WRITE_LOCKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

fn finding_matches_write(finding: &RepairFinding, write: &RepairFindingWrite) -> bool {
    finding.finding_id == write.finding_id
        && finding.scope_kind == write.scope_kind
        && finding.scope_id == write.scope_id
        && finding.repair_task_id == write.repair_task_id
        && finding.lease_fence_token == write.lease_fence_token
        && finding.severity == write.severity
        && finding.status == write.status
        && finding.code == write.code
        && finding.message == write.message
        && finding.subjects == write.subjects
        && finding.proposed_action == write.proposed_action
        && finding.evidence == write.evidence
        && finding.created_at_nanos == write.created_at_nanos
}

fn repair_finding_tuple_key(
    scope_kind: &str,
    scope_id: &str,
    scope_revision: u64,
) -> Result<Vec<u8>> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    if scope_revision == 0 {
        return Err(anyhow!("repair finding scope revision must be nonzero"));
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(scope_kind),
        CoreMetaTuplePart::Utf8(scope_id),
        CoreMetaTuplePart::U64(scope_revision),
    ])
}

fn repair_finding_id_tuple_key(
    scope_kind: &str,
    scope_id: &str,
    finding_id: &str,
) -> Result<Vec<u8>> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    require_safe_component(finding_id, "finding_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(scope_kind),
        CoreMetaTuplePart::Utf8(scope_id),
        CoreMetaTuplePart::Utf8(finding_id),
    ])
}

fn repair_finding_head_tuple_key(scope_kind: &str, scope_id: &str) -> Result<Vec<u8>> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(scope_kind),
        CoreMetaTuplePart::Utf8(scope_id),
    ])
}

fn repair_finding_mvcc_key(
    table_id: u16,
    tuple_key: Vec<u8>,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(CF_MESH, table_id, &tuple_key)
}
