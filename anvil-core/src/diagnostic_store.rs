#[cfg(test)]
use crate::core_store::CoreMetaStore;
use crate::{
    core_store::{
        CF_OBSERVABILITY, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaTuplePart, CoreStore,
        TABLE_DIAGNOSTIC_ROW, commit_coremeta_batch_for_storage, core_meta_committed_row_common,
        core_meta_record_tuple_key, core_meta_root_key_hash, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
    },
    formats::hash32,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;
const DIAGNOSTIC_REF_PREFIX: &str = "diagnostic:";
pub const DIAGNOSTIC_OBJECT_PAGE_MAX: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticObjectRef {
    pub bucket_id: Option<i64>,
    pub object_key: Option<String>,
    pub version_id: Option<String>,
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticObject {
    pub format_version: u16,
    pub diagnostic_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub source: String,
    pub severity: DiagnosticSeverity,
    pub code: String,
    pub message: String,
    pub object_ref: Option<DiagnosticObjectRef>,
    pub details: serde_json::Value,
    pub created_at_nanos: i64,
    pub diagnostic_hash: Option<String>,
    pub diagnostic_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticWrite {
    pub diagnostic_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub source: String,
    pub severity: DiagnosticSeverity,
    pub code: String,
    pub message: String,
    pub object_ref: Option<DiagnosticObjectRef>,
    pub details: serde_json::Value,
    pub created_at_nanos: i64,
}

#[derive(Debug, Clone)]
pub struct DiagnosticObjectPage {
    pub diagnostics: Vec<DiagnosticObject>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum DiagnosticSeverityProto {
    Unspecified = 0,
    Info = 1,
    Warning = 2,
    Error = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum DiagnosticJsonKindProto {
    Unspecified = 0,
    Null = 1,
    Bool = 2,
    Number = 3,
    String = 4,
    Array = 5,
    Object = 6,
}

#[derive(Clone, PartialEq, Message)]
struct DiagnosticJsonValueProto {
    #[prost(enumeration = "DiagnosticJsonKindProto", tag = "1")]
    kind: i32,
    #[prost(bool, tag = "2")]
    bool_value: bool,
    #[prost(string, tag = "3")]
    number_value: String,
    #[prost(string, tag = "4")]
    string_value: String,
    #[prost(message, repeated, tag = "5")]
    array_values: Vec<DiagnosticJsonValueProto>,
    #[prost(string, repeated, tag = "6")]
    object_keys: Vec<String>,
    #[prost(message, repeated, tag = "7")]
    object_values: Vec<DiagnosticJsonValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct DiagnosticObjectRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    body: Option<DiagnosticObjectBodyProto>,
}

#[derive(Clone, PartialEq, Message)]
struct DiagnosticObjectBodyProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    diagnostic_id: String,
    #[prost(string, tag = "3")]
    scope_kind: String,
    #[prost(string, tag = "4")]
    scope_id: String,
    #[prost(string, tag = "5")]
    source: String,
    #[prost(enumeration = "DiagnosticSeverityProto", tag = "6")]
    severity: i32,
    #[prost(string, tag = "7")]
    code: String,
    #[prost(string, tag = "8")]
    message: String,
    #[prost(message, optional, tag = "9")]
    object_ref: Option<DiagnosticObjectRefProto>,
    #[prost(message, optional, tag = "10")]
    details: Option<DiagnosticJsonValueProto>,
    #[prost(int64, tag = "11")]
    created_at_nanos: i64,
    #[prost(string, optional, tag = "12")]
    diagnostic_hash: Option<String>,
    #[prost(string, optional, tag = "13")]
    diagnostic_signature: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct DiagnosticObjectRefProto {
    #[prost(int64, optional, tag = "1")]
    bucket_id: Option<i64>,
    #[prost(string, optional, tag = "2")]
    object_key: Option<String>,
    #[prost(string, optional, tag = "3")]
    version_id: Option<String>,
    #[prost(string, optional, tag = "4")]
    content_hash: Option<String>,
}

impl DiagnosticObject {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_diagnostic(&self)?;
        let hash = hash_diagnostic_object(&self)?;
        let signature = sign_diagnostic_hash(
            signing_key,
            &hash,
            &[
                &self.scope_kind,
                &self.scope_id,
                &self.source,
                &self.diagnostic_id,
            ],
        )?;
        self.diagnostic_hash = Some(hash);
        self.diagnostic_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_diagnostic(self)?;
        let expected_hash = hash_diagnostic_object(self)?;
        if self.diagnostic_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("diagnostic object hash mismatch"));
        }
        let expected_signature = sign_diagnostic_hash(
            signing_key,
            &expected_hash,
            &[
                &self.scope_kind,
                &self.scope_id,
                &self.source,
                &self.diagnostic_id,
            ],
        )?;
        if self.diagnostic_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("diagnostic object signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_diagnostic_object(diagnostic: &DiagnosticObject) -> Result<String> {
    let mut unsigned = diagnostic.clone();
    unsigned.diagnostic_hash = None;
    unsigned.diagnostic_signature = None;
    Ok(hex::encode(hash32(&encode_diagnostic_body(&unsigned)?)))
}

pub async fn write_diagnostic_object(
    storage: &Storage,
    diagnostic: DiagnosticWrite,
    signing_key: &[u8],
) -> Result<DiagnosticObject> {
    validate_write(&diagnostic)?;
    let sealed = DiagnosticObject {
        format_version: 1,
        diagnostic_id: diagnostic.diagnostic_id,
        scope_kind: diagnostic.scope_kind,
        scope_id: diagnostic.scope_id,
        source: diagnostic.source,
        severity: diagnostic.severity,
        code: diagnostic.code,
        message: diagnostic.message,
        object_ref: diagnostic.object_ref,
        details: diagnostic.details,
        created_at_nanos: diagnostic.created_at_nanos,
        diagnostic_hash: None,
        diagnostic_signature: None,
    }
    .seal(signing_key)?;
    write_diagnostic_ref(storage, &sealed).await?;
    Ok(sealed)
}

pub async fn read_diagnostic_object(
    storage: &Storage,
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    diagnostic_id: &str,
    signing_key: &[u8],
) -> Result<Option<DiagnosticObject>> {
    let Some(diagnostic) = read_diagnostic_ref(
        storage,
        &diagnostic_ref_name(scope_kind, scope_id, source, diagnostic_id)?,
    )
    .await?
    else {
        return Ok(None);
    };
    diagnostic.verify(signing_key)?;
    if diagnostic.scope_kind != scope_kind
        || diagnostic.scope_id != scope_id
        || diagnostic.source != source
        || diagnostic.diagnostic_id != diagnostic_id
    {
        return Err(anyhow!("diagnostic object path scope mismatch"));
    }
    Ok(Some(diagnostic))
}

pub async fn list_diagnostic_objects(
    storage: &Storage,
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    min_severity: Option<DiagnosticSeverity>,
    signing_key: &[u8],
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<DiagnosticObjectPage> {
    if !(1..=DIAGNOSTIC_OBJECT_PAGE_MAX).contains(&page_size) {
        return Err(anyhow!(
            "diagnostic object page size must be between 1 and {DIAGNOSTIC_OBJECT_PAGE_MAX}"
        ));
    }
    let prefix = diagnostic_tuple_prefix(scope_kind, scope_id, source)?;
    let store = CoreStore::new(storage.clone()).await?;
    let mut records = store.scan_coremeta_prefix_page(
        CF_OBSERVABILITY,
        TABLE_DIAGNOSTIC_ROW,
        &prefix,
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
                    .ok_or_else(|| anyhow!("diagnostic page continuation has no row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let mut diagnostics = Vec::with_capacity(records.len());
    for record in records {
        let diagnostic = decode_diagnostic_object(&record.payload)?;
        diagnostic.verify(signing_key)?;
        if diagnostic.scope_kind != scope_kind
            || diagnostic.scope_id != scope_id
            || diagnostic.source != source
        {
            return Err(anyhow!("diagnostic object path scope mismatch"));
        }
        if core_meta_record_tuple_key(&record.key)?
            != diagnostic_tuple_key(scope_kind, scope_id, source, &diagnostic.diagnostic_id)?
        {
            return Err(anyhow!("diagnostic object physical row key mismatch"));
        }
        if min_severity
            .map(|minimum| severity_rank(diagnostic.severity) < severity_rank(minimum))
            .unwrap_or(false)
        {
            continue;
        }
        diagnostics.push(diagnostic);
    }
    Ok(DiagnosticObjectPage {
        diagnostics,
        next_tuple_key,
    })
}

async fn write_diagnostic_ref(storage: &Storage, diagnostic: &DiagnosticObject) -> Result<()> {
    let tuple_key = diagnostic_tuple_key(
        &diagnostic.scope_kind,
        &diagnostic.scope_id,
        &diagnostic.source,
        &diagnostic.diagnostic_id,
    )?;
    let payload = encode_diagnostic_object(diagnostic)?;
    let op = CoreMetaBatchOp {
        cf: CF_OBSERVABILITY,
        table_id: TABLE_DIAGNOSTIC_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    commit_coremeta_batch_for_storage(
        storage,
        &format!(
            "diagnostic:{}:{}:{}",
            diagnostic.scope_kind, diagnostic.scope_id, diagnostic.diagnostic_id
        ),
        &[op],
        &[crate::core_store::CoreMetaRootPublication::new(
            diagnostic_root_anchor_key(diagnostic),
            crate::formats::writer::WriterFamily::CoreControl,
        )],
    )
    .await?;
    Ok(())
}

async fn read_diagnostic_ref(
    storage: &Storage,
    ref_name: &str,
) -> Result<Option<DiagnosticObject>> {
    let (scope_kind, scope_id, source, diagnostic_id) = parse_diagnostic_ref_name(ref_name)?;
    let tuple_key = diagnostic_tuple_key(&scope_kind, &scope_id, &source, &diagnostic_id)?;
    let Some(bytes) = CoreStore::new(storage.clone()).await?.read_coremeta_row(
        CF_OBSERVABILITY,
        TABLE_DIAGNOSTIC_ROW,
        &tuple_key,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_diagnostic_object(&bytes)?))
}

fn encode_diagnostic_object(diagnostic: &DiagnosticObject) -> Result<Vec<u8>> {
    encode_diagnostic_object_with_common(diagnostic, diagnostic_common(diagnostic))
}

fn encode_diagnostic_object_with_common(
    diagnostic: &DiagnosticObject,
    common: crate::core_store::CoreMetaRowCommonProto,
) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&DiagnosticObjectRowProto {
        common: Some(common),
        body: Some(diagnostic_to_proto(diagnostic)),
    }))
}

fn decode_diagnostic_object(bytes: &[u8]) -> Result<DiagnosticObject> {
    let row = decode_deterministic_proto::<DiagnosticObjectRowProto>(bytes, "diagnostic object")?;
    let common = row
        .common
        .ok_or_else(|| anyhow!("diagnostic object missing CoreMeta common"))?;
    let diagnostic = diagnostic_from_proto(
        row.body
            .ok_or_else(|| anyhow!("diagnostic object missing domain body"))?,
    )?;
    validate_diagnostic_common(&diagnostic, &common)?;
    Ok(diagnostic)
}

fn encode_diagnostic_body(diagnostic: &DiagnosticObject) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&diagnostic_to_proto(diagnostic)))
}

fn diagnostic_to_proto(diagnostic: &DiagnosticObject) -> DiagnosticObjectBodyProto {
    DiagnosticObjectBodyProto {
        format_version: u32::from(diagnostic.format_version),
        diagnostic_id: diagnostic.diagnostic_id.clone(),
        scope_kind: diagnostic.scope_kind.clone(),
        scope_id: diagnostic.scope_id.clone(),
        source: diagnostic.source.clone(),
        severity: severity_to_proto(diagnostic.severity) as i32,
        code: diagnostic.code.clone(),
        message: diagnostic.message.clone(),
        object_ref: diagnostic.object_ref.as_ref().map(diagnostic_ref_to_proto),
        details: Some(json_value_to_proto(&diagnostic.details)),
        created_at_nanos: diagnostic.created_at_nanos,
        diagnostic_hash: diagnostic.diagnostic_hash.clone(),
        diagnostic_signature: diagnostic.diagnostic_signature.clone(),
    }
}

fn diagnostic_common(diagnostic: &DiagnosticObject) -> crate::core_store::CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        "system",
        core_meta_root_key_hash(&diagnostic_root_anchor_key(diagnostic)),
        1,
        diagnostic.diagnostic_id.clone(),
        diagnostic.created_at_nanos.max(0) as u64,
    )
}

fn diagnostic_root_anchor_key(diagnostic: &DiagnosticObject) -> String {
    format!(
        "diagnostic/{}/{}/{}",
        diagnostic.scope_kind, diagnostic.scope_id, diagnostic.source
    )
}

fn diagnostic_from_proto(proto: DiagnosticObjectBodyProto) -> Result<DiagnosticObject> {
    Ok(DiagnosticObject {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("diagnostic object version exceeds u16"))?,
        diagnostic_id: proto.diagnostic_id,
        scope_kind: proto.scope_kind,
        scope_id: proto.scope_id,
        source: proto.source,
        severity: severity_from_proto(proto.severity)?,
        code: proto.code,
        message: proto.message,
        object_ref: proto.object_ref.map(diagnostic_ref_from_proto),
        details: json_value_from_proto(
            proto
                .details
                .ok_or_else(|| anyhow!("diagnostic object missing details"))?,
        )?,
        created_at_nanos: proto.created_at_nanos,
        diagnostic_hash: proto.diagnostic_hash,
        diagnostic_signature: proto.diagnostic_signature,
    })
}

fn validate_diagnostic_common(
    diagnostic: &DiagnosticObject,
    common: &crate::core_store::CoreMetaRowCommonProto,
) -> Result<()> {
    if common.realm_id != "system" {
        return Err(anyhow!("diagnostic object CoreMeta realm mismatch"));
    }
    if common.root_key_hash != core_meta_root_key_hash(&diagnostic_root_anchor_key(diagnostic)) {
        return Err(anyhow!("diagnostic object CoreMeta root mismatch"));
    }
    if common.root_generation == 0 {
        return Err(anyhow!(
            "diagnostic object CoreMeta root generation must be nonzero"
        ));
    }
    if common.visibility_state_enum() != crate::core_store::CoreMetaVisibilityState::Committed {
        return Err(anyhow!("diagnostic object CoreMeta row is not committed"));
    }
    Ok(())
}

fn diagnostic_ref_to_proto(object_ref: &DiagnosticObjectRef) -> DiagnosticObjectRefProto {
    DiagnosticObjectRefProto {
        bucket_id: object_ref.bucket_id,
        object_key: object_ref.object_key.clone(),
        version_id: object_ref.version_id.clone(),
        content_hash: object_ref.content_hash.clone(),
    }
}

fn diagnostic_ref_from_proto(proto: DiagnosticObjectRefProto) -> DiagnosticObjectRef {
    DiagnosticObjectRef {
        bucket_id: proto.bucket_id,
        object_key: proto.object_key,
        version_id: proto.version_id,
        content_hash: proto.content_hash,
    }
}

fn severity_to_proto(severity: DiagnosticSeverity) -> DiagnosticSeverityProto {
    match severity {
        DiagnosticSeverity::Info => DiagnosticSeverityProto::Info,
        DiagnosticSeverity::Warning => DiagnosticSeverityProto::Warning,
        DiagnosticSeverity::Error => DiagnosticSeverityProto::Error,
    }
}

fn severity_from_proto(severity: i32) -> Result<DiagnosticSeverity> {
    match DiagnosticSeverityProto::try_from(severity)
        .map_err(|_| anyhow!("diagnostic object severity is invalid"))?
    {
        DiagnosticSeverityProto::Info => Ok(DiagnosticSeverity::Info),
        DiagnosticSeverityProto::Warning => Ok(DiagnosticSeverity::Warning),
        DiagnosticSeverityProto::Error => Ok(DiagnosticSeverity::Error),
        DiagnosticSeverityProto::Unspecified => {
            Err(anyhow!("diagnostic object severity is unspecified"))
        }
    }
}

fn json_value_to_proto(value: &serde_json::Value) -> DiagnosticJsonValueProto {
    match value {
        serde_json::Value::Null => DiagnosticJsonValueProto {
            kind: DiagnosticJsonKindProto::Null as i32,
            bool_value: false,
            number_value: String::new(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Bool(value) => DiagnosticJsonValueProto {
            kind: DiagnosticJsonKindProto::Bool as i32,
            bool_value: *value,
            number_value: String::new(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Number(value) => DiagnosticJsonValueProto {
            kind: DiagnosticJsonKindProto::Number as i32,
            bool_value: false,
            number_value: value.to_string(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::String(value) => DiagnosticJsonValueProto {
            kind: DiagnosticJsonKindProto::String as i32,
            bool_value: false,
            number_value: String::new(),
            string_value: value.clone(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Array(values) => DiagnosticJsonValueProto {
            kind: DiagnosticJsonKindProto::Array as i32,
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
            DiagnosticJsonValueProto {
                kind: DiagnosticJsonKindProto::Object as i32,
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

fn json_value_from_proto(proto: DiagnosticJsonValueProto) -> Result<serde_json::Value> {
    match DiagnosticJsonKindProto::try_from(proto.kind)
        .map_err(|_| anyhow!("diagnostic detail value kind is invalid"))?
    {
        DiagnosticJsonKindProto::Null => Ok(serde_json::Value::Null),
        DiagnosticJsonKindProto::Bool => Ok(serde_json::Value::Bool(proto.bool_value)),
        DiagnosticJsonKindProto::Number => {
            let parsed = serde_json::from_str::<serde_json::Value>(&proto.number_value)?;
            if !parsed.is_number() {
                return Err(anyhow!("diagnostic detail number is invalid"));
            }
            Ok(parsed)
        }
        DiagnosticJsonKindProto::String => Ok(serde_json::Value::String(proto.string_value)),
        DiagnosticJsonKindProto::Array => Ok(serde_json::Value::Array(
            proto
                .array_values
                .into_iter()
                .map(json_value_from_proto)
                .collect::<Result<Vec<_>>>()?,
        )),
        DiagnosticJsonKindProto::Object => {
            if proto.object_keys.len() != proto.object_values.len() {
                return Err(anyhow!("diagnostic detail object key/value mismatch"));
            }
            if proto
                .object_keys
                .windows(2)
                .any(|window| window[0] >= window[1])
            {
                return Err(anyhow!("diagnostic detail object keys are not canonical"));
            }
            let mut object = serde_json::Map::new();
            for (key, value) in proto.object_keys.into_iter().zip(proto.object_values) {
                object.insert(key, json_value_from_proto(value)?);
            }
            Ok(serde_json::Value::Object(object))
        }
        DiagnosticJsonKindProto::Unspecified => {
            Err(anyhow!("diagnostic detail value kind is unspecified"))
        }
    }
}

fn validate_write(diagnostic: &DiagnosticWrite) -> Result<()> {
    let unsigned = DiagnosticObject {
        format_version: 1,
        diagnostic_id: diagnostic.diagnostic_id.clone(),
        scope_kind: diagnostic.scope_kind.clone(),
        scope_id: diagnostic.scope_id.clone(),
        source: diagnostic.source.clone(),
        severity: diagnostic.severity,
        code: diagnostic.code.clone(),
        message: diagnostic.message.clone(),
        object_ref: diagnostic.object_ref.clone(),
        details: diagnostic.details.clone(),
        created_at_nanos: diagnostic.created_at_nanos,
        diagnostic_hash: None,
        diagnostic_signature: None,
    };
    validate_unsigned_diagnostic(&unsigned)
}

fn validate_unsigned_diagnostic(diagnostic: &DiagnosticObject) -> Result<()> {
    if diagnostic.format_version != 1 {
        return Err(anyhow!("unsupported diagnostic object version"));
    }
    require_safe_component(&diagnostic.diagnostic_id, "diagnostic_id")?;
    require_safe_component(&diagnostic.scope_kind, "scope_kind")?;
    require_safe_component(&diagnostic.scope_id, "scope_id")?;
    require_safe_component(&diagnostic.source, "source")?;
    require_nonempty(&diagnostic.code, "code")?;
    require_nonempty(&diagnostic.message, "message")?;
    if diagnostic.created_at_nanos < 0 {
        return Err(anyhow!("diagnostic object timestamp must be nonnegative"));
    }
    if let Some(object_ref) = diagnostic.object_ref.as_ref() {
        if let Some(content_hash) = object_ref.content_hash.as_ref() {
            validate_optional_hash(content_hash, "content_hash")?;
        }
    }
    Ok(())
}

fn severity_rank(severity: DiagnosticSeverity) -> u8 {
    match severity {
        DiagnosticSeverity::Info => 0,
        DiagnosticSeverity::Warning => 1,
        DiagnosticSeverity::Error => 2,
    }
}

fn sign_diagnostic_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("diagnostic object signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"diagnostic_object");
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn validate_optional_hash(value: &str, field: &'static str) -> Result<()> {
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

fn diagnostic_ref_prefix(scope_kind: &str, scope_id: &str, source: &str) -> Result<String> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    require_safe_component(source, "source")?;
    Ok(format!(
        "{DIAGNOSTIC_REF_PREFIX}scope:{scope_kind}:id:{scope_id}:source:{source}:"
    ))
}

fn diagnostic_ref_name(
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    diagnostic_id: &str,
) -> Result<String> {
    require_safe_component(diagnostic_id, "diagnostic_id")?;
    Ok(format!(
        "{}diagnostic:{diagnostic_id}",
        diagnostic_ref_prefix(scope_kind, scope_id, source)?
    ))
}

fn diagnostic_tuple_prefix(scope_kind: &str, scope_id: &str, source: &str) -> Result<Vec<u8>> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    require_safe_component(source, "source")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("diagnostic"),
        CoreMetaTuplePart::Utf8(scope_kind),
        CoreMetaTuplePart::Utf8(scope_id),
        CoreMetaTuplePart::Utf8(source),
    ])
}

fn diagnostic_tuple_key(
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    diagnostic_id: &str,
) -> Result<Vec<u8>> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    require_safe_component(source, "source")?;
    require_safe_component(diagnostic_id, "diagnostic_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("diagnostic"),
        CoreMetaTuplePart::Utf8(scope_kind),
        CoreMetaTuplePart::Utf8(scope_id),
        CoreMetaTuplePart::Utf8(source),
        CoreMetaTuplePart::Utf8(diagnostic_id),
    ])
}

fn parse_diagnostic_ref_name(ref_name: &str) -> Result<(String, String, String, String)> {
    let parts = ref_name.split(':').collect::<Vec<_>>();
    if parts.len() != 9
        || parts[0] != DIAGNOSTIC_REF_PREFIX.trim_end_matches(':')
        || parts[1] != "scope"
        || parts[3] != "id"
        || parts[5] != "source"
        || parts[7] != "diagnostic"
    {
        return Err(anyhow!("diagnostic CoreMeta ref name has invalid shape"));
    }
    Ok((
        parts[2].to_string(),
        parts[4].to_string(),
        parts[6].to_string(),
        parts[8].to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"diagnostic object signing key";

    #[tokio::test]
    async fn diagnostic_objects_write_read_and_list_from_coremeta_rows() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = write_diagnostic_object(
            &storage,
            diagnostic("diag-001", 10, DiagnosticSeverity::Info),
            KEY,
        )
        .await
        .unwrap();
        let second = write_diagnostic_object(
            &storage,
            diagnostic("diag-002", 20, DiagnosticSeverity::Error),
            KEY,
        )
        .await
        .unwrap();
        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        for (expected, expected_generation) in [(&first, 1), (&second, 2)] {
            let payload = meta
                .get(
                    CF_OBSERVABILITY,
                    TABLE_DIAGNOSTIC_ROW,
                    &diagnostic_tuple_key(
                        "bucket",
                        "tenant-1-bucket-2",
                        "full-text",
                        &expected.diagnostic_id,
                    )
                    .unwrap(),
                )
                .unwrap()
                .unwrap();
            let common = crate::core_store::core_meta_row_common_from_payload(&payload).unwrap();
            assert_eq!(common.root_generation, expected_generation);
            assert_ne!(common.root_generation, expected.created_at_nanos as u64);
            assert_ne!(common.transaction_id, expected.diagnostic_id);
            let decoded = decode_diagnostic_object(&payload).unwrap();
            decoded.verify(KEY).unwrap();
            assert_eq!(decoded.diagnostic_hash, expected.diagnostic_hash);
            assert_eq!(decoded.diagnostic_signature, expected.diagnostic_signature);
        }
        assert_eq!(
            diagnostic_ref_name("bucket", "tenant-1-bucket-2", "full-text", "diag-001").unwrap(),
            "diagnostic:scope:bucket:id:tenant-1-bucket-2:source:full-text:diagnostic:diag-001"
        );

        assert_eq!(
            read_diagnostic_object(
                &storage,
                "bucket",
                "tenant-1-bucket-2",
                "full-text",
                "diag-001",
                KEY,
            )
            .await
            .unwrap()
            .unwrap(),
            first
        );
        let all = list_diagnostic_objects(
            &storage,
            "bucket",
            "tenant-1-bucket-2",
            "full-text",
            None,
            KEY,
            None,
            1000,
        )
        .await
        .unwrap();
        assert_eq!(all.diagnostics, vec![first.clone(), second.clone()]);
        assert!(all.next_tuple_key.is_none());

        let errors = list_diagnostic_objects(
            &storage,
            "bucket",
            "tenant-1-bucket-2",
            "full-text",
            Some(DiagnosticSeverity::Warning),
            KEY,
            None,
            1000,
        )
        .await
        .unwrap();
        assert_eq!(errors.diagnostics, vec![second]);
        assert!(errors.next_tuple_key.is_none());
    }

    #[test]
    fn diagnostic_hash_and_signature_survive_physical_common_rebinding() {
        let sealed = sealed_diagnostic("diag-001", 10, DiagnosticSeverity::Warning);
        let encoded = encode_diagnostic_object(&sealed).unwrap();
        let mut row = decode_deterministic_proto::<DiagnosticObjectRowProto>(
            &encoded,
            "diagnostic object test row",
        )
        .unwrap();
        let common = row.common.as_mut().unwrap();
        common.root_generation = 73;
        common.transaction_id = "corestore-publication-73".to_string();
        common.created_at_unix_nanos = 999;
        let rebound = encode_deterministic_proto(&row);

        let decoded = decode_diagnostic_object(&rebound).unwrap();
        decoded.verify(KEY).unwrap();
        assert_eq!(decoded, sealed);

        let valid_common = diagnostic_common(&sealed);
        let mut invalid_commons = Vec::new();
        let mut invalid = valid_common.clone();
        invalid.realm_id = "tenant/not-system".to_string();
        invalid_commons.push(invalid);
        let mut invalid = valid_common.clone();
        invalid.root_key_hash = core_meta_root_key_hash("wrong-diagnostic-root");
        invalid_commons.push(invalid);
        let mut invalid = valid_common.clone();
        invalid.root_generation = 0;
        invalid_commons.push(invalid);
        let mut invalid = valid_common;
        invalid.visibility_state = crate::core_store::CoreMetaVisibilityState::Pending as i32;
        invalid_commons.push(invalid);
        for common in invalid_commons {
            let bytes = encode_diagnostic_object_with_common(&sealed, common).unwrap();
            assert!(decode_diagnostic_object(&bytes).is_err());
        }
    }

    #[tokio::test]
    async fn diagnostic_pages_seek_by_source_and_bound_materialization() -> Result<()> {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for (id, created_at) in [("diag-001", 10), ("diag-002", 20), ("diag-003", 30)] {
            write_diagnostic_object(
                &storage,
                diagnostic(id, created_at, DiagnosticSeverity::Info),
                KEY,
            )
            .await
            .unwrap();
        }

        // This malformed row is in the same table but a different source prefix.
        let malformed = DiagnosticObjectRowProto {
            common: Some(core_meta_committed_row_common(
                "system",
                core_meta_root_key_hash("diagnostic/bucket/tenant-1-bucket-2/other-source"),
                1,
                "malformed-diagnostic",
                1,
            )),
            body: Some(DiagnosticObjectBodyProto {
                format_version: 1,
                diagnostic_id: "broken".to_string(),
                scope_kind: "bucket".to_string(),
                scope_id: "tenant-1-bucket-2".to_string(),
                source: "other-source".to_string(),
                severity: DiagnosticSeverityProto::Info as i32,
                code: "Broken".to_string(),
                message: "broken".to_string(),
                object_ref: None,
                details: None,
                created_at_nanos: 1,
                diagnostic_hash: None,
                diagnostic_signature: None,
            }),
        };
        CoreMetaStore::open(storage.core_store_meta_path())?.put(
            CF_OBSERVABILITY,
            TABLE_DIAGNOSTIC_ROW,
            &diagnostic_tuple_key("bucket", "tenant-1-bucket-2", "other-source", "broken")?,
            &encode_deterministic_proto(&malformed),
        )?;

        let hidden = diagnostic("diag-000", 5, DiagnosticSeverity::Info);
        let hidden = DiagnosticObject {
            format_version: 1,
            diagnostic_id: hidden.diagnostic_id,
            scope_kind: hidden.scope_kind,
            scope_id: hidden.scope_id,
            source: hidden.source,
            severity: hidden.severity,
            code: hidden.code,
            message: hidden.message,
            object_ref: hidden.object_ref,
            details: hidden.details,
            created_at_nanos: hidden.created_at_nanos,
            diagnostic_hash: None,
            diagnostic_signature: None,
        }
        .seal(KEY)?;
        let mut hidden_common = diagnostic_common(&hidden);
        hidden_common.root_generation = 4;
        hidden_common.transaction_id = "corestore-hidden-publication".to_string();
        CoreMetaStore::open(storage.core_store_meta_path())?.put(
            CF_OBSERVABILITY,
            TABLE_DIAGNOSTIC_ROW,
            &diagnostic_tuple_key("bucket", "tenant-1-bucket-2", "full-text", "diag-000")?,
            &encode_diagnostic_object_with_common(&hidden, hidden_common)?,
        )?;

        let filtered = list_diagnostic_objects(
            &storage,
            "bucket",
            "tenant-1-bucket-2",
            "full-text",
            Some(DiagnosticSeverity::Error),
            KEY,
            None,
            1,
        )
        .await?;
        assert!(filtered.diagnostics.is_empty());
        assert!(filtered.next_tuple_key.is_some());

        let first = list_diagnostic_objects(
            &storage,
            "bucket",
            "tenant-1-bucket-2",
            "full-text",
            None,
            KEY,
            None,
            1,
        )
        .await?;
        assert_eq!(first.diagnostics.len(), 1);
        assert_eq!(first.diagnostics[0].diagnostic_id, "diag-001");
        let second = list_diagnostic_objects(
            &storage,
            "bucket",
            "tenant-1-bucket-2",
            "full-text",
            None,
            KEY,
            first.next_tuple_key.as_deref(),
            1,
        )
        .await?;
        assert_eq!(second.diagnostics.len(), 1);
        assert_eq!(second.diagnostics[0].diagnostic_id, "diag-002");
        assert!(second.next_tuple_key.is_some());
        assert!(
            list_diagnostic_objects(
                &storage,
                "bucket",
                "tenant-1-bucket-2",
                "full-text",
                None,
                KEY,
                None,
                0,
            )
            .await
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn diagnostic_objects_reject_tamper_and_scope_mismatch() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        write_diagnostic_object(
            &storage,
            diagnostic("diag-001", 10, DiagnosticSeverity::Warning),
            KEY,
        )
        .await
        .unwrap();
        let ref_name =
            diagnostic_ref_name("bucket", "tenant-1-bucket-2", "full-text", "diag-001").unwrap();
        let (scope_kind, scope_id, source, diagnostic_id) =
            parse_diagnostic_ref_name(&ref_name).unwrap();
        let tuple_key =
            diagnostic_tuple_key(&scope_kind, &scope_id, &source, &diagnostic_id).unwrap();
        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        let mut value = meta
            .get(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &tuple_key)
            .unwrap()
            .unwrap();
        *value
            .last_mut()
            .expect("stored diagnostic bytes are not empty") ^= 0x01;
        meta.put(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &tuple_key, &value)
            .unwrap();
        assert!(
            read_diagnostic_object(
                &storage,
                "bucket",
                "tenant-1-bucket-2",
                "full-text",
                "diag-001",
                KEY,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn diagnostic_objects_reject_unsafe_paths_and_invalid_payloads() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        assert!(diagnostic_ref_name("../bucket", "tenant", "source", "diag").is_err());
        assert!(diagnostic_ref_name("bucket", "tenant", "../source", "diag").is_err());
        let mut invalid = diagnostic("diag-001", 10, DiagnosticSeverity::Info);
        invalid.message.clear();
        assert!(
            write_diagnostic_object(&storage, invalid, KEY)
                .await
                .is_err()
        );
        let mut invalid_hash = diagnostic("diag-002", 10, DiagnosticSeverity::Info);
        invalid_hash.object_ref = Some(DiagnosticObjectRef {
            bucket_id: Some(2),
            object_key: Some("a".to_string()),
            version_id: Some(uuid::Uuid::new_v4().to_string()),
            content_hash: Some("not-hex".to_string()),
        });
        assert!(
            write_diagnostic_object(&storage, invalid_hash, KEY)
                .await
                .is_err()
        );
    }

    fn diagnostic(
        id: &str,
        created_at_nanos: i64,
        severity: DiagnosticSeverity,
    ) -> DiagnosticWrite {
        DiagnosticWrite {
            diagnostic_id: id.to_string(),
            scope_kind: "bucket".to_string(),
            scope_id: "tenant-1-bucket-2".to_string(),
            source: "full-text".to_string(),
            severity,
            code: "IndexLag".to_string(),
            message: "index partition is behind source watch cursor".to_string(),
            object_ref: Some(DiagnosticObjectRef {
                bucket_id: Some(2),
                object_key: Some("docs/a.txt".to_string()),
                version_id: Some(uuid::Uuid::new_v4().to_string()),
                content_hash: Some(hex::encode([3; 32])),
            }),
            details: serde_json::json!({"source_cursor": 50, "processed_cursor": 40}),
            created_at_nanos,
        }
    }

    fn sealed_diagnostic(
        id: &str,
        created_at_nanos: i64,
        severity: DiagnosticSeverity,
    ) -> DiagnosticObject {
        let write = diagnostic(id, created_at_nanos, severity);
        DiagnosticObject {
            format_version: 1,
            diagnostic_id: write.diagnostic_id,
            scope_kind: write.scope_kind,
            scope_id: write.scope_id,
            source: write.source,
            severity: write.severity,
            code: write.code,
            message: write.message,
            object_ref: write.object_ref,
            details: write.details,
            created_at_nanos: write.created_at_nanos,
            diagnostic_hash: None,
            diagnostic_signature: None,
        }
        .seal(KEY)
        .unwrap()
    }
}
