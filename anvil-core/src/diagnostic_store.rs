use crate::{
    core_store::{
        CF_OBSERVABILITY, CoreMetaTuplePart, TABLE_DIAGNOSTIC_ROW, TABLE_STREAM_HEAD_ROW,
        TABLE_STREAM_RECORD_INDEX_ROW, core_meta_tuple_key, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::hash32,
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::{
        ProductMutation, coremeta_application_prefix, coremeta_logical_key,
        coremeta_tuple_from_logical_key, stream_logical_key,
    },
    mvcc_transaction::{LogicalKey, PredicateKind},
};
use anyhow::{Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;
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
    pub snapshot_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiagnosticHead {
    schema: String,
    last_sequence: u64,
    last_event_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiagnosticEvent {
    schema: String,
    sequence: u64,
    previous_event_hash: String,
    event_hash: String,
    mutation_id: String,
    payload_ref: String,
    payload: Vec<u8>,
}

const DIAGNOSTIC_HEAD_SCHEMA: &str = "anvil.diagnostic.head.v2";
const DIAGNOSTIC_EVENT_SCHEMA: &str = "anvil.diagnostic.event.v2";
const DIAGNOSTIC_ROW_SCHEMA: &str = "anvil.diagnostic.current.v2";

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
    #[prost(string, tag = "1")]
    schema: String,
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
    mvcc: &MvccSubsystem,
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
    write_diagnostic_ref(mvcc, &sealed).await?;
    Ok(sealed)
}

pub fn read_diagnostic_object(
    mvcc: &MvccSubsystem,
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    diagnostic_id: &str,
    signing_key: &[u8],
) -> Result<Option<DiagnosticObject>> {
    read_diagnostic_object_at_snapshot(
        mvcc,
        scope_kind,
        scope_id,
        source,
        diagnostic_id,
        signing_key,
        mvcc.runtime.applied_version()?,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn read_diagnostic_object_at_snapshot(
    mvcc: &MvccSubsystem,
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    diagnostic_id: &str,
    signing_key: &[u8],
    snapshot_version: u64,
) -> Result<Option<DiagnosticObject>> {
    let Some(diagnostic) = read_diagnostic_ref(
        mvcc,
        scope_kind,
        scope_id,
        source,
        diagnostic_id,
        snapshot_version,
    )?
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

pub fn list_diagnostic_objects(
    mvcc: &MvccSubsystem,
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    min_severity: Option<DiagnosticSeverity>,
    signing_key: &[u8],
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<DiagnosticObjectPage> {
    list_diagnostic_objects_at_snapshot(
        mvcc,
        scope_kind,
        scope_id,
        source,
        min_severity,
        signing_key,
        after_tuple_key,
        page_size,
        mvcc.runtime.applied_version()?,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn list_diagnostic_objects_at_snapshot(
    mvcc: &MvccSubsystem,
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    min_severity: Option<DiagnosticSeverity>,
    signing_key: &[u8],
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
    snapshot_version: u64,
) -> Result<DiagnosticObjectPage> {
    if !(1..=DIAGNOSTIC_OBJECT_PAGE_MAX).contains(&page_size) {
        return Err(anyhow!(
            "diagnostic object page size must be between 1 and {DIAGNOSTIC_OBJECT_PAGE_MAX}"
        ));
    }
    let prefix = diagnostic_tuple_prefix(scope_kind, scope_id, source)?;
    if after_tuple_key.is_some_and(|cursor| !cursor.starts_with(&prefix)) {
        return Err(anyhow!(
            "diagnostic continuation is outside the requested scope"
        ));
    }
    let application_prefix = coremeta_application_prefix(CF_OBSERVABILITY, &prefix)?;
    let mut records = mvcc.runtime.scan_table_prefix_at(
        TABLE_DIAGNOSTIC_ROW,
        &application_prefix,
        snapshot_version,
    )?;
    if let Some(after) = after_tuple_key {
        records.retain(|(key, _)| {
            coremeta_tuple_from_logical_key(key, CF_OBSERVABILITY).is_ok_and(|tuple| tuple > after)
        });
    }
    let has_more = records.len() > page_size;
    if has_more {
        records.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            coremeta_tuple_from_logical_key(
                &records
                    .last()
                    .ok_or_else(|| anyhow!("diagnostic page continuation has no row"))?
                    .0,
                CF_OBSERVABILITY,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let mut diagnostics = Vec::with_capacity(records.len());
    for (logical_key, record) in records {
        let diagnostic = decode_diagnostic_object(&record.value)?;
        diagnostic.verify(signing_key)?;
        if diagnostic.scope_kind != scope_kind
            || diagnostic.scope_id != scope_id
            || diagnostic.source != source
        {
            return Err(anyhow!("diagnostic object path scope mismatch"));
        }
        if coremeta_tuple_from_logical_key(&logical_key, CF_OBSERVABILITY)?
            != diagnostic_tuple_key(scope_kind, scope_id, source, &diagnostic.diagnostic_id)?
        {
            return Err(anyhow!("diagnostic object logical row key mismatch"));
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
        snapshot_version,
    })
}

async fn write_diagnostic_ref(mvcc: &MvccSubsystem, diagnostic: &DiagnosticObject) -> Result<()> {
    let tuple_key = diagnostic_tuple_key(
        &diagnostic.scope_kind,
        &diagnostic.scope_id,
        &diagnostic.source,
        &diagnostic.diagnostic_id,
    )?;
    let payload = encode_diagnostic_object(diagnostic)?;
    let current_key = coremeta_logical_key(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &tuple_key)?;
    let snapshot = mvcc.runtime.applied_version()?;
    if let Some(existing) = mvcc.runtime.read_at(&current_key, snapshot)? {
        if existing.value == payload {
            return Ok(());
        }
        return Err(anyhow!(
            "diagnostic ID already names a different immutable object"
        ));
    }
    let stream_id = diagnostic_stream_id(
        &diagnostic.scope_kind,
        &diagnostic.scope_id,
        &diagnostic.source,
    )?;
    let head_key = stream_logical_key(TABLE_STREAM_HEAD_ROW, &stream_id, None)?;
    let observed_head = mvcc
        .runtime
        .read_at(&head_key, snapshot)?
        .map(|row| row.value);
    let mut head = observed_head
        .as_deref()
        .map(decode_diagnostic_head)
        .transpose()?
        .unwrap_or(DiagnosticHead {
            schema: DIAGNOSTIC_HEAD_SCHEMA.to_string(),
            last_sequence: 0,
            last_event_hash: String::new(),
        });
    head.last_sequence = head
        .last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("diagnostic event sequence overflow"))?;
    let payload_ref = format!("inline:sha256:{}", hex::encode(hash32(&payload)));
    let event_hash = diagnostic_event_hash(
        head.last_sequence,
        &head.last_event_hash,
        &diagnostic.diagnostic_id,
        &payload_ref,
    );
    let event = DiagnosticEvent {
        schema: DIAGNOSTIC_EVENT_SCHEMA.to_string(),
        sequence: head.last_sequence,
        previous_event_hash: head.last_event_hash.clone(),
        event_hash: event_hash.clone(),
        mutation_id: diagnostic.diagnostic_id.clone(),
        payload_ref,
        payload: payload.clone(),
    };
    head.last_event_hash = event_hash;
    let event_key = stream_logical_key(
        TABLE_STREAM_RECORD_INDEX_ROW,
        &stream_id,
        Some(head.last_sequence),
    )?;
    commit_diagnostic_mutations(
        mvcc,
        &stream_id,
        &format!(
            "diagnostic:{}:{}:{}:{}:{}",
            diagnostic.scope_kind,
            diagnostic.scope_id,
            diagnostic.source,
            diagnostic.diagnostic_id,
            head.last_sequence,
        ),
        vec![
            ProductMutation::put(current_key.clone(), payload),
            ProductMutation::put(event_key.clone(), serde_json::to_vec(&event)?),
            ProductMutation::put(head_key.clone(), serde_json::to_vec(&head)?),
        ],
        vec![
            (current_key, PredicateKind::Absent),
            (event_key, PredicateKind::Absent),
            (head_key, predicate_for(observed_head.as_deref())),
        ],
    )
    .await?;
    Ok(())
}

fn read_diagnostic_ref(
    mvcc: &MvccSubsystem,
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    diagnostic_id: &str,
    snapshot_version: u64,
) -> Result<Option<DiagnosticObject>> {
    let tuple_key = diagnostic_tuple_key(scope_kind, scope_id, source, diagnostic_id)?;
    let key = coremeta_logical_key(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &tuple_key)?;
    let Some(bytes) = mvcc.runtime.read_at(&key, snapshot_version)? else {
        return Ok(None);
    };
    Ok(Some(decode_diagnostic_object(&bytes.value)?))
}

fn encode_diagnostic_object(diagnostic: &DiagnosticObject) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&DiagnosticObjectRowProto {
        schema: DIAGNOSTIC_ROW_SCHEMA.to_string(),
        body: Some(diagnostic_to_proto(diagnostic)),
    }))
}

fn decode_diagnostic_object(bytes: &[u8]) -> Result<DiagnosticObject> {
    let row = decode_deterministic_proto::<DiagnosticObjectRowProto>(bytes, "diagnostic object")?;
    if row.schema != DIAGNOSTIC_ROW_SCHEMA {
        return Err(anyhow!("diagnostic MVCC row schema mismatch"));
    }
    let diagnostic = diagnostic_from_proto(
        row.body
            .ok_or_else(|| anyhow!("diagnostic object missing domain body"))?,
    )?;
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

fn diagnostic_stream_id(scope_kind: &str, scope_id: &str, source: &str) -> Result<String> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    require_safe_component(source, "source")?;
    Ok(format!("diagnostic:{scope_kind}:{scope_id}:{source}"))
}

fn decode_diagnostic_head(payload: &[u8]) -> Result<DiagnosticHead> {
    let head: DiagnosticHead = serde_json::from_slice(payload)?;
    if head.schema != DIAGNOSTIC_HEAD_SCHEMA
        || (head.last_sequence == 0) != head.last_event_hash.is_empty()
    {
        return Err(anyhow!("diagnostic MVCC head is invalid"));
    }
    Ok(head)
}

fn diagnostic_event_hash(
    sequence: u64,
    previous_hash: &str,
    mutation_id: &str,
    payload_ref: &str,
) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes.extend_from_slice(previous_hash.as_bytes());
    bytes.extend_from_slice(mutation_id.as_bytes());
    bytes.extend_from_slice(payload_ref.as_bytes());
    hex::encode(hash32(&bytes))
}

fn predicate_for(payload: Option<&[u8]>) -> PredicateKind {
    payload
        .map(|payload| PredicateKind::ValueHash(*blake3::hash(payload).as_bytes()))
        .unwrap_or(PredicateKind::Absent)
}

fn now_unix_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default()
}

async fn commit_diagnostic_mutations(
    mvcc: &MvccSubsystem,
    assignment_identity: &str,
    idempotency_key: &str,
    mutations: Vec<ProductMutation>,
    predicates: Vec<(LogicalKey, PredicateKind)>,
) -> Result<()> {
    let principal = "diagnostic-store";
    let assignment = mvcc
        .reconcile_work_assignment("diagnostic", assignment_identity)
        .await?
        .ok_or_else(|| anyhow!("local node does not own the diagnostic assignment"))?;
    let now = now_unix_ms();
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            principal,
            idempotency_key,
            std::time::Duration::from_secs(30),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now,
        )
        .await?;
    let status = mvcc
        .open_transactions
        .status(&handle.transaction_id, principal, now)?;
    if status.state == "open" {
        mvcc.stage_product_mutations(&handle.transaction_id, principal, mutations, now)?;
        for (key, kind) in predicates {
            mvcc.stage_predicate(&handle.transaction_id, principal, key, kind, now)?;
        }
        mvcc.stage_assignment_guard(&handle.transaction_id, principal, &assignment, now)?;
    }
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            principal,
            now_unix_ms(),
        )
        .await?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
            Err(anyhow!("diagnostic transaction aborted: {reason:?}"))
        }
    }
}
