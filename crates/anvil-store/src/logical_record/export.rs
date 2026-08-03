use std::fmt;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rocksdb::{Direction, IteratorMode};
use serde::{Deserialize, Serialize};

use super::*;
use crate::key::BUCKET_NAME_TYPE;

pub const MAX_LOGICAL_RECORD_EXPORT_RECORDS: u32 = 1_000;
pub const MAX_LOGICAL_RECORD_EXPORT_BYTES: u64 = 64 * 1024 * 1024;
const CURSOR_FORMAT: u8 = 1;
const MAX_CURSOR_KEY_BYTES: usize = 1_024;

/// Opaque, restart-stable position after the last record in an export page.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogicalRecordCursor(String);

impl LogicalRecordCursor {
    pub fn from_token(token: impl Into<String>) -> Result<Self, LogicalRecordError> {
        let cursor = Self(token.into());
        cursor.position()?;
        Ok(cursor)
    }

    pub fn as_token(&self) -> &str {
        &self.0
    }

    fn from_position(position: &CursorPosition) -> Self {
        let mut bytes = Vec::with_capacity(4 + position.key.len());
        bytes.push(CURSOR_FORMAT);
        bytes.push(position.domain as u8);
        bytes.extend_from_slice(&(position.key.len() as u16).to_be_bytes());
        bytes.extend_from_slice(&position.key);
        Self(URL_SAFE_NO_PAD.encode(bytes))
    }

    fn position(&self) -> Result<CursorPosition, LogicalRecordError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(&self.0)
            .map_err(|_| LogicalRecordError::InvalidCursor)?;
        let [format, domain, length_high, length_low, rest @ ..] = bytes.as_slice() else {
            return Err(LogicalRecordError::InvalidCursor);
        };
        if *format != CURSOR_FORMAT {
            return Err(LogicalRecordError::InvalidCursor);
        }
        let length = usize::from(u16::from_be_bytes([*length_high, *length_low]));
        if length == 0 || length > MAX_CURSOR_KEY_BYTES || rest.len() != length {
            return Err(LogicalRecordError::InvalidCursor);
        }
        let domain = ExportDomain::from_byte(*domain).ok_or(LogicalRecordError::InvalidCursor)?;
        if !domain.matches(rest) {
            return Err(LogicalRecordError::InvalidCursor);
        }
        Ok(CursorPosition {
            domain,
            key: rest.to_vec(),
        })
    }
}

impl fmt::Debug for LogicalRecordCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalRecordCursor")
            .field("token", &"[OPAQUE]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalRecordExport {
    pub id: LogicalRecordId,
    pub candidate: LogicalRecordCandidate,
}

impl LogicalRecordExport {
    /// Canonical transfer order shared by every bounded export stream.
    pub fn handoff_order_key(&self) -> Result<Vec<u8>, LogicalRecordError> {
        self.validate()?;
        let domain = ExportDomain::for_id(&self.id);
        let location = self.id.location()?;
        if location.cf != domain.column_family() || !domain.matches(&location.key) {
            return Err(storage("logical record location is not canonical"));
        }
        let mut ordered = Vec::with_capacity(location.key.len() + 1);
        ordered.push(domain as u8);
        ordered.extend_from_slice(&location.key);
        Ok(ordered)
    }

    pub fn validate(&self) -> Result<(), LogicalRecordError> {
        self.id.validate()?;
        match &self.candidate {
            LogicalRecordCandidate::Baseline {
                typed_value,
                baseline_hash,
            } => {
                typed_value.validate()?;
                if typed_value.id() != self.id
                    || computed_baseline_hash(typed_value)? != *baseline_hash
                {
                    return Err(LogicalRecordError::Tampered);
                }
            }
            LogicalRecordCandidate::Versioned(mutation) => {
                mutation.validate()?;
                if mutation.typed_value.id() != self.id {
                    return Err(LogicalRecordError::Tampered);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalRecordExportPage {
    pub records: Vec<LogicalRecordExport>,
    pub next_cursor: Option<LogicalRecordCursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogicalRecordSnapshotApplied {
    pub record_version: Option<VersionId>,
    pub replayed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
enum ExportDomain {
    TenantNameClaim = 0,
    BucketNameClaim = 1,
    TenantRecord = 2,
    BucketRecord = 3,
    BucketOptions = 4,
    BucketPolicy = 5,
    Application = 6,
    Credential = 7,
    TenantSchema = 8,
}

impl ExportDomain {
    const ALL: [Self; 9] = [
        Self::TenantNameClaim,
        Self::BucketNameClaim,
        Self::TenantRecord,
        Self::BucketRecord,
        Self::BucketOptions,
        Self::BucketPolicy,
        Self::Application,
        Self::Credential,
        Self::TenantSchema,
    ];

    fn from_byte(value: u8) -> Option<Self> {
        Self::ALL.into_iter().find(|domain| *domain as u8 == value)
    }

    fn for_id(id: &LogicalRecordId) -> Self {
        match id {
            LogicalRecordId::TenantNameClaim { .. } => Self::TenantNameClaim,
            LogicalRecordId::BucketNameClaim { .. } => Self::BucketNameClaim,
            LogicalRecordId::TenantRecord { .. } => Self::TenantRecord,
            LogicalRecordId::BucketRecord { .. } => Self::BucketRecord,
            LogicalRecordId::BucketOptions { .. } => Self::BucketOptions,
            LogicalRecordId::BucketPolicy { .. } => Self::BucketPolicy,
            LogicalRecordId::Application { .. } => Self::Application,
            LogicalRecordId::Credential { .. } => Self::Credential,
            LogicalRecordId::TenantSchema { .. } => Self::TenantSchema,
        }
    }

    fn column_family(self) -> &'static str {
        match self {
            Self::TenantNameClaim | Self::BucketNameClaim => CF_NAMES,
            Self::TenantRecord | Self::BucketRecord => CF_METADATA,
            Self::BucketOptions => CF_BUCKET_OPTIONS,
            Self::BucketPolicy => CF_POLICIES,
            Self::Application | Self::Credential => CF_CREDENTIALS,
            Self::TenantSchema => CF_AUTHZ_SCHEMAS,
        }
    }

    fn prefix(self) -> Vec<u8> {
        match self {
            Self::TenantNameClaim | Self::TenantRecord => {
                vec![STORAGE_KEY_FORMAT_VERSION, TENANT_NAME_TYPE]
            }
            Self::BucketNameClaim | Self::BucketRecord => {
                vec![STORAGE_KEY_FORMAT_VERSION, BUCKET_NAME_TYPE]
            }
            Self::BucketOptions | Self::BucketPolicy => vec![STORAGE_KEY_FORMAT_VERSION],
            Self::Application => b"application\0".to_vec(),
            Self::Credential => b"client\0".to_vec(),
            Self::TenantSchema => vec![b'S'],
        }
    }

    fn matches(self, key: &[u8]) -> bool {
        if !key.starts_with(&self.prefix()) {
            return false;
        }
        match self {
            Self::TenantNameClaim => key.len() > 2,
            Self::BucketNameClaim => key.len() > 10,
            Self::TenantRecord => key.len() == 10,
            Self::BucketRecord => key.len() == 18,
            Self::BucketOptions | Self::BucketPolicy => key.len() == 17,
            Self::Application => key.len() > b"application\0".len(),
            Self::Credential => key.len() > b"client\0".len(),
            Self::TenantSchema => parse_schema_key(key).is_ok(),
        }
    }

    fn record_id(self, key: &[u8], value: &[u8]) -> Result<LogicalRecordId, LogicalRecordError> {
        let id = match self {
            Self::TenantNameClaim => LogicalRecordId::TenantNameClaim {
                storage_tenant: StorageTenantId::parse(utf8(&key[2..])?)
                    .map_err(|error| storage(error.to_string()))?,
            },
            Self::BucketNameClaim => LogicalRecordId::BucketNameClaim {
                tenant_id: read_u64(&key[2..10])?,
                bucket: utf8(&key[10..])?.to_owned(),
            },
            Self::TenantRecord => LogicalRecordId::TenantRecord {
                tenant_id: read_u64(&key[2..10])?,
            },
            Self::BucketRecord => LogicalRecordId::BucketRecord {
                tenant_id: read_u64(&key[2..10])?,
                bucket_id: read_u64(&key[10..18])?,
            },
            Self::BucketOptions => LogicalRecordId::BucketOptions {
                tenant_id: read_u64(&key[1..9])?,
                bucket_id: read_u64(&key[9..17])?,
            },
            Self::BucketPolicy => LogicalRecordId::BucketPolicy {
                tenant_id: read_u64(&key[1..9])?,
                bucket_id: read_u64(&key[9..17])?,
            },
            Self::Application => LogicalRecordId::Application {
                app_id: utf8(&key[b"application\0".len()..])?.to_owned(),
            },
            Self::Credential => LogicalRecordId::Credential {
                client_id: utf8(&key[b"client\0".len()..])?.to_owned(),
            },
            Self::TenantSchema => schema_record_id(key, value)?,
        };
        let location = id.location()?;
        if location.cf != self.column_family() || location.key != key {
            return Err(storage("logical record key is not canonical"));
        }
        Ok(id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CursorPosition {
    domain: ExportDomain,
    key: Vec<u8>,
}

impl Store {
    /// Enumerates only the closed logical-record authority. Object records,
    /// Zanzibar realm state, local journals, counters, and caches are outside
    /// this export and retain their own typed transfer protocols.
    pub fn export_logical_records(
        &self,
        cursor: Option<&LogicalRecordCursor>,
        max_records: u32,
        max_bytes: u64,
    ) -> Result<LogicalRecordExportPage, LogicalRecordError> {
        if max_records == 0
            || max_records > MAX_LOGICAL_RECORD_EXPORT_RECORDS
            || max_bytes == 0
            || max_bytes > MAX_LOGICAL_RECORD_EXPORT_BYTES
        {
            return Err(LogicalRecordError::InvalidExportLimit(format!(
                "records must be 1..={MAX_LOGICAL_RECORD_EXPORT_RECORDS} and bytes must be 1..={MAX_LOGICAL_RECORD_EXPORT_BYTES}"
            )));
        }
        let after = cursor.map(LogicalRecordCursor::position).transpose()?;
        let mut records = Vec::with_capacity(max_records as usize);
        let mut encoded_bytes = 0_u64;
        let mut last_position = None;
        let mut truncated = false;

        'domains: for domain in ExportDomain::ALL {
            if after
                .as_ref()
                .is_some_and(|position| position.domain > domain)
            {
                continue;
            }
            let start = after
                .as_ref()
                .filter(|position| position.domain == domain)
                .map_or_else(|| domain.prefix(), |position| position.key.clone());
            for item in self.db.iterator_cf(
                self.logical_record_cf(domain.column_family())?,
                IteratorMode::From(&start, Direction::Forward),
            ) {
                let (key, value) = item.map_err(storage)?;
                if !key.starts_with(&domain.prefix()) {
                    break;
                }
                if !domain.matches(&key) {
                    return Err(storage("logical record key has a malformed shape"));
                }
                if after.as_ref().is_some_and(|position| {
                    position.domain == domain && key.as_ref() <= position.key.as_slice()
                }) {
                    continue;
                }
                let id = domain.record_id(&key, &value)?;
                let record = LogicalRecordExport {
                    candidate: decode_candidate(&id, &value)?,
                    id,
                };
                record.validate()?;
                let record_bytes = u64::try_from(canonical_bytes(&record)?.len())
                    .map_err(|_| storage("logical record size overflow"))?;
                if record_bytes > MAX_LOGICAL_RECORD_EXPORT_BYTES {
                    return Err(LogicalRecordError::ExportRecordTooLarge {
                        required_bytes: record_bytes,
                    });
                }
                if records.len() == max_records as usize
                    || encoded_bytes.saturating_add(record_bytes) > max_bytes
                {
                    if records.is_empty() {
                        return Err(LogicalRecordError::ExportRecordTooLarge {
                            required_bytes: record_bytes,
                        });
                    }
                    truncated = true;
                    break 'domains;
                }
                encoded_bytes += record_bytes;
                last_position = Some(CursorPosition {
                    domain,
                    key: key.to_vec(),
                });
                records.push(record);
            }
        }
        let next_cursor = if truncated {
            last_position
                .as_ref()
                .map(LogicalRecordCursor::from_position)
        } else {
            None
        };
        Ok(LogicalRecordExportPage {
            records,
            next_cursor,
        })
    }

    /// Installs one record selected by quorum reconciliation during join or
    /// ownership handoff. It is not an ordinary mutation authority: the exact
    /// identity must be absent, while an existing exact candidate is a replay
    /// and any other existing value is a conflict.
    pub fn install_quorum_reconciled_logical_record(
        &self,
        record: &LogicalRecordExport,
    ) -> Result<LogicalRecordSnapshotApplied, LogicalRecordError> {
        record.validate()?;
        let location = record.id.location()?;
        let _guard = self
            .authz_write_lock
            .lock()
            .map_err(|_| storage("logical-record write lock is poisoned"))?;
        if let Some(existing) = self.logical_record_candidate(&record.id)? {
            if existing == record.candidate {
                return Ok(LogicalRecordSnapshotApplied {
                    record_version: candidate_version(&record.candidate),
                    replayed: true,
                });
            }
            return Err(LogicalRecordError::SnapshotConflict);
        }
        let (encoded, record_version) = match &record.candidate {
            LogicalRecordCandidate::Baseline { typed_value, .. } => {
                (encode_baseline(typed_value)?, None)
            }
            LogicalRecordCandidate::Versioned(mutation) => {
                (canonical_bytes(mutation)?, Some(mutation.record_version))
            }
        };
        self.write_logical_record(&location, encoded, record_version)?;
        Ok(LogicalRecordSnapshotApplied {
            record_version,
            replayed: false,
        })
    }

    /// Replaces one exact logical record with the candidate proved by a
    /// complete-record read quorum, or removes it when that quorum proved
    /// absence. This is a trusted repair boundary, not an ordinary mutation:
    /// callers must establish the exact quorum before invoking it.
    pub fn repair_quorum_reconciled_logical_record(
        &self,
        id: &LogicalRecordId,
        candidate: Option<&LogicalRecordCandidate>,
    ) -> Result<LogicalRecordSnapshotApplied, LogicalRecordError> {
        let location = id.location()?;
        if let Some(candidate) = candidate {
            LogicalRecordExport {
                id: id.clone(),
                candidate: candidate.clone(),
            }
            .validate()?;
        }
        let _guard = self
            .authz_write_lock
            .lock()
            .map_err(|_| storage("logical-record write lock is poisoned"))?;
        let current = self.logical_record_candidate(id)?;
        if current.as_ref() == candidate {
            return Ok(LogicalRecordSnapshotApplied {
                record_version: candidate.and_then(candidate_version),
                replayed: true,
            });
        }

        let Some(candidate) = candidate else {
            let mut batch = rocksdb::WriteBatch::default();
            batch.delete_cf(self.logical_record_cf(location.cf)?, &location.key);
            let mut options = rocksdb::WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage)?;
            return Ok(LogicalRecordSnapshotApplied {
                record_version: None,
                replayed: false,
            });
        };
        let (encoded, record_version) = match candidate {
            LogicalRecordCandidate::Baseline { typed_value, .. } => {
                (encode_baseline(typed_value)?, None)
            }
            LogicalRecordCandidate::Versioned(mutation) => {
                (canonical_bytes(mutation)?, Some(mutation.record_version))
            }
        };
        self.write_logical_record(&location, encoded, record_version)?;
        Ok(LogicalRecordSnapshotApplied {
            record_version,
            replayed: false,
        })
    }
}

fn candidate_version(candidate: &LogicalRecordCandidate) -> Option<VersionId> {
    match candidate {
        LogicalRecordCandidate::Baseline { .. } => None,
        LogicalRecordCandidate::Versioned(mutation) => Some(mutation.record_version),
    }
}

fn encode_baseline(value: &LogicalRecordValue) -> Result<Vec<u8>, LogicalRecordError> {
    value.validate()?;
    match value {
        LogicalRecordValue::TenantNameClaim { tenant_id, .. } => {
            Ok(tenant_id.to_be_bytes().to_vec())
        }
        LogicalRecordValue::BucketNameClaim { bucket_id, .. } => {
            Ok(bucket_id.to_be_bytes().to_vec())
        }
        LogicalRecordValue::TenantRecord(record) => {
            canonical_bytes(&StoredTenant::from(record.clone()))
        }
        LogicalRecordValue::BucketRecord(record) => {
            canonical_bytes(&StoredBucket::from(record.clone()))
        }
        LogicalRecordValue::BucketOptions { versioning, .. } => {
            Ok(encode_object_versioning(*versioning).to_vec())
        }
        LogicalRecordValue::BucketPolicy { policy, .. } => canonical_bytes(policy),
        LogicalRecordValue::Application(record) => {
            canonical_bytes(&StoredApplication::from(record.clone()))
        }
        LogicalRecordValue::Credential(record) => {
            canonical_bytes(&StoredApplicationCredential::from(record.clone()))
        }
        LogicalRecordValue::TenantSchema(record) => canonical_bytes(&StoredSchema {
            schema_ref: record.schema_ref.clone(),
            schema: record.schema.clone(),
            published_at_revision: record.published_at_revision,
        }),
    }
}

fn schema_record_id(key: &[u8], value: &[u8]) -> Result<LogicalRecordId, LogicalRecordError> {
    let (tenant, schema_id, revision) = parse_schema_key(key)?;
    let storage_tenant =
        StorageTenantId::parse(tenant).map_err(|error| storage(error.to_string()))?;
    if looks_like_envelope(value) {
        let mutation: LogicalRecordMutation =
            serde_json::from_slice(value).map_err(|_| LogicalRecordError::Tampered)?;
        mutation.validate()?;
        let id = mutation.typed_value.id();
        let LogicalRecordId::TenantSchema {
            storage_tenant: value_tenant,
            schema_ref,
        } = &id
        else {
            return Err(LogicalRecordError::Tampered);
        };
        if *value_tenant != storage_tenant
            || schema_ref.schema_id.as_str() != schema_id
            || schema_ref.schema_revision != revision
        {
            return Err(LogicalRecordError::Tampered);
        }
        return Ok(id);
    }
    let stored: StoredSchema = decode_json(value)?;
    if stored.schema_ref.schema_id.as_str() != schema_id
        || stored.schema_ref.schema_revision != revision
    {
        return Err(storage("persisted schema identity does not match its key"));
    }
    Ok(LogicalRecordId::TenantSchema {
        storage_tenant,
        schema_ref: stored.schema_ref,
    })
}

fn parse_schema_key(key: &[u8]) -> Result<(&str, &str, u64), LogicalRecordError> {
    if key.first() != Some(&b'S') {
        return Err(storage("schema key has the wrong type"));
    }
    let mut rest = &key[1..];
    let tenant = take_component(&mut rest)?;
    let schema = take_component(&mut rest)?;
    if rest.len() != 8 {
        return Err(storage("schema key has a malformed revision"));
    }
    Ok((utf8(tenant)?, utf8(schema)?, read_u64(rest)?))
}

fn take_component<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], LogicalRecordError> {
    if bytes.len() < 4 {
        return Err(storage("schema key has a truncated component length"));
    }
    let length = u32::from_be_bytes(bytes[..4].try_into().expect("checked length")) as usize;
    *bytes = &bytes[4..];
    if length == 0 || bytes.len() < length {
        return Err(storage("schema key has a malformed component"));
    }
    let (component, remaining) = bytes.split_at(length);
    *bytes = remaining;
    Ok(component)
}

fn read_u64(bytes: &[u8]) -> Result<u64, LogicalRecordError> {
    let encoded: [u8; 8] = bytes
        .try_into()
        .map_err(|_| storage("logical record key has a malformed stable ID"))?;
    let value = u64::from_be_bytes(encoded);
    require_nonzero(value, "stable identity")?;
    Ok(value)
}

fn utf8(bytes: &[u8]) -> Result<&str, LogicalRecordError> {
    std::str::from_utf8(bytes).map_err(storage)
}

#[cfg(test)]
mod tests;
