//! Complete-realm Zanzibar state transfer.
//!
//! One record is one complete `(storage_tenant, realm)` aggregate. The API
//! never exposes raw authorization column families or per-tuple replication.

use std::fmt;
use std::io::{Read, Seek, SeekFrom, Write};

use anvil_authz::{Authorization, RealmId, Tuple};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rocksdb::{Direction, IteratorMode, Snapshot, WriteBatch};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use super::replication::StoredRealmBinding;
use super::{
    AUTHZ_REALM_MUTATION_STAMP_FORMAT, AuthzRealmMutation, AuthzRealmMutationStamp,
    AuthzRealmSchema, AuthzRepository, AuthzRevision, AuthzScope, AuthzStoreError, RealmBinding,
    STORED_TUPLE_RECEIPT_FORMAT, StorageTenantId, StoredSchema, StoredTuple, StoredTupleReceipt,
    binding_key, current_unix_millis, decode_json, encode_json, receipt_key, receipt_record_bytes,
    schema_digest_key, schema_revision_key, storage_error, tenant_revision_key, tuple_key,
    tuple_prefix, validate_binding, validate_stored_schema, validate_stored_tuple_receipt_shape,
};
use crate::store::{
    CF_AUTHZ_BINDINGS, CF_AUTHZ_RECEIPTS, CF_AUTHZ_SCHEMAS, CF_AUTHZ_TENANTS, CF_AUTHZ_TUPLES,
};

pub const AUTHZ_REALM_SNAPSHOT_FORMAT: u16 = 1;
pub const AUTHZ_REALM_TRANSFER_MANIFEST_FORMAT: u16 = 1;
pub const MAX_AUTHZ_REALM_EXPORT_RECORDS: u32 = 1_000;
pub const MAX_AUTHZ_REALM_EXPORT_BYTES: u64 = 64 * 1024 * 1024;
const AUTHZ_REALM_CURSOR_FORMAT: u8 = 1;
const MAX_AUTHZ_REALM_CURSOR_KEY_BYTES: usize = 4 * 1024;
const AUTHZ_REALM_TRANSFER_HASH_DOMAIN: &[u8] = b"anvil.authz-realm-transfer.v1\0";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AuthzRealmCursor(String);

impl AuthzRealmCursor {
    pub fn from_token(token: impl Into<String>) -> Result<Self, AuthzRealmSnapshotError> {
        let cursor = Self(token.into());
        cursor.decode()?;
        Ok(cursor)
    }

    pub fn as_token(&self) -> &str {
        &self.0
    }

    fn from_key(key: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(key.len() + 1);
        bytes.push(AUTHZ_REALM_CURSOR_FORMAT);
        bytes.extend_from_slice(key);
        Self(URL_SAFE_NO_PAD.encode(bytes))
    }

    fn decode(&self) -> Result<Vec<u8>, AuthzRealmSnapshotError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(&self.0)
            .map_err(|_| AuthzRealmSnapshotError::InvalidCursor)?;
        if bytes.first() != Some(&AUTHZ_REALM_CURSOR_FORMAT)
            || bytes.len() <= 1
            || bytes.len() > MAX_AUTHZ_REALM_CURSOR_KEY_BYTES + 1
        {
            return Err(AuthzRealmSnapshotError::InvalidCursor);
        }
        let key = bytes[1..].to_vec();
        decode_binding_scope(&key).map_err(|_| AuthzRealmSnapshotError::InvalidCursor)?;
        Ok(key)
    }
}

impl fmt::Debug for AuthzRealmCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthzRealmCursor")
            .field("token", &"[OPAQUE]")
            .finish()
    }
}

/// One complete logical Zanzibar realm replica. Receipt mutations are the
/// live, typed retry guarantees for this realm; released untyped 0.5.0
/// receipts remain source-local until their bounded lifetime ends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzRealmAggregate {
    pub format: u16,
    pub scope: AuthzScope,
    pub revision: AuthzRevision,
    pub binding: RealmBinding,
    pub mutation_stamp: Option<AuthzRealmMutationStamp>,
    pub aggregate_revision: Option<AuthzRevision>,
    pub schema: AuthzRealmSchema,
    pub tuples: Vec<Tuple>,
    pub receipts: Vec<AuthzRealmMutation>,
}

impl AuthzRealmAggregate {
    pub fn validate(&self) -> Result<(), AuthzRealmSnapshotError> {
        self.validate_with_repository_limits(
            super::AuthzStoreLimits::default().evaluator,
            super::AuthzStoreLimits::default().max_mutations_per_batch,
        )
    }

    fn validate_with_repository_limits(
        &self,
        evaluator_limits: anvil_authz::AuthorizationLimits,
        max_mutations_per_batch: usize,
    ) -> Result<(), AuthzRealmSnapshotError> {
        if self.format != AUTHZ_REALM_SNAPSHOT_FORMAT || self.revision == AuthzRevision::ZERO {
            return Err(invalid_aggregate(
                "unsupported format or zero realm revision",
            ));
        }
        self.scope.validate().map_err(invalid_aggregate)?;
        validate_binding(&self.binding, &self.scope).map_err(invalid_aggregate)?;
        if self.binding.authz_revision > self.revision {
            return Err(invalid_aggregate(
                "binding revision is ahead of the realm revision",
            ));
        }
        validate_lineage(self)?;

        if self.schema.schema_ref != self.binding.schema_ref
            || self.schema.published_at_revision > self.revision
        {
            return Err(invalid_aggregate(
                "bound schema and realm revision disagree",
            ));
        }
        let stored_schema = StoredSchema {
            schema_ref: self.schema.schema_ref.clone(),
            schema: self.schema.schema.clone(),
            published_at_revision: self.schema.published_at_revision,
        };
        validate_stored_schema(&stored_schema, &self.binding.schema_ref, evaluator_limits)
            .map_err(invalid_aggregate)?;

        if self.tuples.len() != self.binding.tuple_count
            || self.tuples.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid_aggregate(
                "tuples must be sorted, unique, and match the binding count",
            ));
        }
        Authorization::new(
            self.scope.realm.clone(),
            self.schema.schema.clone(),
            self.tuples.iter().cloned(),
            evaluator_limits,
        )
        .map_err(invalid_aggregate)?;

        let mut previous_receipt_key = None;
        for mutation in &self.receipts {
            mutation.validate().map_err(invalid_aggregate)?;
            if mutation.scope != self.scope || mutation.revision() > self.revision {
                return Err(invalid_aggregate(
                    "receipt mutation belongs to another realm or future revision",
                ));
            }
            let super::AuthzRealmChange::MutateTuples {
                mutations, receipt, ..
            } = &mutation.change
            else {
                return Err(invalid_aggregate(
                    "only tuple mutation receipts belong in a realm snapshot",
                ));
            };
            if mutations.len() > max_mutations_per_batch {
                return Err(invalid_aggregate(
                    "receipt mutation exceeds the repository batch limit",
                ));
            }
            let key = receipt_key(
                &self.scope.storage_tenant,
                &receipt.principal,
                &mutation.command_id,
            )
            .map_err(invalid_aggregate)?;
            if previous_receipt_key
                .as_ref()
                .is_some_and(|previous: &Vec<u8>| previous >= &key)
            {
                return Err(invalid_aggregate(
                    "receipt mutations must be sorted and unique by receipt key",
                ));
            }
            previous_receipt_key = Some(key);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzRealmKeyPage {
    pub scopes: Vec<AuthzScope>,
    pub next_cursor: Option<AuthzRealmCursor>,
}

/// Integrity evidence emitted after one canonical complete-realm byte stream.
/// Streamed bytes have no authority until the whole manifest is verified and
/// the decoded aggregate is atomically installed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzRealmTransferManifest {
    pub format: u16,
    pub scope: AuthzScope,
    pub revision: AuthzRevision,
    pub predecessor_revision: Option<AuthzRevision>,
    pub mutation_fingerprint: Option<[u8; 32]>,
    pub encoded_bytes: u64,
    pub content_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthzRealmSnapshotApplied {
    pub revision: AuthzRevision,
    pub replayed: bool,
    pub retained_receipts: usize,
}

#[derive(Debug, Error)]
pub enum AuthzRealmSnapshotError {
    #[error("authorization realm export cursor is invalid")]
    InvalidCursor,
    #[error("authorization realm export limits are invalid: {0}")]
    InvalidExportLimit(String),
    #[error(
        "one authorization realm key requires {required_bytes} bytes, exceeding the page limit"
    )]
    ExportKeyTooLarge { required_bytes: u64 },
    #[error("invalid authorization realm aggregate: {0}")]
    InvalidAggregate(String),
    #[error("authorization realm aggregate conflicts with existing local state")]
    SnapshotConflict,
    #[error("authorization realm transfer failed integrity validation: {0}")]
    TransferIntegrity(String),
    #[error(transparent)]
    Store(#[from] AuthzStoreError),
}

impl AuthzRepository {
    /// Reads one complete realm candidate for a quorum read. Reconciliation
    /// policy deliberately remains outside the storage kernel.
    pub fn export_authz_realm(
        &self,
        scope: &AuthzScope,
    ) -> Result<Option<AuthzRealmAggregate>, AuthzRealmSnapshotError> {
        scope.validate().map_err(invalid_aggregate)?;
        let snapshot = self.db.snapshot();
        let now = current_unix_millis()?;
        self.read_realm_aggregate(&snapshot, scope, now)
    }

    /// Enumerates lightweight realm keys in deterministic binding-key order.
    /// Each selected key is then transferred as one complete byte stream.
    pub fn export_authz_realm_keys(
        &self,
        cursor: Option<&AuthzRealmCursor>,
        max_records: u32,
        max_bytes: u64,
    ) -> Result<AuthzRealmKeyPage, AuthzRealmSnapshotError> {
        validate_export_limits(max_records, max_bytes)?;
        let after = cursor.map(AuthzRealmCursor::decode).transpose()?;
        let snapshot = self.db.snapshot();
        let prefix = *b"B";
        let start = after.as_deref().unwrap_or(&prefix);
        let mut scopes = Vec::with_capacity(max_records as usize);
        let mut encoded_bytes = 0_u64;
        let mut last_key = None;

        for item in snapshot.iterator_cf(
            self.cf(CF_AUTHZ_BINDINGS)?,
            IteratorMode::From(start, Direction::Forward),
        ) {
            let (key, _) = item.map_err(storage_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if after
                .as_ref()
                .is_some_and(|after| key.as_ref() <= after.as_slice())
            {
                continue;
            }
            let scope = decode_binding_scope(&key)?;
            if !append_scope(
                &mut scopes,
                &mut encoded_bytes,
                max_records,
                max_bytes,
                &scope,
            )? {
                return Ok(AuthzRealmKeyPage {
                    scopes,
                    next_cursor: last_key.as_deref().map(AuthzRealmCursor::from_key),
                });
            }
            last_key = Some(key.to_vec());
        }
        Ok(AuthzRealmKeyPage {
            scopes,
            next_cursor: None,
        })
    }

    /// Writes the canonical encoding of one complete realm. Callers split
    /// these bytes into bounded transport frames and send the returned
    /// manifest after the final frame.
    pub fn export_authz_realm_stream<W: Write>(
        &self,
        scope: &AuthzScope,
        writer: W,
    ) -> Result<Option<AuthzRealmTransferManifest>, AuthzRealmSnapshotError> {
        let Some(aggregate) = self.export_authz_realm(scope)? else {
            return Ok(None);
        };
        let mut writer = HashedWriter::new(writer);
        serde_json::to_writer(&mut writer, &aggregate).map_err(storage_error)?;
        writer.flush().map_err(storage_error)?;
        let (encoded_bytes, content_hash) = writer.finish();
        Ok(Some(AuthzRealmTransferManifest {
            format: AUTHZ_REALM_TRANSFER_MANIFEST_FORMAT,
            scope: aggregate.scope,
            revision: aggregate.revision,
            predecessor_revision: aggregate
                .mutation_stamp
                .and_then(|stamp| stamp.predecessor_revision),
            mutation_fingerprint: aggregate
                .mutation_stamp
                .map(|stamp| stamp.mutation_fingerprint),
            encoded_bytes,
            content_hash,
        }))
    }

    /// Spools an untrusted complete-realm stream into an anonymous temporary
    /// file beside the store, verifies its exact length/domain hash and
    /// canonical encoding, then invokes the ordinary atomic aggregate install.
    /// The operating system removes the non-authoritative spool on every exit.
    pub fn install_quorum_reconciled_authz_realm_stream<R: Read>(
        &self,
        manifest: &AuthzRealmTransferManifest,
        mut reader: R,
    ) -> Result<AuthzRealmSnapshotApplied, AuthzRealmSnapshotError> {
        validate_transfer_manifest(manifest)?;
        let spool_directory = self.db.path().parent().ok_or_else(|| {
            AuthzRealmSnapshotError::Store(AuthzStoreError::Storage(
                "authorization store has no spool directory".into(),
            ))
        })?;
        let mut spool = tempfile::tempfile_in(spool_directory).map_err(storage_error)?;
        let observed_hash = copy_exact_transfer(&mut reader, &mut spool, manifest.encoded_bytes)?;
        if observed_hash != manifest.content_hash {
            return Err(transfer_integrity("content hash does not match manifest"));
        }
        spool.seek(SeekFrom::Start(0)).map_err(storage_error)?;
        let mut deserializer = serde_json::Deserializer::from_reader(&mut spool);
        let aggregate =
            AuthzRealmAggregate::deserialize(&mut deserializer).map_err(storage_error)?;
        deserializer.end().map_err(storage_error)?;
        if aggregate.scope != manifest.scope || aggregate.revision != manifest.revision {
            return Err(transfer_integrity(
                "decoded scope or revision does not match manifest",
            ));
        }
        let canonical = canonical_manifest(&aggregate)?;
        if canonical != *manifest {
            return Err(transfer_integrity(
                "stream is not the canonical aggregate encoding",
            ));
        }
        self.install_quorum_reconciled_authz_realm(&aggregate)
    }

    /// Installs one complete realm after the caller has already proved it as
    /// the exact replica-quorum winner. This method does not choose winners.
    pub fn install_quorum_reconciled_authz_realm(
        &self,
        aggregate: &AuthzRealmAggregate,
    ) -> Result<AuthzRealmSnapshotApplied, AuthzRealmSnapshotError> {
        self.install_quorum_reconciled_authz_realm_candidate(&aggregate.scope, Some(aggregate))
    }

    /// Atomically installs the exact winner already proven by an external
    /// replica quorum. This is deliberately not a general overwrite API: the
    /// storage kernel chooses no winner and accepts no quorum evidence.
    /// `None` removes a minority realm that lost to a quorum of absence.
    pub fn install_quorum_reconciled_authz_realm_candidate(
        &self,
        scope: &AuthzScope,
        aggregate: Option<&AuthzRealmAggregate>,
    ) -> Result<AuthzRealmSnapshotApplied, AuthzRealmSnapshotError> {
        scope.validate().map_err(invalid_aggregate)?;
        if aggregate.is_some_and(|aggregate| aggregate.scope != *scope) {
            return Err(invalid_aggregate(
                "quorum winner belongs to another authorization realm",
            ));
        }
        if let Some(aggregate) = aggregate {
            aggregate.validate_with_repository_limits(
                self.limits.evaluator,
                self.limits.max_mutations_per_batch,
            )?;
        }
        let _guard = self.lock_writes()?;
        let now = current_unix_millis()?;
        let mut incoming = aggregate.cloned();
        if let Some(incoming) = incoming.as_mut() {
            incoming
                .receipts
                .retain(|mutation| receipt_expiry(mutation) > now);
        }
        let snapshot = self.db.snapshot();
        let existing = self.read_realm_aggregate(&snapshot, scope, now)?;
        if existing == incoming {
            return Ok(AuthzRealmSnapshotApplied {
                revision: incoming
                    .as_ref()
                    .map_or(AuthzRevision::ZERO, |aggregate| aggregate.revision),
                replayed: true,
                retained_receipts: incoming
                    .as_ref()
                    .map_or(0, |aggregate| aggregate.receipts.len()),
            });
        }
        self.replace_realm(scope, incoming.as_ref(), now)?;
        Ok(AuthzRealmSnapshotApplied {
            revision: incoming
                .as_ref()
                .map_or(AuthzRevision::ZERO, |aggregate| aggregate.revision),
            replayed: false,
            retained_receipts: incoming
                .as_ref()
                .map_or(0, |aggregate| aggregate.receipts.len()),
        })
    }

    fn read_realm_aggregate(
        &self,
        snapshot: &Snapshot<'_>,
        scope: &AuthzScope,
        now: u64,
    ) -> Result<Option<AuthzRealmAggregate>, AuthzRealmSnapshotError> {
        let Some(stored_binding) = snapshot_json::<StoredRealmBinding>(
            snapshot,
            self.cf(CF_AUTHZ_BINDINGS)?,
            &binding_key(scope),
        )?
        else {
            return Ok(None);
        };
        validate_binding(&stored_binding.binding, scope).map_err(invalid_aggregate)?;
        let revision = match (
            stored_binding.aggregate_revision,
            stored_binding.mutation_stamp,
        ) {
            (Some(revision), Some(_)) if revision != AuthzRevision::ZERO => revision,
            (None, None) => snapshot_json::<AuthzRevision>(
                snapshot,
                self.cf(CF_AUTHZ_TENANTS)?,
                &tenant_revision_key(&scope.storage_tenant),
            )?
            .ok_or_else(|| {
                AuthzRealmSnapshotError::Store(AuthzStoreError::Storage(
                    "authorization realm has no tenant revision".into(),
                ))
            })?,
            _ => {
                return Err(invalid_aggregate(
                    "authorization realm lineage is inconsistent",
                ));
            }
        };
        let stored_schema = snapshot_json::<StoredSchema>(
            snapshot,
            self.cf(CF_AUTHZ_SCHEMAS)?,
            &schema_revision_key(&scope.storage_tenant, &stored_binding.binding.schema_ref),
        )?
        .ok_or_else(|| {
            AuthzRealmSnapshotError::Store(AuthzStoreError::SchemaNotFound(
                stored_binding.binding.schema_ref.schema_id.clone(),
                stored_binding.binding.schema_ref.schema_revision,
            ))
        })?;

        let tuple_key_prefix = tuple_prefix(scope);
        let mut tuples = Vec::new();
        for item in snapshot.iterator_cf(
            self.cf(CF_AUTHZ_TUPLES)?,
            IteratorMode::From(&tuple_key_prefix, Direction::Forward),
        ) {
            let (key, encoded) = item.map_err(storage_error)?;
            if !key.starts_with(&tuple_key_prefix) {
                break;
            }
            let tuple = decode_json::<StoredTuple>(&encoded)?.tuple;
            if tuple_key(scope, &tuple)?.as_slice() != key.as_ref() {
                return Err(AuthzRealmSnapshotError::Store(AuthzStoreError::Storage(
                    "authorization tuple key is inconsistent".into(),
                )));
            }
            tuples.push(tuple);
        }
        tuples.sort();

        let mut keyed_receipts = Vec::new();
        for item in snapshot.iterator_cf(self.cf(CF_AUTHZ_RECEIPTS)?, IteratorMode::Start) {
            let (key, encoded) = item.map_err(storage_error)?;
            let stored = decode_json::<StoredTupleReceipt>(&encoded)?;
            validate_stored_tuple_receipt_shape(&stored).map_err(invalid_aggregate)?;
            let expected_key = receipt_key(
                &stored.receipt.scope.storage_tenant,
                &stored.receipt.principal,
                &stored.operation_id,
            )?;
            if key.as_ref() != expected_key.as_slice() {
                return Err(AuthzRealmSnapshotError::Store(AuthzStoreError::Storage(
                    "authorization receipt key is inconsistent".into(),
                )));
            }
            if stored.expires_at_unix_millis <= now || stored.receipt.scope != *scope {
                continue;
            }
            let Some(mutation) = stored.realm_mutation.clone() else {
                continue;
            };
            validate_stored_receipt_mutation(&stored, &mutation)?;
            keyed_receipts.push((key.to_vec(), mutation));
        }
        keyed_receipts.sort_by(|left, right| left.0.cmp(&right.0));

        let aggregate = AuthzRealmAggregate {
            format: AUTHZ_REALM_SNAPSHOT_FORMAT,
            scope: scope.clone(),
            revision,
            binding: stored_binding.binding,
            mutation_stamp: stored_binding.mutation_stamp,
            aggregate_revision: stored_binding.aggregate_revision,
            schema: AuthzRealmSchema {
                schema_ref: stored_schema.schema_ref,
                schema: stored_schema.schema,
                published_at_revision: stored_schema.published_at_revision,
            },
            tuples,
            receipts: keyed_receipts
                .into_iter()
                .map(|(_, mutation)| mutation)
                .collect(),
        };
        aggregate.validate_with_repository_limits(
            self.limits.evaluator,
            self.limits.max_mutations_per_batch,
        )?;
        Ok(Some(aggregate))
    }

    fn replace_realm(
        &self,
        scope: &AuthzScope,
        aggregate: Option<&AuthzRealmAggregate>,
        now: u64,
    ) -> Result<(), AuthzRealmSnapshotError> {
        let schema = aggregate.map(|aggregate| {
            let stored = StoredSchema {
                schema_ref: aggregate.schema.schema_ref.clone(),
                schema: aggregate.schema.schema.clone(),
                published_at_revision: aggregate.schema.published_at_revision,
            };
            let revision_key = schema_revision_key(&scope.storage_tenant, &stored.schema_ref);
            let digest_key = schema_digest_key(
                &scope.storage_tenant,
                &stored.schema_ref.schema_id,
                stored.schema_ref.schema_digest,
            );
            (stored, revision_key, digest_key)
        });
        if let Some((stored, revision_key, digest_key)) = schema.as_ref() {
            if self
                .read_json::<StoredSchema>(CF_AUTHZ_SCHEMAS, revision_key)?
                .is_some_and(|existing| existing != *stored)
                || self
                    .read_json::<super::SchemaRef>(CF_AUTHZ_SCHEMAS, digest_key)?
                    .is_some_and(|existing| existing != stored.schema_ref)
            {
                return Err(AuthzRealmSnapshotError::SnapshotConflict);
            }
        }

        let mut encoded_receipts =
            Vec::with_capacity(aggregate.map_or(0, |aggregate| aggregate.receipts.len()));
        for mutation in aggregate
            .into_iter()
            .flat_map(|aggregate| &aggregate.receipts)
        {
            let (key, stored) = stored_receipt_from_mutation(mutation)?;
            let encoded = encode_json(&stored)?;
            encoded_receipts.push((key, encoded));
        }
        let mut retained_entries = 0_usize;
        let mut retained_bytes = 0_u64;
        let mut receipt_deletions = Vec::new();
        for item in self
            .db
            .iterator_cf(self.cf(CF_AUTHZ_RECEIPTS)?, IteratorMode::Start)
        {
            let (key, encoded) = item.map_err(storage_error)?;
            let stored = decode_json::<StoredTupleReceipt>(&encoded)?;
            validate_stored_tuple_receipt_shape(&stored).map_err(invalid_aggregate)?;
            let expected_key = receipt_key(
                &stored.receipt.scope.storage_tenant,
                &stored.receipt.principal,
                &stored.operation_id,
            )?;
            if key.as_ref() != expected_key.as_slice() {
                return Err(AuthzRealmSnapshotError::Store(AuthzStoreError::Storage(
                    "authorization receipt key is inconsistent".into(),
                )));
            }
            if stored.expires_at_unix_millis <= now || stored.receipt.scope == *scope {
                receipt_deletions.push(key.to_vec());
                continue;
            }
            retained_entries = retained_entries
                .checked_add(1)
                .ok_or(AuthzStoreError::ReceiptCapacity)?;
            retained_bytes = retained_bytes
                .checked_add(receipt_record_bytes(&key, &encoded)?)
                .ok_or(AuthzStoreError::ReceiptCapacity)?;
        }
        let next_entries = retained_entries
            .checked_add(encoded_receipts.len())
            .ok_or(AuthzStoreError::ReceiptCapacity)?;
        let incoming_bytes = encoded_receipts
            .iter()
            .try_fold(0_u64, |total, (key, value)| {
                total
                    .checked_add(receipt_record_bytes(key, value)?)
                    .ok_or(AuthzStoreError::ReceiptCapacity)
            })?;
        let next_bytes = retained_bytes
            .checked_add(incoming_bytes)
            .ok_or(AuthzStoreError::ReceiptCapacity)?;
        if next_entries > self.limits.max_receipt_entries
            || next_bytes > self.limits.max_receipt_bytes
        {
            return Err(AuthzStoreError::ReceiptCapacity.into());
        }

        let mut batch = WriteBatch::default();
        batch.delete_cf(self.cf(CF_AUTHZ_BINDINGS)?, binding_key(scope));
        let tuple_prefix = tuple_prefix(scope);
        for item in self.db.iterator_cf(
            self.cf(CF_AUTHZ_TUPLES)?,
            IteratorMode::From(&tuple_prefix, Direction::Forward),
        ) {
            let (key, _) = item.map_err(storage_error)?;
            if !key.starts_with(&tuple_prefix) {
                break;
            }
            batch.delete_cf(self.cf(CF_AUTHZ_TUPLES)?, key);
        }
        for key in receipt_deletions {
            batch.delete_cf(self.cf(CF_AUTHZ_RECEIPTS)?, key);
        }
        let Some(aggregate) = aggregate else {
            self.write(batch)?;
            return Ok(());
        };
        let (stored_schema, schema_key, digest_key) =
            schema.expect("present aggregate has a prepared schema");
        if self
            .read_json::<StoredSchema>(CF_AUTHZ_SCHEMAS, &schema_key)?
            .is_none()
        {
            batch.put_cf(
                self.cf(CF_AUTHZ_SCHEMAS)?,
                schema_key,
                encode_json(&stored_schema)?,
            );
        }
        batch.put_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            digest_key,
            encode_json(&stored_schema.schema_ref)?,
        );
        batch.put_cf(
            self.cf(CF_AUTHZ_BINDINGS)?,
            binding_key(scope),
            encode_json(&StoredRealmBinding {
                binding: aggregate.binding.clone(),
                mutation_stamp: aggregate.mutation_stamp,
                aggregate_revision: aggregate.aggregate_revision,
            })?,
        );
        for tuple in &aggregate.tuples {
            batch.put_cf(
                self.cf(CF_AUTHZ_TUPLES)?,
                tuple_key(scope, tuple)?,
                encode_json(&StoredTuple {
                    tuple: tuple.clone(),
                })?,
            );
        }
        for (key, encoded) in encoded_receipts {
            batch.put_cf(self.cf(CF_AUTHZ_RECEIPTS)?, key, encoded);
        }
        let local_revision = self.tenant_revision(&scope.storage_tenant)?;
        if aggregate.revision > local_revision {
            self.stage_tenant_revision(&mut batch, &scope.storage_tenant, aggregate.revision)?;
        }
        self.write(batch)?;
        Ok(())
    }
}

fn validate_lineage(aggregate: &AuthzRealmAggregate) -> Result<(), AuthzRealmSnapshotError> {
    match (aggregate.mutation_stamp, aggregate.aggregate_revision) {
        (None, None) => Ok(()),
        (Some(stamp), Some(revision))
            if stamp.format == AUTHZ_REALM_MUTATION_STAMP_FORMAT
                && revision == aggregate.revision
                && revision != AuthzRevision::ZERO
                && stamp.predecessor_revision.is_none_or(|predecessor| {
                    predecessor != AuthzRevision::ZERO && predecessor < revision
                })
                && stamp.serving_fence_term != 0
                && stamp.source_id.node_id != 0
                && stamp.source_id.source_epoch != [0; 32]
                && stamp.source_journal_position != 0 =>
        {
            Ok(())
        }
        _ => Err(invalid_aggregate("realm lineage is inconsistent")),
    }
}

fn validate_export_limits(max_records: u32, max_bytes: u64) -> Result<(), AuthzRealmSnapshotError> {
    if max_records == 0
        || max_records > MAX_AUTHZ_REALM_EXPORT_RECORDS
        || max_bytes == 0
        || max_bytes > MAX_AUTHZ_REALM_EXPORT_BYTES
    {
        return Err(AuthzRealmSnapshotError::InvalidExportLimit(format!(
            "records must be 1..={MAX_AUTHZ_REALM_EXPORT_RECORDS} and bytes must be 1..={MAX_AUTHZ_REALM_EXPORT_BYTES}"
        )));
    }
    Ok(())
}

fn append_scope(
    scopes: &mut Vec<AuthzScope>,
    encoded_bytes: &mut u64,
    max_records: u32,
    max_bytes: u64,
    scope: &AuthzScope,
) -> Result<bool, AuthzRealmSnapshotError> {
    let required_bytes = u64::try_from(encode_json(scope)?.len()).map_err(|_| {
        AuthzRealmSnapshotError::Store(AuthzStoreError::Storage(
            "authorization realm key size overflow".into(),
        ))
    })?;
    if required_bytes > MAX_AUTHZ_REALM_EXPORT_BYTES
        || (scopes.is_empty() && required_bytes > max_bytes)
    {
        return Err(AuthzRealmSnapshotError::ExportKeyTooLarge { required_bytes });
    }
    if scopes.len() == max_records as usize
        || encoded_bytes.saturating_add(required_bytes) > max_bytes
    {
        return Ok(false);
    }
    *encoded_bytes += required_bytes;
    scopes.push(scope.clone());
    Ok(true)
}

struct HashedWriter<W> {
    inner: W,
    hasher: blake3::Hasher,
    encoded_bytes: u64,
}

impl<W> HashedWriter<W> {
    fn new(inner: W) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(AUTHZ_REALM_TRANSFER_HASH_DOMAIN);
        Self {
            inner,
            hasher,
            encoded_bytes: 0,
        }
    }

    fn finish(self) -> (u64, [u8; 32]) {
        (self.encoded_bytes, *self.hasher.finalize().as_bytes())
    }
}

impl<W: Write> Write for HashedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("authorization transfer size overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn canonical_manifest(
    aggregate: &AuthzRealmAggregate,
) -> Result<AuthzRealmTransferManifest, AuthzRealmSnapshotError> {
    let mut writer = HashedWriter::new(std::io::sink());
    serde_json::to_writer(&mut writer, aggregate).map_err(storage_error)?;
    let (encoded_bytes, content_hash) = writer.finish();
    Ok(AuthzRealmTransferManifest {
        format: AUTHZ_REALM_TRANSFER_MANIFEST_FORMAT,
        scope: aggregate.scope.clone(),
        revision: aggregate.revision,
        predecessor_revision: aggregate
            .mutation_stamp
            .and_then(|stamp| stamp.predecessor_revision),
        mutation_fingerprint: aggregate
            .mutation_stamp
            .map(|stamp| stamp.mutation_fingerprint),
        encoded_bytes,
        content_hash,
    })
}

fn validate_transfer_manifest(
    manifest: &AuthzRealmTransferManifest,
) -> Result<(), AuthzRealmSnapshotError> {
    if manifest.format != AUTHZ_REALM_TRANSFER_MANIFEST_FORMAT
        || manifest.revision == AuthzRevision::ZERO
        || manifest.encoded_bytes == 0
    {
        return Err(transfer_integrity(
            "unsupported format, zero revision, or empty stream",
        ));
    }
    match (manifest.predecessor_revision, manifest.mutation_fingerprint) {
        (None, None) => {}
        (predecessor, Some(fingerprint))
            if fingerprint != [0; 32]
                && predecessor.is_none_or(|revision| {
                    revision != AuthzRevision::ZERO && revision < manifest.revision
                }) => {}
        _ => {
            return Err(transfer_integrity(
                "manifest has inconsistent realm lineage",
            ));
        }
    }
    manifest.scope.validate().map_err(transfer_integrity)
}

fn copy_exact_transfer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    encoded_bytes: u64,
) -> Result<[u8; 32], AuthzRealmSnapshotError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(AUTHZ_REALM_TRANSFER_HASH_DOMAIN);
    let mut remaining = encoded_bytes;
    let mut buffer = [0_u8; 64 * 1024];
    while remaining != 0 {
        let limit =
            usize::try_from(remaining.min(buffer.len() as u64)).map_err(transfer_integrity)?;
        let read = reader.read(&mut buffer[..limit]).map_err(storage_error)?;
        if read == 0 {
            return Err(transfer_integrity("stream ended before manifest length"));
        }
        writer.write_all(&buffer[..read]).map_err(storage_error)?;
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    let mut trailing = [0_u8; 1];
    if reader.read(&mut trailing).map_err(storage_error)? != 0 {
        return Err(transfer_integrity("stream exceeds manifest length"));
    }
    writer.flush().map_err(storage_error)?;
    Ok(*hasher.finalize().as_bytes())
}

fn snapshot_json<T: DeserializeOwned>(
    snapshot: &Snapshot<'_>,
    cf: &rocksdb::ColumnFamily,
    key: &[u8],
) -> Result<Option<T>, AuthzRealmSnapshotError> {
    snapshot
        .get_cf(cf, key)
        .map_err(storage_error)?
        .map(|encoded| decode_json(&encoded))
        .transpose()
        .map_err(Into::into)
}

fn decode_binding_scope(key: &[u8]) -> Result<AuthzScope, AuthzRealmSnapshotError> {
    if key.first() != Some(&b'B') {
        return Err(invalid_aggregate("authorization binding key is malformed"));
    }
    let mut offset = 1;
    let tenant = read_component(key, &mut offset)?;
    let realm = read_component(key, &mut offset)?;
    if offset != key.len() {
        return Err(invalid_aggregate("authorization binding key is malformed"));
    }
    let scope = AuthzScope::new(
        StorageTenantId::parse(String::from_utf8(tenant.to_vec()).map_err(invalid_aggregate)?)?,
        RealmId::parse(String::from_utf8(realm.to_vec()).map_err(invalid_aggregate)?)
            .map_err(invalid_aggregate)?,
    )?;
    if binding_key(&scope) != key {
        return Err(invalid_aggregate(
            "authorization binding key is not canonical",
        ));
    }
    Ok(scope)
}

fn read_component<'a>(
    key: &'a [u8],
    offset: &mut usize,
) -> Result<&'a [u8], AuthzRealmSnapshotError> {
    let end_length = offset
        .checked_add(4)
        .ok_or_else(|| invalid_aggregate("authorization binding key is malformed"))?;
    let length_bytes: [u8; 4] = key
        .get(*offset..end_length)
        .ok_or_else(|| invalid_aggregate("authorization binding key is malformed"))?
        .try_into()
        .map_err(invalid_aggregate)?;
    let length = usize::try_from(u32::from_be_bytes(length_bytes)).map_err(invalid_aggregate)?;
    let end = end_length
        .checked_add(length)
        .ok_or_else(|| invalid_aggregate("authorization binding key is malformed"))?;
    let component = key
        .get(end_length..end)
        .ok_or_else(|| invalid_aggregate("authorization binding key is malformed"))?;
    *offset = end;
    Ok(component)
}

fn validate_stored_receipt_mutation(
    stored: &StoredTupleReceipt,
    mutation: &AuthzRealmMutation,
) -> Result<(), AuthzRealmSnapshotError> {
    let super::AuthzRealmChange::MutateTuples {
        receipt,
        receipt_created_at_unix_millis,
        ..
    } = &mutation.change
    else {
        return Err(invalid_aggregate(
            "authorization receipt contains a non-tuple mutation",
        ));
    };
    if mutation.command_id != stored.operation_id
        || mutation.input_fingerprint != stored.fingerprint
        || mutation.scope != stored.receipt.scope
        || receipt != &stored.receipt
        || *receipt_created_at_unix_millis != stored.created_at_unix_millis
        || receipt.replay_guarantee_expires_at_unix_millis != stored.expires_at_unix_millis
        || stored.realm_mutation.as_ref() != Some(mutation)
    {
        return Err(invalid_aggregate(
            "authorization receipt disagrees with its typed mutation",
        ));
    }
    mutation.validate().map_err(invalid_aggregate)
}

fn stored_receipt_from_mutation(
    mutation: &AuthzRealmMutation,
) -> Result<(Vec<u8>, StoredTupleReceipt), AuthzRealmSnapshotError> {
    let super::AuthzRealmChange::MutateTuples {
        receipt,
        receipt_created_at_unix_millis,
        ..
    } = &mutation.change
    else {
        return Err(invalid_aggregate(
            "authorization receipt contains a non-tuple mutation",
        ));
    };
    let key = receipt_key(
        &mutation.scope.storage_tenant,
        &receipt.principal,
        &mutation.command_id,
    )?;
    Ok((
        key,
        StoredTupleReceipt {
            format: STORED_TUPLE_RECEIPT_FORMAT,
            operation_id: mutation.command_id.clone(),
            created_at_unix_millis: *receipt_created_at_unix_millis,
            expires_at_unix_millis: receipt.replay_guarantee_expires_at_unix_millis,
            fingerprint: mutation.input_fingerprint,
            receipt: receipt.clone(),
            realm_mutation: Some(mutation.clone()),
        },
    ))
}

fn receipt_expiry(mutation: &AuthzRealmMutation) -> u64 {
    match &mutation.change {
        super::AuthzRealmChange::MutateTuples { receipt, .. } => {
            receipt.replay_guarantee_expires_at_unix_millis
        }
        super::AuthzRealmChange::BindSchema { .. } => 0,
    }
}

fn invalid_aggregate(error: impl fmt::Display) -> AuthzRealmSnapshotError {
    AuthzRealmSnapshotError::InvalidAggregate(error.to_string())
}

fn transfer_integrity(error: impl fmt::Display) -> AuthzRealmSnapshotError {
    AuthzRealmSnapshotError::TransferIntegrity(error.to_string())
}

#[cfg(test)]
mod tests;
