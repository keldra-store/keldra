//! Bounded baseline reads for one input-ordered object mutation batch.
//!
//! Snapshot-backed caches may be populated before the store commit fence, but
//! every exact raw observation must then be revalidated under that fence. The
//! cache is never authoritative: mutations still enter the existing pending
//! maps in input order and share the existing final `WriteBatch`.

use std::cell::RefCell;

use rocksdb::{DB, SnapshotWithThreadMode};

use super::object_alias_registry::decode_registry;
use super::receipt_codec::decode_stored_receipt;
use super::*;
use crate::ObjectAliasRegistry;

const PREFETCH_KEYS_PER_MULTI_GET: usize = 256;

type Cached<T> = Result<Option<T>, MutationError>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObservedMutationCell {
    cf_name: &'static str,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

/// Exact raw values selected by one coherent mutation-prefetch snapshot.
#[derive(Clone, Debug, Default)]
pub(super) struct MutationReadToken {
    cells: Vec<ObservedMutationCell>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct MutationReadRevalidation {
    pub(super) unchanged: bool,
    pub(super) checked_keys: usize,
}

enum MutationReadSource<'a> {
    Current,
    Snapshot(SnapshotWithThreadMode<'a, DB>),
}

/// A current or snapshot-backed RocksDB view for one complete bulk prefetch.
struct MutationReadView<'a> {
    store: &'a Store,
    source: MutationReadSource<'a>,
    observations: RefCell<BTreeMap<(&'static str, Vec<u8>), Option<Vec<u8>>>>,
}

impl<'a> MutationReadView<'a> {
    fn current(store: &'a Store) -> Self {
        Self {
            store,
            source: MutationReadSource::Current,
            observations: RefCell::new(BTreeMap::new()),
        }
    }

    fn snapshot(store: &'a Store) -> Self {
        Self {
            store,
            source: MutationReadSource::Snapshot(store.db.snapshot()),
            observations: RefCell::new(BTreeMap::new()),
        }
    }

    fn token(&self) -> MutationReadToken {
        MutationReadToken {
            cells: self
                .observations
                .borrow()
                .iter()
                .map(|((cf_name, key), value)| ObservedMutationCell {
                    cf_name,
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }

    fn multi_get_raw(
        &self,
        cf_name: &'static str,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<BTreeMap<Vec<u8>, Cached<Vec<u8>>>, MutationError> {
        let cf = self.store.cf(cf_name)?;
        let mut values = BTreeMap::new();
        let ordered = keys.iter().collect::<Vec<_>>();
        for keys in ordered.chunks(PREFETCH_KEYS_PER_MULTI_GET) {
            let fetched = match &self.source {
                MutationReadSource::Current => self
                    .store
                    .db
                    .multi_get_cf(keys.iter().map(|key| (cf, key.as_slice()))),
                MutationReadSource::Snapshot(snapshot) => {
                    snapshot.multi_get_cf(keys.iter().map(|key| (cf, key.as_slice())))
                }
            };
            if fetched.len() != keys.len() {
                return Err(MutationError::Storage(format!(
                    "{cf_name} bulk prefetch returned the wrong result count"
                )));
            }
            for (key, value) in keys.iter().zip(fetched) {
                let value = value
                    .map(|encoded| encoded.map(|bytes| bytes.to_vec()))
                    .map_err(storage_error)?;
                if matches!(&self.source, MutationReadSource::Snapshot(_)) {
                    self.observations
                        .borrow_mut()
                        .insert((cf_name, (*key).clone()), value.clone());
                }
                values.insert((*key).clone(), Ok(value));
            }
        }
        Ok(values)
    }
}

#[derive(Default)]
pub(super) struct MutationReadCache {
    heads: BTreeMap<Vec<u8>, Cached<Head>>,
    stored_versions: BTreeMap<Vec<u8>, Cached<StoredVersion>>,
    receipts: BTreeMap<Vec<u8>, Cached<StoredReceipt>>,
    blob_references: BTreeMap<Vec<u8>, Cached<BlobReferenceState>>,
    inline_payloads: BTreeMap<Vec<u8>, Cached<Vec<u8>>>,
    alias_registries: BTreeMap<Vec<u8>, Cached<ObjectAliasRegistry>>,
    policies: BTreeMap<Vec<u8>, Result<BucketPolicy, MutationError>>,
    versioning: BTreeMap<Vec<u8>, Result<ObjectVersioning, MutationError>>,
}

#[derive(Default)]
struct PrefetchMetrics {
    head_keys: u64,
    version_keys: u64,
    receipt_keys: u64,
    blob_reference_keys: u64,
    inline_payload_keys: u64,
    alias_registry_keys: u64,
    policy_keys: u64,
    versioning_keys: u64,
    head_seconds: f64,
    version_seconds: f64,
    receipt_seconds: f64,
    blob_reference_seconds: f64,
    inline_payload_seconds: f64,
    alias_registry_seconds: f64,
    policy_seconds: f64,
    versioning_seconds: f64,
}

impl MutationReadCache {
    pub(super) fn load(
        store: &Store,
        operations: &[&PreparedOperation],
    ) -> Result<Self, MutationError> {
        Self::load_from(&MutationReadView::current(store), operations)
    }

    pub(super) fn load_snapshot(
        store: &Store,
        operations: &[&PreparedOperation],
    ) -> Result<(Self, MutationReadToken), MutationError> {
        let view = MutationReadView::snapshot(store);
        let cache = Self::load_from(&view, operations)?;
        Ok((cache, view.token()))
    }

    fn load_from(
        view: &MutationReadView<'_>,
        operations: &[&PreparedOperation],
    ) -> Result<Self, MutationError> {
        let mut metrics = PrefetchMetrics::default();
        let head_keys = operations
            .iter()
            .map(|operation| operation.encoded_head_key())
            .collect::<BTreeSet<_>>();
        let receipt_keys = operations
            .iter()
            .filter_map(|operation| {
                operation
                    .command_id()
                    .map(|command_id| receipt_key(operation.identity(), command_id))
            })
            .collect::<BTreeSet<_>>();
        let bucket_keys = operations
            .iter()
            .map(|operation| operation.identity().encode().to_vec())
            .collect::<BTreeSet<_>>();

        let (heads, elapsed) = multi_get_json::<Head>(view, CF_HEADS, &head_keys)?;
        metrics.head_keys = head_keys.len() as u64;
        metrics.head_seconds = elapsed;

        let started = std::time::Instant::now();
        let alias_registries = view
            .multi_get_raw(CF_OBJECT_ALIAS_REGISTRIES, &head_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = cached
                    .and_then(|value| value.map(|encoded| decode_registry(&encoded)).transpose());
                (key, decoded)
            })
            .collect();
        metrics.alias_registry_keys = head_keys.len() as u64;
        metrics.alias_registry_seconds = started.elapsed().as_secs_f64();

        let mut version_key_by_head = BTreeMap::new();
        let mut version_keys = BTreeSet::new();
        for (head_key, cached) in &heads {
            if let Ok(Some(head)) = cached {
                let key = exact_version_key(head_key, head.version);
                version_keys.insert(key.clone());
                version_key_by_head.insert(head_key.clone(), key);
            }
        }
        let started = std::time::Instant::now();
        let versions_by_key = view
            .multi_get_raw(CF_VERSIONS, &version_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = cached.and_then(|value| {
                    value
                        .map(|encoded| StoredVersion::decode(&encoded))
                        .transpose()
                });
                (key, decoded)
            })
            .collect::<BTreeMap<_, _>>();
        let elapsed = started.elapsed();
        metrics.version_keys = version_keys.len() as u64;
        metrics.version_seconds = elapsed.as_secs_f64();
        let stored_versions: BTreeMap<Vec<u8>, Cached<StoredVersion>> = version_key_by_head
            .into_iter()
            .map(|(head_key, version_key)| {
                let cached = versions_by_key.get(&version_key).cloned().ok_or_else(|| {
                    MutationError::Storage("bulk version prefetch omitted a requested key".into())
                });
                (head_key, cached.and_then(|value| value))
            })
            .collect();

        let started = std::time::Instant::now();
        let receipts = view
            .multi_get_raw(CF_RECEIPTS, &receipt_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = cached.and_then(|value| {
                    value
                        .map(|encoded| decode_stored_receipt(&encoded))
                        .transpose()
                });
                (key, decoded)
            })
            .collect();
        let elapsed = started.elapsed().as_secs_f64();
        metrics.receipt_keys = receipt_keys.len() as u64;
        metrics.receipt_seconds = elapsed;

        let started = std::time::Instant::now();
        let policies = view
            .multi_get_raw(CF_POLICIES, &bucket_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = decode_bucket_policy(&key, cached);
                (key, decoded)
            })
            .collect();
        metrics.policy_keys = bucket_keys.len() as u64;
        metrics.policy_seconds = started.elapsed().as_secs_f64();
        let started = std::time::Instant::now();
        let versioning = view
            .multi_get_raw(CF_BUCKET_OPTIONS, &bucket_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = decode_bucket_versioning(&key, cached);
                (key, decoded)
            })
            .collect();
        metrics.versioning_keys = bucket_keys.len() as u64;
        metrics.versioning_seconds = started.elapsed().as_secs_f64();

        let mut blob_reference_keys = BTreeSet::new();
        let mut inline_payload_keys = BTreeSet::new();
        for operation in operations {
            if let Some(reference) = operation.payload_reference() {
                let key = blob_reference_key(reference);
                blob_reference_keys.insert(key.clone());
                if is_inline_payload_artifact(reference) {
                    inline_payload_keys.insert(complete_artifact_key(reference));
                }
            }
        }
        for cached in stored_versions.values() {
            if let Ok(Some(stored)) = cached
                && let Some(reference) = stored.version.blob.as_ref()
            {
                blob_reference_keys.insert(blob_reference_key(reference));
            }
        }

        let started = std::time::Instant::now();
        let blob_references = view
            .multi_get_raw(CF_BLOB_REFERENCES, &blob_reference_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = cached.and_then(|value| {
                    value
                        .map(|encoded| decode_blob_reference_state(&encoded))
                        .transpose()
                });
                (key, decoded)
            })
            .collect();
        metrics.blob_reference_keys = blob_reference_keys.len() as u64;
        metrics.blob_reference_seconds = started.elapsed().as_secs_f64();

        let started = std::time::Instant::now();
        let inline_payloads = view.multi_get_raw(CF_PAYLOAD_ARTIFACTS, &inline_payload_keys)?;
        metrics.inline_payload_keys = inline_payload_keys.len() as u64;
        metrics.inline_payload_seconds = started.elapsed().as_secs_f64();
        metrics.emit();

        Ok(Self {
            heads,
            stored_versions,
            receipts,
            blob_references,
            inline_payloads,
            alias_registries,
            policies,
            versioning,
        })
    }

    pub(super) fn head(&self, key: &[u8]) -> Option<Cached<Head>> {
        self.heads.get(key).cloned()
    }

    pub(super) fn stored_version(&self, head_key: &[u8]) -> Option<Cached<StoredVersion>> {
        self.stored_versions.get(head_key).cloned()
    }

    pub(super) fn receipt(&self, key: &[u8]) -> Option<Cached<StoredReceipt>> {
        self.receipts.get(key).cloned()
    }

    pub(super) fn blob_reference(&self, reference: &BlobRef) -> Option<Cached<BlobReferenceState>> {
        self.blob_references
            .get(&blob_reference_key(reference))
            .cloned()
    }

    pub(super) fn blob_reference_by_key(&self, key: &[u8]) -> Option<Cached<BlobReferenceState>> {
        self.blob_references.get(key).cloned()
    }

    pub(super) fn inline_payload(&self, reference: &BlobRef) -> Option<Cached<Vec<u8>>> {
        self.inline_payloads
            .get(&complete_artifact_key(reference))
            .cloned()
    }

    pub(super) fn alias_registry(
        &self,
        head_key: &[u8],
        canonical_path: &str,
    ) -> Option<Cached<ObjectAliasRegistry>> {
        self.alias_registries.get(head_key).cloned().map(|cached| {
            cached.and_then(|registry| {
                if let Some(registry) = registry.as_ref() {
                    registry.validate(canonical_path)?;
                }
                Ok(registry)
            })
        })
    }

    pub(super) fn seed_bucket_settings(
        &self,
        policies: &mut BTreeMap<Vec<u8>, Result<BucketPolicy, MutationError>>,
        versioning: &mut BTreeMap<Vec<u8>, Result<ObjectVersioning, MutationError>>,
    ) {
        for (key, value) in &self.policies {
            policies.entry(key.clone()).or_insert_with(|| value.clone());
        }
        for (key, value) in &self.versioning {
            versioning
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
    }
}

impl MutationReadToken {
    pub(super) fn len(&self) -> usize {
        self.cells.len()
    }

    /// Re-read every observed cell under the caller's commit guard. A stale
    /// early cell does not short-circuit the remaining exact comparisons.
    pub(super) fn revalidate(
        &self,
        store: &Store,
    ) -> Result<MutationReadRevalidation, MutationError> {
        let mut unchanged = true;
        let mut checked_keys = 0;
        let mut start = 0;
        while start < self.cells.len() {
            let cf_name = self.cells[start].cf_name;
            let mut end = start + 1;
            while end < self.cells.len()
                && self.cells[end].cf_name == cf_name
                && end - start < PREFETCH_KEYS_PER_MULTI_GET
            {
                end += 1;
            }
            let cells = &self.cells[start..end];
            let cf = store.cf(cf_name)?;
            let fetched = store
                .db
                .multi_get_cf(cells.iter().map(|cell| (cf, cell.key.as_slice())));
            if fetched.len() != cells.len() {
                return Err(MutationError::Storage(format!(
                    "{cf_name} mutation token revalidation returned the wrong result count"
                )));
            }
            for (cell, value) in cells.iter().zip(fetched) {
                let value = value
                    .map(|encoded| encoded.map(|bytes| bytes.to_vec()))
                    .map_err(storage_error)?;
                checked_keys += 1;
                unchanged &= value == cell.value;
            }
            start = end;
        }
        Ok(MutationReadRevalidation {
            unchanged,
            checked_keys,
        })
    }
}

impl PrefetchMetrics {
    fn emit(self) {
        tracing::info!(
            monotonic_counter.keldra_store_bulk_prefetch_head_keys_total = self.head_keys,
            monotonic_counter.keldra_store_bulk_prefetch_version_keys_total = self.version_keys,
            monotonic_counter.keldra_store_bulk_prefetch_receipt_keys_total = self.receipt_keys,
            monotonic_counter.keldra_store_bulk_prefetch_blob_reference_keys_total =
                self.blob_reference_keys,
            monotonic_counter.keldra_store_bulk_prefetch_inline_payload_keys_total =
                self.inline_payload_keys,
            monotonic_counter.keldra_store_bulk_prefetch_alias_registry_keys_total =
                self.alias_registry_keys,
            monotonic_counter.keldra_store_bulk_prefetch_policy_keys_total = self.policy_keys,
            monotonic_counter.keldra_store_bulk_prefetch_versioning_keys_total =
                self.versioning_keys,
            histogram.keldra_store_bulk_prefetch_heads_duration_seconds = self.head_seconds,
            histogram.keldra_store_bulk_prefetch_versions_duration_seconds = self.version_seconds,
            histogram.keldra_store_bulk_prefetch_receipts_duration_seconds = self.receipt_seconds,
            histogram.keldra_store_bulk_prefetch_blob_references_duration_seconds =
                self.blob_reference_seconds,
            histogram.keldra_store_bulk_prefetch_inline_payloads_duration_seconds =
                self.inline_payload_seconds,
            histogram.keldra_store_bulk_prefetch_alias_registries_duration_seconds =
                self.alias_registry_seconds,
            histogram.keldra_store_bulk_prefetch_policies_duration_seconds = self.policy_seconds,
            histogram.keldra_store_bulk_prefetch_versioning_duration_seconds =
                self.versioning_seconds,
            "object storage bulk baseline prefetched"
        );
    }
}

fn decode_bucket_policy(
    key: &[u8],
    cached: Cached<Vec<u8>>,
) -> Result<BucketPolicy, MutationError> {
    let Some(encoded) = cached? else {
        return Ok(BucketPolicy::default());
    };
    let identity = BucketIdentity::decode(key).map_err(storage_error)?;
    let id = crate::LogicalRecordId::BucketPolicy {
        tenant_id: identity.tenant_id.0,
        bucket_id: identity.bucket_id.0,
    };
    match decode_current_value(&id, &encoded).map_err(storage_error)? {
        crate::LogicalRecordValue::BucketPolicy {
            tenant_id,
            bucket_id,
            policy,
        } if tenant_id == identity.tenant_id.0 && bucket_id == identity.bucket_id.0 => Ok(policy),
        _ => Err(MutationError::Storage(
            "bucket policy has the wrong logical type or identity".into(),
        )),
    }
}

fn decode_bucket_versioning(
    key: &[u8],
    cached: Cached<Vec<u8>>,
) -> Result<ObjectVersioning, MutationError> {
    let Some(encoded) = cached? else {
        return Ok(ObjectVersioning::default());
    };
    let identity = BucketIdentity::decode(key).map_err(storage_error)?;
    let id = crate::LogicalRecordId::BucketOptions {
        tenant_id: identity.tenant_id.0,
        bucket_id: identity.bucket_id.0,
    };
    match decode_current_value(&id, &encoded).map_err(storage_error)? {
        crate::LogicalRecordValue::BucketOptions {
            tenant_id,
            bucket_id,
            versioning,
        } if tenant_id == identity.tenant_id.0 && bucket_id == identity.bucket_id.0 => {
            Ok(versioning)
        }
        _ => Err(MutationError::Storage(
            "bucket options have the wrong logical type or identity".into(),
        )),
    }
}

fn multi_get_json<T>(
    view: &MutationReadView<'_>,
    cf_name: &'static str,
    keys: &BTreeSet<Vec<u8>>,
) -> Result<(BTreeMap<Vec<u8>, Cached<T>>, f64), MutationError>
where
    T: for<'de> Deserialize<'de>,
{
    let started = std::time::Instant::now();
    let values = view
        .multi_get_raw(cf_name, keys)?
        .into_iter()
        .map(|(key, cached)| {
            let decoded = cached.and_then(|value| {
                value
                    .map(|encoded| serde_json::from_slice(&encoded).map_err(storage_error))
                    .transpose()
            });
            (key, decoded)
        })
        .collect();
    Ok((values, started.elapsed().as_secs_f64()))
}

fn exact_version_key(head_key: &[u8], version: VersionId) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(head_key.len() + 1 + size_of::<u64>());
    encoded.extend_from_slice(head_key);
    encoded.push(0);
    encoded.extend_from_slice(&version.0.to_be_bytes());
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoreOptions;

    async fn store() -> (tempfile::TempDir, Store) {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        (temporary, store)
    }

    #[tokio::test]
    async fn exact_token_detects_absence_becoming_present() {
        let (_temporary, store) = store().await;
        let key = b"speculative-absent".to_vec();
        let token = MutationReadToken {
            cells: vec![ObservedMutationCell {
                cf_name: CF_METADATA,
                key: key.clone(),
                value: None,
            }],
        };
        let initial = token.revalidate(&store).unwrap();
        assert!(initial.unchanged);
        assert_eq!(initial.checked_keys, 1);

        store
            .db
            .put_cf(store.cf(CF_METADATA).unwrap(), key, b"present")
            .unwrap();
        let changed = token.revalidate(&store).unwrap();
        assert!(!changed.unchanged);
        assert_eq!(changed.checked_keys, 1);
    }

    #[tokio::test]
    async fn exact_token_detects_present_value_change_and_checks_every_key() {
        let (_temporary, store) = store().await;
        let first = b"speculative-first".to_vec();
        let second = b"speculative-second".to_vec();
        let metadata = store.cf(CF_METADATA).unwrap();
        store.db.put_cf(metadata, &first, b"one").unwrap();
        store.db.put_cf(metadata, &second, b"two").unwrap();
        let token = MutationReadToken {
            cells: vec![
                ObservedMutationCell {
                    cf_name: CF_METADATA,
                    key: first.clone(),
                    value: Some(b"one".to_vec()),
                },
                ObservedMutationCell {
                    cf_name: CF_METADATA,
                    key: second,
                    value: Some(b"two".to_vec()),
                },
            ],
        };

        store.db.put_cf(metadata, first, b"changed").unwrap();
        let changed = token.revalidate(&store).unwrap();
        assert!(!changed.unchanged);
        assert_eq!(changed.checked_keys, token.len());

        store.db.delete_cf(metadata, b"speculative-first").unwrap();
        let removed = token.revalidate(&store).unwrap();
        assert!(!removed.unchanged);
        assert_eq!(removed.checked_keys, token.len());
    }

    #[tokio::test]
    async fn snapshot_view_retains_the_value_selected_before_a_current_write() {
        let (_temporary, store) = store().await;
        let key = b"snapshot-cell".to_vec();
        let metadata = store.cf(CF_METADATA).unwrap();
        store.db.put_cf(metadata, &key, b"before").unwrap();
        let view = MutationReadView::snapshot(&store);
        let keys = BTreeSet::from([key.clone()]);
        let selected = view.multi_get_raw(CF_METADATA, &keys).unwrap();

        store.db.put_cf(metadata, &key, b"after").unwrap();
        assert_eq!(
            selected.get(&key).unwrap().as_ref().unwrap().as_deref(),
            Some(b"before".as_slice())
        );
        let revalidated = view.token().revalidate(&store).unwrap();
        assert!(!revalidated.unchanged);
        assert_eq!(revalidated.checked_keys, 1);
    }
}
