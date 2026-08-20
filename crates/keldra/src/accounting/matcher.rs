//! Bounded, disposable accounting-definition matching for public traffic.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use keldra_consensus::NodeId;
use keldra_store::DefinitionKind;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{AccountingTrafficBatch, AccountingTrafficEntry};
use crate::cluster_placement::ClusterPlacement;
use crate::index_runtime::coordination::{
    ClusterDefinitionLocatorScanner, load_definition_locator_object,
};
use crate::placement::PlacementKind;

use super::{
    AccountingIdentity, LoadedAccountingDefinition, StoredAccountingDefinition, definition_path,
    is_accounting_path,
};

const LOCATOR_PAGE_RECORDS: usize = 256;
const DEFAULT_MAX_BUCKETS: usize = 4_096;
const DEFAULT_MAX_DEFINITIONS: usize = 65_536;
const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct AccountingMatcherConfig {
    pub(crate) max_buckets: usize,
    pub(crate) max_definitions: usize,
    pub(crate) max_bytes: u64,
}

impl AccountingMatcherConfig {
    pub(crate) fn new(max_buckets: usize, max_definitions: usize, max_bytes: u64) -> Option<Self> {
        (max_buckets != 0 && max_definitions != 0 && max_bytes != 0).then_some(Self {
            max_buckets,
            max_definitions,
            max_bytes,
        })
    }
}

impl Default for AccountingMatcherConfig {
    fn default() -> Self {
        Self {
            max_buckets: DEFAULT_MAX_BUCKETS,
            max_definitions: DEFAULT_MAX_DEFINITIONS,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

#[derive(Clone)]
pub(crate) struct AccountingMatcher {
    scanner: ClusterDefinitionLocatorScanner,
    reader: ClusterObjectReader,
    config: AccountingMatcherConfig,
    cache: Arc<Mutex<MatcherCache>>,
    load_gate: Arc<AsyncMutex<()>>,
    delivery_gate: Arc<AsyncMutex<()>>,
}

impl AccountingMatcher {
    pub(crate) fn new(
        scanner: ClusterDefinitionLocatorScanner,
        reader: ClusterObjectReader,
        config: AccountingMatcherConfig,
    ) -> Self {
        Self {
            scanner,
            reader,
            config,
            cache: Arc::new(Mutex::new(MatcherCache::default())),
            load_gate: Arc::new(AsyncMutex::new(())),
            delivery_gate: Arc::new(AsyncMutex::new(())),
        }
    }

    pub(crate) async fn match_batch(
        &self,
        batch: &AccountingTrafficBatch,
    ) -> Result<MatchedAccountingBatch, Status> {
        let permit = self.delivery_gate.clone().lock_owned().await;
        let bucket = BucketIdentity::new(batch.tenant_id, batch.bucket_id)?;
        let routes = self.routes(bucket).await?;
        Ok(MatchedAccountingBatch {
            matches: aggregate_matches(&routes, &batch.entries)?,
            _permit: permit,
        })
    }

    pub(crate) async fn invalidate_bucket(
        &self,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<(), Status> {
        let bucket = BucketIdentity::new(tenant_id, bucket_id)?;
        invalidate_matcher_cache(&self.delivery_gate, &self.cache, Some(bucket)).await;
        Ok(())
    }

    pub(crate) async fn clear(&self) {
        invalidate_matcher_cache(&self.delivery_gate, &self.cache, None).await;
    }

    async fn routes(&self, bucket: BucketIdentity) -> Result<Arc<MatcherRoutes>, Status> {
        let cached = {
            let mut cache = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cache.lookup(bucket)
        };
        if let Some(routes) = cached {
            return Ok(routes);
        }
        // A bucket load may use the complete configured matcher-memory bound.
        // Serialize loads process-wide, then recheck so waiters for the same
        // bucket reuse the result produced by the first request.
        let _load_permit = self.load_gate.lock().await;
        let rechecked = {
            let mut cache = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cache.lookup(bucket)
        };
        if let Some(routes) = rechecked {
            return Ok(routes);
        }
        let loaded = self.load_bucket(bucket).await?;
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.insert(bucket, loaded, self.config)
    }

    async fn load_bucket(&self, bucket: BucketIdentity) -> Result<MatcherRoutes, Status> {
        let mut scan = self.scanner.begin_bucket(
            DefinitionKind::Accounting,
            bucket.tenant_id,
            bucket.bucket_id,
        )?;
        let mut by_prefix = BTreeMap::<String, Vec<Arc<LoadedAccountingDefinition>>>::new();
        let mut definition_count = 0_usize;
        let mut charged_bytes = 0_u64;
        while let Some(locators) = scan.next_page(LOCATOR_PAGE_RECORDS).await? {
            for locator in locators {
                if locator.kind != DefinitionKind::Accounting
                    || locator.tenant_id != bucket.tenant_id
                    || locator.bucket_id != bucket.bucket_id
                    || definition_path(locator.definition_id)? != locator.path
                {
                    return Err(Status::data_loss(
                        "accounting locator escaped its requested bucket or path shape",
                    ));
                }
                let Some(object) = load_definition_locator_object(&self.reader, &locator).await?
                else {
                    continue;
                };
                let stored = StoredAccountingDefinition::decode(&object.bytes)?;
                if stored.accounting_id != locator.definition_id {
                    return Err(Status::data_loss(
                        "accounting definition identity disagrees with its locator",
                    ));
                }
                definition_count = definition_count.checked_add(1).ok_or_else(|| {
                    Status::resource_exhausted("accounting matcher definition count overflow")
                })?;
                charged_bytes = charged_bytes
                    .checked_add(definition_charge(&stored))
                    .ok_or_else(|| {
                        Status::resource_exhausted("accounting matcher cache charge overflow")
                    })?;
                if definition_count > self.config.max_definitions
                    || charged_bytes > self.config.max_bytes
                {
                    return Err(Status::resource_exhausted(
                        "accounting definitions exceed the configured matcher cache bound",
                    ));
                }
                by_prefix
                    .entry(stored.path_prefix.clone())
                    .or_default()
                    .push(Arc::new(LoadedAccountingDefinition {
                        tenant_id: locator.tenant_id,
                        bucket_id: locator.bucket_id,
                        version: object.object_version,
                        stored,
                    }));
            }
        }
        Ok(MatcherRoutes {
            by_prefix,
            definition_count,
            charged_bytes: charged_bytes.max(1),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BucketIdentity {
    tenant_id: u64,
    bucket_id: u64,
}

impl BucketIdentity {
    fn new(tenant_id: u64, bucket_id: u64) -> Result<Self, Status> {
        if tenant_id == 0 || bucket_id == 0 {
            return Err(Status::invalid_argument(
                "accounting matcher bucket identity must be non-zero",
            ));
        }
        Ok(Self {
            tenant_id,
            bucket_id,
        })
    }

    fn placement_key(self) -> [u8; 16] {
        let mut key = [0_u8; 16];
        key[..8].copy_from_slice(&self.tenant_id.to_be_bytes());
        key[8..].copy_from_slice(&self.bucket_id.to_be_bytes());
        key
    }
}

pub(crate) fn matcher_node(
    placement: &ClusterPlacement,
    tenant_id: u64,
    bucket_id: u64,
) -> Result<NodeId, Status> {
    let bucket = BucketIdentity::new(tenant_id, bucket_id)?;
    placement
        .rank(PlacementKind::AccountingMatcher, &bucket.placement_key())
        .into_iter()
        .next()
        .ok_or_else(|| Status::unavailable("accounting matcher has no ACTIVE node"))
}

#[derive(Clone)]
struct MatcherRoutes {
    by_prefix: BTreeMap<String, Vec<Arc<LoadedAccountingDefinition>>>,
    definition_count: usize,
    charged_bytes: u64,
}

#[derive(Default)]
struct MatcherCache {
    buckets: BTreeMap<BucketIdentity, CachedBucket>,
    definition_count: usize,
    charged_bytes: u64,
    generation: u64,
}

struct CachedBucket {
    routes: Arc<MatcherRoutes>,
    loaded_at: Instant,
    last_access: Instant,
    generation: u64,
}

impl MatcherCache {
    fn lookup(&mut self, bucket: BucketIdentity) -> Option<Arc<MatcherRoutes>> {
        let Some(entry) = self.buckets.get_mut(&bucket) else {
            return None;
        };
        entry.last_access = Instant::now();
        let age = entry.loaded_at.elapsed();
        tracing::debug!(
            tenant.id = bucket.tenant_id,
            bucket.id = bucket.bucket_id,
            gauge.keldra_accounting_matcher_cache_generation = entry.generation,
            gauge.keldra_accounting_matcher_cache_age_millis = age.as_millis() as u64,
            gauge.keldra_accounting_definition_propagation_lag_millis = age.as_millis() as u64,
            "accounting matcher cache lookup"
        );
        Some(entry.routes.clone())
    }

    fn remove(&mut self, bucket: BucketIdentity) {
        let Some(removed) = self.buckets.remove(&bucket) else {
            return;
        };
        self.definition_count = self
            .definition_count
            .saturating_sub(removed.routes.definition_count);
        self.charged_bytes = self
            .charged_bytes
            .saturating_sub(removed.routes.charged_bytes);
        self.generation = self.generation.wrapping_add(1).max(1);
    }

    fn clear(&mut self) {
        if self.buckets.is_empty() {
            return;
        }
        self.buckets.clear();
        self.definition_count = 0;
        self.charged_bytes = 0;
        self.generation = self.generation.wrapping_add(1).max(1);
    }

    fn insert(
        &mut self,
        bucket: BucketIdentity,
        routes: MatcherRoutes,
        config: AccountingMatcherConfig,
    ) -> Result<Arc<MatcherRoutes>, Status> {
        if routes.charged_bytes > config.max_bytes
            || routes.definition_count > config.max_definitions
        {
            return Err(Status::resource_exhausted(
                "accounting matcher bucket exceeds the configured cache bound",
            ));
        }
        if let Some(previous) = self.buckets.remove(&bucket) {
            self.definition_count = self
                .definition_count
                .saturating_sub(previous.routes.definition_count);
            self.charged_bytes = self
                .charged_bytes
                .saturating_sub(previous.routes.charged_bytes);
        }
        while self.buckets.len() >= config.max_buckets
            || self
                .definition_count
                .saturating_add(routes.definition_count)
                > config.max_definitions
            || self.charged_bytes.saturating_add(routes.charged_bytes) > config.max_bytes
        {
            let Some(oldest) = self
                .buckets
                .iter()
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(identity, _)| *identity)
            else {
                break;
            };
            let removed = self.buckets.remove(&oldest).expect("oldest bucket exists");
            self.definition_count = self
                .definition_count
                .saturating_sub(removed.routes.definition_count);
            self.charged_bytes = self
                .charged_bytes
                .saturating_sub(removed.routes.charged_bytes);
        }
        self.generation = self.generation.wrapping_add(1).max(1);
        let routes = Arc::new(routes);
        self.definition_count = self
            .definition_count
            .saturating_add(routes.definition_count);
        self.charged_bytes = self.charged_bytes.saturating_add(routes.charged_bytes);
        self.buckets.insert(
            bucket,
            CachedBucket {
                routes: routes.clone(),
                loaded_at: Instant::now(),
                last_access: Instant::now(),
                generation: self.generation,
            },
        );
        tracing::debug!(
            tenant.id = bucket.tenant_id,
            bucket.id = bucket.bucket_id,
            gauge.keldra_accounting_matcher_cache_buckets = self.buckets.len() as u64,
            gauge.keldra_accounting_matcher_cache_definitions = self.definition_count as u64,
            gauge.keldra_accounting_matcher_cache_bytes = self.charged_bytes,
            gauge.keldra_accounting_matcher_cache_generation = self.generation,
            "accounting matcher cache refreshed"
        );
        Ok(routes)
    }
}

async fn invalidate_matcher_cache(
    delivery_gate: &Arc<AsyncMutex<()>>,
    cache: &Arc<Mutex<MatcherCache>>,
    bucket: Option<BucketIdentity>,
) {
    let _permit = delivery_gate.lock().await;
    let mut cache = cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match bucket {
        Some(bucket) => cache.remove(bucket),
        None => cache.clear(),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MatchedAccountingTraffic {
    pub(crate) definition: Arc<LoadedAccountingDefinition>,
    pub(crate) accepted_inbound_bytes: u64,
    pub(crate) served_outbound_bytes: u64,
}

pub(crate) struct MatchedAccountingBatch {
    matches: Vec<MatchedAccountingTraffic>,
    _permit: OwnedMutexGuard<()>,
}

impl MatchedAccountingBatch {
    pub(crate) fn iter(&self) -> impl Iterator<Item = &MatchedAccountingTraffic> {
        self.matches.iter()
    }
}

fn aggregate_matches(
    routes: &MatcherRoutes,
    entries: &[AccountingTrafficEntry],
) -> Result<Vec<MatchedAccountingTraffic>, Status> {
    let mut totals = BTreeMap::<AccountingIdentity, MatchedAccountingTraffic>::new();
    for entry in entries {
        if is_accounting_path(&entry.exact_path) {
            continue;
        }
        visit_path_prefixes(&entry.exact_path, |prefix| {
            let Some(definitions) = routes.by_prefix.get(prefix) else {
                return Ok(());
            };
            for definition in definitions {
                let identity = (
                    definition.tenant_id,
                    definition.bucket_id,
                    definition.stored.accounting_id,
                );
                let total = totals
                    .entry(identity)
                    .or_insert_with(|| MatchedAccountingTraffic {
                        definition: definition.clone(),
                        accepted_inbound_bytes: 0,
                        served_outbound_bytes: 0,
                    });
                total.accepted_inbound_bytes = total
                    .accepted_inbound_bytes
                    .checked_add(entry.accepted_inbound_bytes)
                    .ok_or_else(|| Status::resource_exhausted("accounting inbound sum overflow"))?;
                total.served_outbound_bytes = total
                    .served_outbound_bytes
                    .checked_add(entry.served_outbound_bytes)
                    .ok_or_else(|| {
                        Status::resource_exhausted("accounting outbound sum overflow")
                    })?;
            }
            Ok(())
        })?;
    }
    Ok(totals.into_values().collect())
}

fn visit_path_prefixes(
    path: &str,
    mut visit: impl FnMut(&str) -> Result<(), Status>,
) -> Result<(), Status> {
    visit("")?;
    for (offset, byte) in path.bytes().enumerate() {
        if byte == b'/' {
            visit(&path[..offset])?;
        }
    }
    visit(path)
}

fn definition_charge(definition: &StoredAccountingDefinition) -> u64 {
    256_u64
        .saturating_add(definition.storage_tenant.len() as u64)
        .saturating_add(definition.bucket.len() as u64)
        .saturating_add(definition.path_prefix.len() as u64)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use keldra_store::VersionId;

    use super::*;

    fn definition(prefix: &str) -> LoadedAccountingDefinition {
        LoadedAccountingDefinition {
            tenant_id: 11,
            bucket_id: 12,
            version: VersionId(3),
            stored: StoredAccountingDefinition::create(
                "tenant".into(),
                "bucket".into(),
                prefix.into(),
                11,
                12,
            )
            .unwrap(),
        }
    }

    #[test]
    fn segment_prefixes_match_overlapping_definitions_once() {
        let all = Arc::new(definition(""));
        let tenant = Arc::new(definition("tenant/7"));
        let sibling = Arc::new(definition("tenant/70"));
        let routes = MatcherRoutes {
            by_prefix: BTreeMap::from([
                ("".into(), vec![all.clone()]),
                ("tenant/7".into(), vec![tenant.clone()]),
                ("tenant/70".into(), vec![sibling]),
            ]),
            definition_count: 3,
            charged_bytes: 1,
        };
        let matched = aggregate_matches(
            &routes,
            &[AccountingTrafficEntry {
                exact_path: "tenant/7/file".into(),
                accepted_inbound_bytes: 9,
                served_outbound_bytes: 4,
            }],
        )
        .unwrap();
        assert_eq!(matched.len(), 2);
        assert!(
            matched
                .iter()
                .any(|value| Arc::ptr_eq(&value.definition, &all))
        );
        assert!(
            matched
                .iter()
                .any(|value| Arc::ptr_eq(&value.definition, &tenant))
        );
        assert!(
            matched
                .iter()
                .all(|value| value.accepted_inbound_bytes == 9)
        );
    }

    #[test]
    fn cache_eviction_obeys_shared_bucket_and_byte_bounds() {
        let config = AccountingMatcherConfig::new(1, 10, 20).unwrap();
        let mut cache = MatcherCache::default();
        let routes = || MatcherRoutes {
            by_prefix: BTreeMap::new(),
            definition_count: 0,
            charged_bytes: 10,
        };
        cache
            .insert(BucketIdentity::new(1, 1).unwrap(), routes(), config)
            .unwrap();
        cache
            .insert(BucketIdentity::new(1, 2).unwrap(), routes(), config)
            .unwrap();
        assert_eq!(cache.buckets.len(), 1);
        assert!(
            cache
                .buckets
                .contains_key(&BucketIdentity::new(1, 2).unwrap())
        );
        assert_eq!(cache.charged_bytes, 10);
    }

    #[test]
    fn cache_eviction_obeys_the_shared_definition_bound() {
        let config = AccountingMatcherConfig::new(10, 1, 1_024).unwrap();
        let routes = || MatcherRoutes {
            by_prefix: BTreeMap::new(),
            definition_count: 1,
            charged_bytes: 10,
        };
        let mut cache = MatcherCache::default();
        cache
            .insert(BucketIdentity::new(1, 1).unwrap(), routes(), config)
            .unwrap();
        cache
            .insert(BucketIdentity::new(1, 2).unwrap(), routes(), config)
            .unwrap();
        assert_eq!(cache.buckets.len(), 1);
        assert_eq!(cache.definition_count, 1);
        assert!(
            cache
                .buckets
                .contains_key(&BucketIdentity::new(1, 2).unwrap())
        );
    }

    #[tokio::test]
    async fn process_load_gate_allows_only_one_in_flight_bucket_load() {
        let gate = Arc::new(AsyncMutex::new(()));
        let barrier = Arc::new(tokio::sync::Barrier::new(9));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let gate = gate.clone();
            let barrier = barrier.clone();
            let in_flight = in_flight.clone();
            let maximum = maximum.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                let _permit = gate.lock().await;
                let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(active, Ordering::SeqCst);
                tokio::task::yield_now().await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
            });
        }
        barrier.wait().await;
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cached_bucket_does_not_expire_with_elapsed_time() {
        let config = AccountingMatcherConfig::default();
        let bucket = BucketIdentity::new(1, 1).unwrap();
        let mut cache = MatcherCache::default();
        cache
            .insert(
                bucket,
                MatcherRoutes {
                    by_prefix: BTreeMap::new(),
                    definition_count: 0,
                    charged_bytes: 1,
                },
                config,
            )
            .unwrap();
        cache.buckets.get_mut(&bucket).unwrap().loaded_at =
            Instant::now() - Duration::from_secs(24 * 60 * 60);

        assert!(cache.lookup(bucket).is_some());
        assert!(cache.lookup(bucket).is_some());
    }

    #[tokio::test]
    async fn bucket_invalidation_waits_for_delivery_and_is_exact_and_idempotent() {
        let config = AccountingMatcherConfig::default();
        let first = BucketIdentity::new(1, 1).unwrap();
        let second = BucketIdentity::new(1, 2).unwrap();
        let gate = Arc::new(AsyncMutex::new(()));
        let cache = Arc::new(Mutex::new(MatcherCache::default()));
        for bucket in [first, second] {
            cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(
                    bucket,
                    MatcherRoutes {
                        by_prefix: BTreeMap::new(),
                        definition_count: 0,
                        charged_bytes: 1,
                    },
                    config,
                )
                .unwrap();
        }
        let held = gate.lock().await;
        let waiting_gate = gate.clone();
        let waiting_cache = cache.clone();
        let waiter = tokio::spawn(async move {
            invalidate_matcher_cache(&waiting_gate, &waiting_cache, Some(first)).await;
        });
        tokio::task::yield_now().await;
        assert!(cache.lock().unwrap().buckets.contains_key(&first));
        drop(held);
        waiter.await.unwrap();

        assert!(!cache.lock().unwrap().buckets.contains_key(&first));
        assert!(cache.lock().unwrap().buckets.contains_key(&second));
        invalidate_matcher_cache(&gate, &cache, Some(first)).await;
        assert!(cache.lock().unwrap().buckets.contains_key(&second));
    }

    #[tokio::test]
    async fn gap_invalidation_clears_every_cached_bucket() {
        let config = AccountingMatcherConfig::default();
        let gate = Arc::new(AsyncMutex::new(()));
        let cache = Arc::new(Mutex::new(MatcherCache::default()));
        for bucket in [
            BucketIdentity::new(1, 1).unwrap(),
            BucketIdentity::new(2, 2).unwrap(),
        ] {
            cache
                .lock()
                .unwrap()
                .insert(
                    bucket,
                    MatcherRoutes {
                        by_prefix: BTreeMap::new(),
                        definition_count: 0,
                        charged_bytes: 1,
                    },
                    config,
                )
                .unwrap();
        }

        invalidate_matcher_cache(&gate, &cache, None).await;
        assert!(cache.lock().unwrap().buckets.is_empty());
        assert_eq!(cache.lock().unwrap().charged_bytes, 0);
    }

    #[tokio::test]
    async fn match_delivery_permit_is_held_until_the_matched_batch_is_dropped() {
        let gate = Arc::new(AsyncMutex::new(()));
        let batch = MatchedAccountingBatch {
            matches: Vec::new(),
            _permit: gate.clone().lock_owned().await,
        };
        let mut waiter = tokio::spawn({
            let gate = gate.clone();
            async move { gate.lock_owned().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiter)
                .await
                .is_err()
        );
        drop(batch);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the permit should become available")
            .expect("the waiter should not panic");
    }
}
