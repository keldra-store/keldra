use std::collections::VecDeque;

use super::*;

const COMPILED_AUTHORIZATION_CACHE_ENTRIES: usize = 64;
const COMPILED_AUTHORIZATION_CACHE_TUPLES: usize = 262_144;
const COMPILED_AUTHORIZATION_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct AuthzRepository {
    pub(super) db: Arc<DB>,
    pub(super) write_lock: Arc<Mutex<()>>,
    pub(super) sync_writes: bool,
    pub(super) limits: AuthzStoreLimits,
    pub(super) compiled_cache: Arc<Mutex<CompiledAuthorizationCache>>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct CompiledAuthorizationKey {
    pub(super) scope: AuthzScope,
    pub(super) realm_revision: AuthzRevision,
    pub(super) binding_generation: u64,
    pub(super) schema_ref: SchemaRef,
    pub(super) limits: AuthorizationLimits,
}

pub(super) struct CompiledAuthorizationEntry {
    pub(super) key: CompiledAuthorizationKey,
    pub(super) value: Arc<Authorization>,
    pub(super) tuple_count: usize,
    pub(super) byte_weight: usize,
}

pub(crate) struct CompiledAuthorizationCache {
    pub(super) entries: VecDeque<CompiledAuthorizationEntry>,
    pub(super) tuple_count: usize,
    pub(super) byte_weight: usize,
    pub(super) max_entries: usize,
    pub(super) max_tuples: usize,
    pub(super) max_byte_weight: usize,
}

impl Default for CompiledAuthorizationCache {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            tuple_count: 0,
            byte_weight: 0,
            max_entries: COMPILED_AUTHORIZATION_CACHE_ENTRIES,
            max_tuples: COMPILED_AUTHORIZATION_CACHE_TUPLES,
            max_byte_weight: COMPILED_AUTHORIZATION_CACHE_BYTES,
        }
    }
}

impl CompiledAuthorizationCache {
    pub(super) fn get(&mut self, key: &CompiledAuthorizationKey) -> Option<Arc<Authorization>> {
        let index = self.entries.iter().position(|entry| entry.key == *key)?;
        let entry = self.entries.remove(index)?;
        let value = entry.value.clone();
        self.entries.push_back(entry);
        Some(value)
    }

    pub(super) fn insert(&mut self, key: CompiledAuthorizationKey, value: Arc<Authorization>) {
        let tuple_count = value.tuple_count();
        let byte_weight = value.estimated_heap_bytes();
        if tuple_count > self.max_tuples || byte_weight > self.max_byte_weight {
            return;
        }
        let mut index = 0;
        while index < self.entries.len() {
            let remove = self.entries[index].key == key
                || (self.entries[index].key.scope == key.scope
                    && self.entries[index].key.realm_revision <= key.realm_revision);
            if remove {
                if let Some(replaced) = self.entries.remove(index) {
                    self.tuple_count = self.tuple_count.saturating_sub(replaced.tuple_count);
                    self.byte_weight = self.byte_weight.saturating_sub(replaced.byte_weight);
                }
            } else {
                index += 1;
            }
        }
        self.tuple_count = self.tuple_count.saturating_add(tuple_count);
        self.byte_weight = self.byte_weight.saturating_add(byte_weight);
        self.entries.push_back(CompiledAuthorizationEntry {
            key,
            value,
            tuple_count,
            byte_weight,
        });
        while self.entries.len() > self.max_entries
            || self.tuple_count > self.max_tuples
            || self.byte_weight > self.max_byte_weight
        {
            let Some(evicted) = self.entries.pop_front() else {
                break;
            };
            self.tuple_count = self.tuple_count.saturating_sub(evicted.tuple_count);
            self.byte_weight = self.byte_weight.saturating_sub(evicted.byte_weight);
        }
    }
}

impl fmt::Debug for AuthzRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthzRepository")
            .finish_non_exhaustive()
    }
}

impl Store {
    pub fn authz(&self) -> AuthzRepository {
        AuthzRepository {
            db: self.db.clone(),
            write_lock: self.authz_write_lock.clone(),
            sync_writes: self.sync_writes,
            limits: AuthzStoreLimits::default(),
            compiled_cache: self.authz_compiled_cache.clone(),
        }
    }

    pub fn authz_with_limits(&self, limits: AuthzStoreLimits) -> AuthzRepository {
        AuthzRepository {
            db: self.db.clone(),
            write_lock: self.authz_write_lock.clone(),
            sync_writes: self.sync_writes,
            limits,
            compiled_cache: self.authz_compiled_cache.clone(),
        }
    }
}
