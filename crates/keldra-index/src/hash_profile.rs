//! Opt-in, call-site-specific BLAKE3 cost attribution.
//!
//! The fast path is a direct `blake3::hash` unless `KELDRA_HASH_PROFILE=1`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

pub struct HashProfileSite {
    name: &'static str,
    registered: AtomicBool,
    calls: AtomicU64,
    bytes: AtomicU64,
    nanoseconds: AtomicU64,
}

impl HashProfileSite {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            registered: AtomicBool::new(false),
            calls: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            nanoseconds: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HashProfileSnapshot {
    pub site: &'static str,
    pub calls: u64,
    pub bytes: u64,
    pub nanoseconds: u64,
}

static ENABLED: OnceLock<bool> = OnceLock::new();
static SITES: OnceLock<Mutex<Vec<&'static HashProfileSite>>> = OnceLock::new();

fn enabled() -> bool {
    *ENABLED
        .get_or_init(|| std::env::var_os("KELDRA_HASH_PROFILE").is_some_and(|value| value == "1"))
}

fn register(site: &'static HashProfileSite) {
    if site
        .registered
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        SITES
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(site);
    }
}

#[doc(hidden)]
pub fn hash(site: &'static HashProfileSite, bytes: &[u8]) -> blake3::Hash {
    if !enabled() {
        return blake3::hash(bytes);
    }
    register(site);
    let started = Instant::now();
    let hash = blake3::hash(bytes);
    let elapsed = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
    site.calls.fetch_add(1, Ordering::Relaxed);
    site.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
    site.nanoseconds.fetch_add(elapsed, Ordering::Relaxed);
    hash
}

pub fn snapshots() -> Vec<HashProfileSnapshot> {
    let mut snapshots = SITES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .map(|site| HashProfileSnapshot {
            site: site.name,
            calls: site.calls.load(Ordering::Relaxed),
            bytes: site.bytes.load(Ordering::Relaxed),
            nanoseconds: site.nanoseconds.load(Ordering::Relaxed),
        })
        .collect::<Vec<_>>();
    snapshots.sort_unstable_by_key(|snapshot| snapshot.site);
    snapshots
}

#[macro_export]
macro_rules! profiled_blake3_hash {
    ($bytes:expr) => {{
        static SITE: $crate::hash_profile::HashProfileSite =
            $crate::hash_profile::HashProfileSite::new(concat!(module_path!(), ":", line!()));
        $crate::hash_profile::hash(&SITE, $bytes)
    }};
}
