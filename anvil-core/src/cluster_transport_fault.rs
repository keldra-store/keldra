//! Scoped transport gates for deterministic in-process cluster fault tests.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Mutex, OnceLock},
};

static PARTITIONS: OnceLock<Mutex<BTreeMap<String, BTreeSet<String>>>> = OnceLock::new();

pub fn partition_node(cluster_id: &str, node_id: impl Into<String>) {
    PARTITIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("cluster transport partition lock poisoned")
        .entry(cluster_id.to_string())
        .or_default()
        .insert(node_id.into());
}

pub fn heal_node(cluster_id: &str, node_id: &str) {
    let mut partitions = PARTITIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("cluster transport partition lock poisoned");
    if let Some(nodes) = partitions.get_mut(cluster_id) {
        nodes.remove(node_id);
        if nodes.is_empty() {
            partitions.remove(cluster_id);
        }
    }
}

pub fn link_available(cluster_id: &str, source: &str, target: &str) -> bool {
    let partitions = PARTITIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("cluster transport partition lock poisoned");
    !partitions
        .get(cluster_id)
        .is_some_and(|nodes| nodes.contains(source) || nodes.contains(target))
}
