use serde::Serialize;
use std::collections::BTreeMap;

pub const CONTENT_TYPE: &str = "application/json";
pub const INDEX_NAME_PREFIX: &str = "contention";
pub const PROJECTION_PRESERVING_MARKERS: u64 = 256;

#[derive(Serialize)]
struct Record {
    payload: String,
    /// Each entry is one physical recipe. Every recipe receives the same
    /// multi-value probes, so P changes recipe count, not query semantics.
    /// The class preserves the stable/mutable oracle and the unique marker
    /// token provides an exact canary without a second indexed field.
    probes: BTreeMap<String, Vec<String>>,
}

pub fn stable_path(id: u64) -> String {
    format!("contention/stable/{id:08}.json")
}

pub fn mutable_path(id: u64) -> String {
    format!("contention/mutable/{id:08}.json")
}

pub fn marker_path(sequence: u64) -> String {
    format!("contention/marker/{sequence:016}.json")
}

pub fn marker_id(ordinal: u64) -> u64 {
    (1u64 << 63) | ordinal
}

pub fn payload(
    seed: u64,
    id: u64,
    class: &'static str,
    generation: u64,
    physical_recipe_count: usize,
) -> Vec<u8> {
    payload_with_generations(
        seed,
        id,
        class,
        generation,
        generation,
        physical_recipe_count,
    )
}

pub fn payload_with_generations(
    seed: u64,
    id: u64,
    class: &'static str,
    indexed_generation: u64,
    payload_generation: u64,
    physical_recipe_count: usize,
) -> Vec<u8> {
    let mixed = mix64(seed ^ id.rotate_left(19) ^ payload_generation.rotate_left(37));
    serde_json::to_vec(&Record {
        payload: format!("{mixed:016x}-{seed:016x}"),
        probes: recipe_probes(id, class, indexed_generation, physical_recipe_count),
    })
    .expect("generated contention record is serializable")
}

pub fn payload_at_least(
    seed: u64,
    id: u64,
    class: &'static str,
    generation: u64,
    minimum_bytes: usize,
    physical_recipe_count: usize,
) -> Vec<u8> {
    payload_with_generations_at_least(
        seed,
        id,
        class,
        generation,
        generation,
        minimum_bytes,
        physical_recipe_count,
    )
}

pub fn payload_with_generations_at_least(
    seed: u64,
    id: u64,
    class: &'static str,
    indexed_generation: u64,
    payload_generation: u64,
    minimum_bytes: usize,
    physical_recipe_count: usize,
) -> Vec<u8> {
    let mut encoded = payload_with_generations(
        seed,
        id,
        class,
        indexed_generation,
        payload_generation,
        physical_recipe_count,
    );
    if encoded.len() >= minimum_bytes {
        return encoded;
    }
    let missing = minimum_bytes - encoded.len();
    let mixed = mix64(seed ^ id.rotate_left(19) ^ payload_generation.rotate_left(37));
    encoded = serde_json::to_vec(&Record {
        payload: format!("{mixed:016x}-{seed:016x}{}", "x".repeat(missing)),
        probes: recipe_probes(id, class, indexed_generation, physical_recipe_count),
    })
    .expect("generated contention record is serializable");
    debug_assert_eq!(encoded.len(), minimum_bytes);
    encoded
}

pub fn index_name(position: usize) -> String {
    format!("{INDEX_NAME_PREFIX}-{position:03}")
}

pub fn corpus_digest(seed: u64, stable: u64, mutable: u64, physical_recipe_count: usize) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"keldra.index-contention.corpus.v1\0");
    hash.update(seed.to_be_bytes());
    hash.update(stable.to_be_bytes());
    hash.update(mutable.to_be_bytes());
    hash.update(physical_recipe_count.to_be_bytes());
    for id in 0..stable {
        hash.update(stable_path(id));
        hash.update(payload(seed, id, "stable", 0, physical_recipe_count));
    }
    for id in 0..mutable {
        hash.update(mutable_path(id));
        hash.update(payload(seed, id, "mutable", 0, physical_recipe_count));
    }
    format!("sha256:{}", hex::encode(hash.finalize()))
}

pub fn marker_probe(marker_id: u64) -> String {
    format!("marker:{marker_id}")
}

fn recipe_probes(
    id: u64,
    class: &'static str,
    indexed_generation: u64,
    physical_recipe_count: usize,
) -> BTreeMap<String, Vec<String>> {
    let identity = if class == "marker" {
        marker_probe(id)
    } else {
        format!("object:{id}")
    };
    (0..physical_recipe_count)
        .map(|recipe| {
            (
                format!("{recipe:02}"),
                vec![
                    class.to_owned(),
                    identity.clone(),
                    format!("generation:{indexed_generation}"),
                ],
            )
        })
        .collect()
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corpus_and_names_are_deterministic() {
        assert_eq!(index_name(4), "contention-004");
        assert_eq!(corpus_digest(7, 4, 3, 1), corpus_digest(7, 4, 3, 1));
        assert_ne!(corpus_digest(7, 4, 3, 1), corpus_digest(8, 4, 3, 1));
        assert_ne!(corpus_digest(7, 4, 3, 1), corpus_digest(7, 4, 3, 2));
        assert_ne!(
            payload(7, 2, "mutable", 1, 1),
            payload(7, 2, "mutable", 2, 1)
        );
        assert_ne!(
            payload_with_generations(7, 2, "mutable", 0, 1, 1),
            payload_with_generations(7, 2, "mutable", 0, 2, 1)
        );
        let padded = payload_at_least(7, 2, "mutable", 1, 25_000, 4);
        assert_eq!(padded.len(), 25_000);
        assert_eq!(
            payload_at_least(7, 2, "mutable", 1, 1, 1),
            payload(7, 2, "mutable", 1, 1)
        );
    }

    #[test]
    fn every_recipe_has_an_equivalent_multivalue_probe_set() {
        let value: serde_json::Value =
            serde_json::from_slice(&payload(7, 2, "mutable", 1, 4)).unwrap();
        assert_eq!(value.pointer("/probes/00/0").unwrap(), "mutable");
        assert_eq!(value.pointer("/probes/01/0").unwrap(), "mutable");
        assert_eq!(value.pointer("/probes/03/0").unwrap(), "mutable");
        assert_eq!(value.pointer("/probes/03/1").unwrap(), "object:2");
        assert_eq!(value.pointer("/probes/03/2").unwrap(), "generation:1");
        assert_eq!(marker_probe(marker_id(7)), "marker:9223372036854775815");
    }
}
