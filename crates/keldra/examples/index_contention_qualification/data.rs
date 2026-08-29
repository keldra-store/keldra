use serde::Serialize;

pub const CONTENT_TYPE: &str = "application/json";
pub const INDEX_NAME_PREFIX: &str = "contention";

#[derive(Serialize)]
struct Record<'a> {
    record_id: u64,
    class: &'a str,
    generation: u64,
    payload: String,
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

pub fn payload(seed: u64, id: u64, class: &'static str, generation: u64) -> Vec<u8> {
    let mixed = mix64(seed ^ id.rotate_left(19) ^ generation.rotate_left(37));
    serde_json::to_vec(&Record {
        record_id: id,
        class,
        generation,
        payload: format!("{mixed:016x}-{seed:016x}"),
    })
    .expect("generated contention record is serializable")
}

pub fn payload_at_least(
    seed: u64,
    id: u64,
    class: &'static str,
    generation: u64,
    minimum_bytes: usize,
) -> Vec<u8> {
    let mut encoded = payload(seed, id, class, generation);
    if encoded.len() >= minimum_bytes {
        return encoded;
    }
    let missing = minimum_bytes - encoded.len();
    let mixed = mix64(seed ^ id.rotate_left(19) ^ generation.rotate_left(37));
    encoded = serde_json::to_vec(&Record {
        record_id: id,
        class,
        generation,
        payload: format!("{mixed:016x}-{seed:016x}{}", "x".repeat(missing)),
    })
    .expect("generated contention record is serializable");
    debug_assert_eq!(encoded.len(), minimum_bytes);
    encoded
}

pub fn index_name(position: usize) -> String {
    format!("{INDEX_NAME_PREFIX}-{position:03}")
}

pub fn corpus_digest(seed: u64, stable: u64, mutable: u64) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"keldra.index-contention.corpus.v1\0");
    hash.update(seed.to_be_bytes());
    hash.update(stable.to_be_bytes());
    hash.update(mutable.to_be_bytes());
    for id in 0..stable {
        hash.update(stable_path(id));
        hash.update(payload(seed, id, "stable", 0));
    }
    for id in 0..mutable {
        hash.update(mutable_path(id));
        hash.update(payload(seed, id, "mutable", 0));
    }
    format!("sha256:{}", hex::encode(hash.finalize()))
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
        assert_eq!(corpus_digest(7, 4, 3), corpus_digest(7, 4, 3));
        assert_ne!(corpus_digest(7, 4, 3), corpus_digest(8, 4, 3));
        assert_ne!(payload(7, 2, "mutable", 1), payload(7, 2, "mutable", 2));
        let padded = payload_at_least(7, 2, "mutable", 1, 25_000);
        assert_eq!(padded.len(), 25_000);
        assert_eq!(
            payload_at_least(7, 2, "mutable", 1, 1),
            payload(7, 2, "mutable", 1)
        );
    }
}
