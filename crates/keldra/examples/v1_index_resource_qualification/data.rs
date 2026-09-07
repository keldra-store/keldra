use serde::Serialize;

pub const FIELD_COUNT: usize = 12;
// Keeps every partition below the public query page limit at the default
// production-shaped corpus size, so an exact cardinality pass needs one
// request per disjoint partition rather than retaining the corpus in memory.
pub const PARTITION_COUNT: u64 = 1_024;

const ECOSYSTEMS: [&str; 6] = ["cargo", "npm", "pypi", "maven", "go", "nuget"];
const SEVERITIES: [&str; 4] = ["low", "medium", "high", "critical"];
const SOURCES: [&str; 4] = ["feed-a", "feed-b", "feed-c", "feed-d"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordFlavor {
    Initial,
    Updated,
}

#[derive(Serialize)]
struct GeneratedRecord {
    record_id: u64,
    ecosystem: &'static str,
    package: String,
    severity: &'static str,
    active: bool,
    withdrawn: bool,
    score: f64,
    published_day: u64,
    modified_day: u64,
    sequence: u64,
    source: &'static str,
    partition: u64,
}

pub fn object_path(record_id: u64) -> String {
    format!("records/{record_id:012}.json")
}

pub fn payload(seed: u64, record_id: u64, flavor: RecordFlavor) -> Vec<u8> {
    let mixed = mix64(seed ^ record_id.rotate_left(17));
    let updated = flavor == RecordFlavor::Updated;
    let published_day = 18_000 + mixed % 2_000;
    let value = GeneratedRecord {
        record_id,
        ecosystem: ECOSYSTEMS[(mixed as usize) % ECOSYSTEMS.len()],
        package: format!("package-{:08x}", mixed as u32),
        severity: if updated {
            "updated"
        } else {
            SEVERITIES[((mixed >> 8) as usize) % SEVERITIES.len()]
        },
        active: if updated { true } else { mixed & 1 == 0 },
        withdrawn: !updated && record_id.is_multiple_of(97),
        score: ((mixed >> 16) % 10_001) as f64 / 100.0,
        published_day,
        modified_day: published_day + ((mixed >> 32) % 365) + u64::from(updated),
        sequence: record_id,
        source: SOURCES[((mixed >> 40) as usize) % SOURCES.len()],
        partition: record_id % PARTITION_COUNT,
    };
    serde_json::to_vec(&value).expect("generated record is JSON serializable")
}

pub fn is_active(seed: u64, record_id: u64) -> bool {
    mix64(seed ^ record_id.rotate_left(17)) & 1 == 0
}

pub fn partition_paths(records: u64, partition: u64) -> Vec<String> {
    (partition..records)
        .step_by(PARTITION_COUNT as usize)
        .map(object_path)
        .collect()
}

pub fn active_partition_paths(seed: u64, records: u64, partition: u64) -> Vec<String> {
    (partition..records)
        .step_by(PARTITION_COUNT as usize)
        .filter(|record_id| is_active(seed, *record_id))
        .map(object_path)
        .collect()
}

pub fn trailing_paths(records: u64, count: u64) -> Vec<String> {
    let start = records.saturating_sub(count);
    (start..records).map(object_path).collect()
}

pub const fn indexed_fields() -> [(&'static str, &'static str); FIELD_COUNT] {
    [
        ("record_id", "/record_id"),
        ("ecosystem", "/ecosystem"),
        ("package", "/package"),
        ("severity", "/severity"),
        ("active", "/active"),
        ("withdrawn", "/withdrawn"),
        ("score", "/score"),
        ("published_day", "/published_day"),
        ("modified_day", "/modified_day"),
        ("sequence", "/sequence"),
        ("source", "/source"),
        ("partition", "/partition"),
    ]
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        FIELD_COUNT, RecordFlavor, active_partition_paths, indexed_fields, object_path,
        partition_paths, payload, trailing_paths,
    };

    #[test]
    fn generated_records_are_deterministic_and_have_twelve_indexed_fields() {
        let first = payload(42, 7, RecordFlavor::Initial);
        assert_eq!(first, payload(42, 7, RecordFlavor::Initial));
        assert_ne!(first, payload(43, 7, RecordFlavor::Initial));
        assert_ne!(first, payload(42, 8, RecordFlavor::Initial));
        assert_ne!(first, payload(42, 7, RecordFlavor::Updated));

        let parsed: Value = serde_json::from_slice(&first).unwrap();
        let object = parsed.as_object().unwrap();
        assert_eq!(object.len(), FIELD_COUNT);
        for (name, pointer) in indexed_fields() {
            assert_eq!(pointer, format!("/{name}"));
            assert!(object.contains_key(name));
        }
    }

    #[test]
    fn expected_sets_are_exact_without_retaining_the_corpus() {
        assert_eq!(
            partition_paths(8_200, 5),
            vec![
                object_path(5),
                object_path(1_029),
                object_path(2_053),
                object_path(3_077),
                object_path(4_101),
                object_path(5_125),
                object_path(6_149),
                object_path(7_173),
                object_path(8_197),
            ]
        );
        assert!(
            active_partition_paths(99, 8_200, 5)
                .iter()
                .all(|path| partition_paths(8_200, 5).contains(path))
        );
        assert_eq!(
            trailing_paths(10, 3),
            vec![object_path(7), object_path(8), object_path(9)]
        );
    }
}
