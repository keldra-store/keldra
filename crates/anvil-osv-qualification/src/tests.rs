use super::*;
use std::{
    io::Write as _,
    sync::atomic::{AtomicUsize, Ordering},
};

fn test_archive(entries: &[(&str, &[u8])]) -> zip::ZipArchive<Cursor<Vec<u8>>> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    for (name, bytes) in entries {
        writer.start_file(*name, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    let mut bytes = writer.finish().unwrap();
    bytes.set_position(0);
    zip::ZipArchive::new(bytes).unwrap()
}

#[derive(Debug)]
struct PanicOnFirstCloneRead {
    bytes: Cursor<Vec<u8>>,
    clone_count: Arc<AtomicUsize>,
    panic_on_read: bool,
}

impl PanicOnFirstCloneRead {
    fn new(bytes: Cursor<Vec<u8>>) -> Self {
        Self {
            bytes,
            clone_count: Arc::new(AtomicUsize::new(0)),
            panic_on_read: false,
        }
    }
}

impl Clone for PanicOnFirstCloneRead {
    fn clone(&self) -> Self {
        let clone_index = self.clone_count.fetch_add(1, Ordering::Relaxed);
        Self {
            bytes: self.bytes.clone(),
            clone_count: self.clone_count.clone(),
            panic_on_read: clone_index == 0,
        }
    }
}

impl Read for PanicOnFirstCloneRead {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        assert!(!self.panic_on_read, "injected archive reader panic");
        self.bytes.read(buffer)
    }
}

impl Seek for PanicOnFirstCloneRead {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        assert!(!self.panic_on_read, "injected archive reader panic");
        self.bytes.seek(position)
    }
}

#[test]
fn parallel_archive_workers_preserve_entry_order_and_classification() {
    let archive = test_archive(&[
        ("a.json", br#"{"id":"A"}"#),
        ("notes.txt", b"ignored"),
        ("broken.json", b"{"),
        ("b.json", br#"{"id":"B","affected":[]}"#),
        ("missing-id.json", br#"{"summary":"malformed"}"#),
    ]);
    let mut observed = Vec::new();
    let timings = consume_archive_entries_in_order(archive, 4, 1024, |index, outcome| {
        let classification = match outcome {
            ArchiveEntryOutcome::Ignored => "ignored".to_string(),
            ArchiveEntryOutcome::Oversized => "oversized".to_string(),
            ArchiveEntryOutcome::Malformed { .. } => "malformed".to_string(),
            ArchiveEntryOutcome::Prepared { records, .. } => {
                format!("prepared:{}", records.source_record_id)
            }
        };
        observed.push((index, classification));
        Ok(())
    })
    .unwrap();

    assert_eq!(
        observed,
        [
            (0, "prepared:A".into()),
            (1, "ignored".into()),
            (2, "malformed".into()),
            (3, "prepared:B".into()),
            (4, "malformed".into()),
        ]
    );
    assert!(timings.inflate_read > Duration::ZERO);
    assert!(timings.json_prepare > Duration::ZERO);
}

#[test]
fn parallel_archive_workers_enforce_entry_and_total_decompressed_bounds() {
    let archive = test_archive(&[("large.json", br#"{"id":"too-large"}"#)]);
    let mut oversized = false;
    consume_archive_entries_in_order(archive, 4, 8, |_, outcome| {
        oversized = matches!(outcome, ArchiveEntryOutcome::Oversized);
        Ok(())
    })
    .unwrap();
    assert!(oversized);

    let mut report = ParsingReport {
        decompressed_json_bytes: MAX_DECOMPRESSED_JSON_BYTES,
        ..ParsingReport::default()
    };
    let error = add_decompressed_json_bytes(&mut report, 1).unwrap_err();
    assert!(error.to_string().contains("OSV decompressed JSON exceeds"));
}

#[test]
fn parallel_archive_consumer_error_cancels_bounded_workers() {
    let archive = test_archive(&[
        ("0.json", br#"{"id":"0"}"#),
        ("1.json", br#"{"id":"1"}"#),
        ("2.json", br#"{"id":"2"}"#),
        ("3.json", br#"{"id":"3"}"#),
        ("4.json", br#"{"id":"4"}"#),
        ("5.json", br#"{"id":"5"}"#),
        ("6.json", br#"{"id":"6"}"#),
        ("7.json", br#"{"id":"7"}"#),
    ]);
    let error = consume_archive_entries_in_order(archive, 4, 1024, |index, _| {
        ensure!(index < 2, "injected ordered consumer failure");
        Ok(())
    })
    .unwrap_err();
    assert_eq!(error.to_string(), "injected ordered consumer failure");
}

#[test]
fn parallel_archive_worker_panic_disconnects_without_hanging() {
    let archive = test_archive(&[("0.json", br#"{"id":"0"}"#), ("1.json", br#"{"id":"1"}"#)]);
    let mut bytes = archive.into_inner();
    bytes.set_position(0);
    let archive = zip::ZipArchive::new(PanicOnFirstCloneRead::new(bytes)).unwrap();
    let error = consume_archive_entries_in_order(archive, 2, 1024, |_, _| Ok(())).unwrap_err();
    assert!(format!("{error:#}").contains("OSV archive reader panicked"));
}

#[derive(Serialize)]
struct CloneReferenceContent<'a> {
    schema: &'static str,
    source_id: &'static str,
    source_record_id: &'a str,
    ecosystem: &'a str,
    package: &'a str,
    normalised_ecosystem: &'a str,
    normalised_package: &'a str,
    modified_at: &'a Option<String>,
    published_at: &'a Option<String>,
    withdrawn: bool,
    aliases: &'a [String],
    summary: &'a Option<String>,
    details: &'a Option<String>,
    state: &'a str,
    document: &'a Value,
}

fn prepare_records_serial(document: Value) -> Result<Vec<PreparedRecord>> {
    let PreparedRecordJobs {
        document,
        source_record_id,
        jobs,
        ..
    } = prepare_record_jobs(document)?;
    jobs.iter()
        .map(|job| prepare_record(&document, &source_record_id, job))
        .collect()
}

fn prepare_records_clone_reference(document: Value) -> Result<Vec<PreparedRecord>> {
    let PreparedRecordJobs {
        document,
        source_record_id,
        jobs,
        ..
    } = prepare_record_jobs(document)?;
    jobs.iter()
        .map(|job| prepare_record_clone_reference(&document, &source_record_id, job))
        .collect()
}

fn prepare_record_clone_reference(
    document: &Value,
    source_record_id: &str,
    job: &RecordJob,
) -> Result<PreparedRecord> {
    let mut scoped_document = document.clone();
    scoped_document
        .as_object_mut()
        .context("OSV document must be a JSON object")?
        .insert("affected".into(), Value::Array(job.affected.clone()));
    let normalised_ecosystem = job.ecosystem.trim().to_ascii_lowercase();
    let normalised_package = normalize_package_name(&job.ecosystem, &job.package);
    let record_identity_hash = digest_bytes(
        format!("osv\0{source_record_id}\0{normalised_ecosystem}\0{normalised_package}").as_bytes(),
    );
    let modified_at = string_field(document, "modified");
    let modified_day = timestamp_day(modified_at.as_deref());
    let published_at = string_field(document, "published");
    let withdrawn = document
        .get("withdrawn")
        .is_some_and(|value| !value.is_null());
    let aliases = string_array(document, "aliases");
    let summary = string_field(document, "summary");
    let details = string_field(document, "details");
    let state = if withdrawn { "withdrawn" } else { "active" };
    let content_sha256 = digest_bytes(&serde_json::to_vec(&CloneReferenceContent {
        schema: "anvil.osv.source-record.v1",
        source_id: "osv",
        source_record_id,
        ecosystem: &job.ecosystem,
        package: &job.package,
        normalised_ecosystem: &normalised_ecosystem,
        normalised_package: &normalised_package,
        modified_at: &modified_at,
        published_at: &published_at,
        withdrawn,
        aliases: &aliases,
        summary: &summary,
        details: &details,
        state,
        document: &scoped_document,
    })?);
    let record = OsvSourceRecord {
        schema: "anvil.osv.source-record.v1".into(),
        source_id: "osv".into(),
        source_record_id: source_record_id.into(),
        record_identity_hash,
        content_sha256,
        ecosystem: job.ecosystem.clone(),
        package: job.package.clone(),
        normalised_ecosystem: normalised_ecosystem.clone(),
        normalised_package,
        modified_at,
        modified_day: modified_day.clone(),
        published_at,
        withdrawn,
        aliases,
        summary,
        details,
        state: state.into(),
        document: scoped_document,
    };
    Ok(PreparedRecord {
        encoded: serde_json::to_vec(&record)?,
        normalised_ecosystem,
        modified_day,
    })
}

fn decode_record(prepared: &PreparedRecord) -> OsvSourceRecord {
    serde_json::from_slice(&prepared.encoded).unwrap()
}

fn test_shard_builders() -> Vec<ShardBuilder> {
    (0..6)
        .map(|index| {
            let prepared = prepare_records_serial(serde_json::json!({
                "id": format!("GHSA-compression-{index}"),
                "modified": format!("2026-07-{:02}T12:00:00Z", index + 1),
                "details": "deterministic shard compression",
                "affected": [{
                    "package": {"ecosystem": "npm", "name": format!("example-{index}")},
                    "versions": ["1.0.0", "1.0.1"]
                }]
            }))
            .unwrap()
            .remove(0);
            let mut builder = ShardBuilder::new("npm".into(), index, 1024);
            builder.push(prepared);
            builder
        })
        .collect()
}

#[test]
fn parallel_shard_compression_is_byte_identical_to_serial() {
    let expected = test_shard_builders()
        .into_iter()
        .map(ShardBuilder::finish)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let builders = test_shard_builders();
    let (sender, mut receiver) = mpsc::channel(builders.len());
    let mut compressor = ShardCompressor::start(3, sender).unwrap();
    for builder in builders {
        compressor.submit(builder).unwrap();
    }
    compressor.finish().unwrap();

    let mut actual = Vec::new();
    while let Some(shard) = receiver.blocking_recv() {
        actual.push(shard);
    }
    assert_eq!(actual, expected);
}

#[test]
fn shard_compression_flushes_a_partial_wave_in_submission_order() {
    let expected = test_shard_builders()
        .into_iter()
        .take(5)
        .map(ShardBuilder::finish)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let builders = test_shard_builders();
    let (sender, mut receiver) = mpsc::channel(builders.len());
    let mut compressor = ShardCompressor::start(3, sender).unwrap();
    for builder in builders.into_iter().take(5) {
        compressor.submit(builder).unwrap();
    }
    assert_eq!(compressor.pending.len(), 2);
    compressor.finish().unwrap();

    let mut actual = Vec::new();
    while let Some(shard) = receiver.blocking_recv() {
        actual.push(shard);
    }
    assert_eq!(actual, expected);
}

#[test]
fn shard_compression_rejects_zero_workers() {
    let (sender, _receiver) = mpsc::channel(1);
    let error = match ShardCompressor::start(0, sender) {
        Ok(_) => panic!("zero compression workers must be rejected"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "shard compression requires at least one worker"
    );
}

#[test]
fn shard_compression_never_retains_more_than_one_bounded_wave() {
    let builders = test_shard_builders();
    let (sender, _receiver) = mpsc::channel(builders.len());
    let mut compressor = ShardCompressor::start(3, sender).unwrap();
    for builder in builders {
        compressor.submit(builder).unwrap();
        assert!(compressor.pending.len() < compressor.worker_count);
    }
    compressor.finish().unwrap();
}

#[test]
fn shard_compression_reports_a_stopped_consumer_without_hanging() {
    let builder = test_shard_builders().remove(0);
    let (sender, receiver) = mpsc::channel(1);
    drop(receiver);
    let mut compressor = ShardCompressor::start(1, sender).unwrap();
    let error = compressor.submit(builder).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("OSV shard consumer stopped before parsing completed")
    );
}

#[test]
fn borrowed_record_encoding_matches_clone_reference_for_mixed_scopes() {
    let document: Value = serde_json::from_str(
        r#"{
                "z": 1.0,
                "summary": "mixed package, \"quoted\", and unicode Δ records",
                "modified": "2026-07-14T12:00:00Z,\"published_at\":false",
                "id": "GHSA-mixed\\path,\"ecosystem\":false",
                "affected": [
                    {"package": {"ecosystem": "npm", "name": "Zed"}, "versions": ["1"]},
                    {"versions": ["unscoped"]},
                    {"package": {"ecosystem": "Cargo", "name": "crate-a"}, "versions": ["2"]},
                    {"package": {"ecosystem": "npm", "name": "zed"}, "versions": ["3"]}
                ],
                "a": true
            }"#,
    )
    .unwrap();
    let expected = prepare_records_clone_reference(document.clone()).unwrap();
    let actual = prepare_records_serial(document).unwrap();

    assert_eq!(actual, expected);
    let records = actual.iter().map(decode_record).collect::<Vec<_>>();
    assert_eq!(
        records
            .iter()
            .map(|record| record.ecosystem.as_str())
            .collect::<Vec<_>>(),
        ["crates.io", "npm", "unscoped"]
    );
    assert_eq!(records[1].document["affected"].as_array().unwrap().len(), 2);
    assert_eq!(records[2].document["affected"].as_array().unwrap().len(), 1);
}

#[test]
fn scoped_document_emits_affected_at_its_lexical_map_position() {
    let prepared = prepare_record_jobs(
        serde_json::from_str(
            r#"{"z":1.0,"middle":2,"id":"GHSA-lexical","affected":[{"versions":["1"]}],"a":1}"#,
        )
        .unwrap(),
    )
    .unwrap();
    let scoped = ScopedDocument {
        base: prepared.document.as_object().unwrap(),
        affected: &prepared.jobs[0].affected,
    };
    assert_eq!(
        serde_json::to_vec(&scoped).unwrap(),
        br#"{"a":1,"affected":[{"versions":["1"]}],"id":"GHSA-lexical","middle":2,"z":1.0}"#
    );
}

#[test]
fn exact_osv_transform_materialises_package_records() {
    let records = prepare_records_serial(serde_json::json!({
        "id": "GHSA-test",
        "modified": "2026-07-14T12:00:00Z",
        "aliases": ["CVE-2026-1", "CVE-2026-1"],
        "affected": [
            {"package": {"ecosystem": "npm", "name": "Example"}, "versions": ["1.0.0"]},
            {"package": {"ecosystem": "npm", "name": "example"}, "versions": ["1.0.1"]},
            {"package": {"ecosystem": "Go", "name": "example.org/module"}}
        ]
    }))
    .unwrap();

    assert_eq!(records.len(), 2);
    let npm = records
        .iter()
        .map(decode_record)
        .find(|record| record.ecosystem == "npm")
        .unwrap();
    assert_eq!(npm.normalised_package, "example");
    assert_eq!(npm.aliases, ["CVE-2026-1"]);
    assert_eq!(npm.modified_day, "2026-07-14");
    assert_eq!(npm.document["affected"].as_array().unwrap().len(), 2);
    let content = CloneReferenceContent {
        schema: "anvil.osv.source-record.v1",
        source_id: "osv",
        source_record_id: &npm.source_record_id,
        ecosystem: &npm.ecosystem,
        package: &npm.package,
        normalised_ecosystem: &npm.normalised_ecosystem,
        normalised_package: &npm.normalised_package,
        modified_at: &npm.modified_at,
        published_at: &npm.published_at,
        withdrawn: npm.withdrawn,
        aliases: &npm.aliases,
        summary: &npm.summary,
        details: &npm.details,
        state: &npm.state,
        document: &npm.document,
    };
    assert_eq!(
        npm.content_sha256,
        digest_bytes(&serde_json::to_vec(&content).unwrap())
    );
}

#[test]
fn streaming_content_digest_matches_materialised_json_bytes() {
    let prepared = prepare_record_jobs(serde_json::json!({
        "z": 1.0,
        "published": "2026-07-13T12:00:00Z",
        "modified": "2026-07-14T12:00:00Z",
        "id": "GHSA-streaming-digest",
        "aliases": ["CVE-2026-1"],
        "affected": [{
            "package": {"ecosystem": "npm", "name": "Example"},
            "versions": ["1.0.0"]
        }],
        "a": true
    }))
    .unwrap();
    let job = &prepared.jobs[0];
    let normalised_ecosystem = job.ecosystem.trim().to_ascii_lowercase();
    let normalised_package = normalize_package_name(&job.ecosystem, &job.package);
    let modified_at = string_field(&prepared.document, "modified");
    let published_at = string_field(&prepared.document, "published");
    let aliases = string_array(&prepared.document, "aliases");
    let summary = string_field(&prepared.document, "summary");
    let details = string_field(&prepared.document, "details");
    let content = OsvSourceRecordContent {
        schema: "anvil.osv.source-record.v1",
        source_id: "osv",
        source_record_id: &prepared.source_record_id,
        ecosystem: &job.ecosystem,
        package: &job.package,
        normalised_ecosystem: &normalised_ecosystem,
        normalised_package: &normalised_package,
        modified_at: &modified_at,
        published_at: &published_at,
        withdrawn: false,
        aliases: &aliases,
        summary: &summary,
        details: &details,
        state: "active",
        document: ScopedDocument {
            base: prepared.document.as_object().unwrap(),
            affected: &job.affected,
        },
    };

    assert_eq!(
        digest_json(&content).unwrap(),
        digest_bytes(&serde_json::to_vec(&content).unwrap())
    );
}

#[test]
fn pinned_normalised_transform_stabilises_key_order_and_number_spelling() {
    // The qualification shard schema intentionally uses serde_json here.
    // Stable map ordering and number spelling are asserted below.
    let first: Value =
        serde_json::from_str(r#"{"z":1.0,"id":"GHSA-canonical","a":1,"affected":[]}"#).unwrap();
    let second: Value =
        serde_json::from_str(r#"{"affected":[],"a":1,"id":"GHSA-canonical","z":1.0}"#).unwrap();
    let first = decode_record(&prepare_records_serial(first).unwrap().remove(0));
    let second = decode_record(&prepare_records_serial(second).unwrap().remove(0));
    assert_eq!(first, second);

    let scoped = serde_json::to_vec(&first.document).unwrap();
    assert_eq!(
        scoped,
        br#"{"a":1,"affected":[],"id":"GHSA-canonical","z":1.0}"#
    );
}

#[test]
fn shard_encoding_is_content_addressed_zstd_6_ndjson() {
    let mut records = prepare_records_serial(serde_json::json!({
        "id": "GHSA-shard",
        "affected": [{"package": {"ecosystem": "npm", "name": "example"}}]
    }))
    .unwrap();
    let mut builder = ShardBuilder::new("npm".into(), 0, MIN_SHARD_UNCOMPRESSED_BYTES);
    builder.push(records.remove(0));
    let shard = builder.finish().unwrap();
    let decoded = zstd::stream::decode_all(Cursor::new(&shard.encoded_payload)).unwrap();

    assert_eq!(digest_bytes(&decoded), shard.records_sha256);
    assert_eq!(digest_bytes(&shard.encoded_payload), shard.encoded_sha256);
    assert_eq!(decoded.last(), Some(&b'\n'));
    assert_eq!(
        shard_path(&shard.records_sha256),
        format!(
            "shards/v1/{}/{}.ndjson.zst",
            &shard.records_sha256[..2],
            shard.records_sha256
        )
    );
}

#[test]
fn source_definition_has_exact_schema_and_content_addressed_identity() {
    let path = source_definition_path();
    assert_eq!(
        path,
        format!(
            "entities/source-definition/{}/current.json",
            digest_bytes(b"source-definition\0osv")
        )
    );
    let definition = SourceDefinition {
        schema: "anvil.osv.source-definition.v1".into(),
        source_id: "osv".into(),
        source_bucket: OSV_QUALIFICATION_BUCKET.into(),
        canonical_url: DEFAULT_SOURCE_URL.into(),
        publisher: "Google OSV".into(),
        cadence_hours: 6,
        authentication_profile: "public-https".into(),
        downloaded_artifact_retention: "ephemeral-until-shard-manifest-commit".into(),
        redistribution_policy: "record-level-upstream-rights".into(),
        enabled: true,
    };
    let value = serde_json::to_value(definition).unwrap();
    assert_eq!(value["schema"], "anvil.osv.source-definition.v1");
    assert_eq!(value["authentication_profile"], "public-https");
    assert_eq!(value["source_bucket"], OSV_QUALIFICATION_BUCKET);
}

#[test]
fn snapshot_identity_requires_an_explicit_canonical_day() {
    assert!(validate_snapshot_day("2026-07-18").is_ok());
    for invalid in ["", "20260718", "2026-7-18", " 2026-07-18"] {
        assert!(validate_snapshot_day(invalid).is_err());
    }
}

#[test]
fn qualification_requires_an_exact_anvil_commit() {
    assert!(validate_git_commit("--anvil-commit", &"a".repeat(40)).is_ok());
    assert!(validate_git_commit("--anvil-commit", &"A".repeat(40)).is_ok());
    let short = "a".repeat(39);
    let non_hex = "g".repeat(40);
    for invalid in ["", "main", short.as_str(), non_hex.as_str()] {
        assert!(validate_git_commit("--anvil-commit", invalid).is_err());
    }
}

#[test]
fn deterministic_batch_boundary_honours_items_and_payload() {
    assert!(!batch_would_overflow(2, 20, 10, 3, 30));
    assert!(batch_would_overflow(3, 20, 1, 3, 30));
    assert!(batch_would_overflow(2, 20, 11, 3, 30));
}

#[test]
fn single_node_qualification_accepts_only_local_durability() {
    assert!(<DurabilityArgument as clap::ValueEnum>::from_str("local", false).is_ok());
    for rejected in ["replicated", "quorum", "anything", " local", "local "] {
        let parsed = <DurabilityArgument as clap::ValueEnum>::from_str(rejected, false);
        let accepted = matches!(parsed, Ok(DurabilityArgument::Local));
        assert!(!accepted, "unexpectedly accepted {rejected:?}");
    }
}

#[test]
fn qualification_rejects_credentials_the_server_cannot_accept() {
    assert!(validate_client_secret_value(&"s".repeat(32)).is_ok());
    assert!(validate_client_secret_value(&"s".repeat(4 * 1024)).is_ok());
    assert!(validate_client_secret_value(&"s".repeat(31)).is_err());
    assert!(validate_client_secret_value(&"s".repeat(4 * 1024 + 1)).is_err());
}
