#![recursion_limit = "256"]

use std::{fs, path::PathBuf};

fn workspace_file(path: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("server crate has workspace parent")
        .to_path_buf();
    fs::read_to_string(root.join(path)).unwrap_or_else(|err| panic!("read {path}: {err}"))
}

#[test]
fn writer_segment_envelope_validates_canonical_range_index() {
    let formats = workspace_file("anvil-core/src/formats.rs");
    for required in [
        "pub struct RangeIndexEntry",
        "pub fn encode_range_index",
        "pub fn decode_range_index",
        "decode_range_index(range_index)?",
        "segment_hash_input.extend_from_slice(range_index)",
        "canonical_writer_segment_header_bytes",
        "canonical writer segment trailer",
    ] {
        assert!(
            formats.contains(required),
            "formats.rs must retain canonical writer range-index support: missing {required}"
        );
    }
}

#[test]
fn writer_segment_payload_exposes_corestore_byte_metadata() {
    let formats = workspace_file("anvil-core/src/formats.rs");
    for required in [
        "pub struct EncodedWriterSegment",
        "pub bytes: Vec<u8>",
        "pub family: FileFamily",
        "pub segment_hash: Hash32",
        "pub body_hash: Hash32",
        "pub record_count: u64",
        "pub first_record_hash: Hash32",
        "pub last_record_hash: Hash32",
    ] {
        assert!(
            formats.contains(required),
            "encoded writer segments must expose CoreStore byte-pipeline metadata: missing {required}"
        );
    }
}

#[test]
fn writer_segment_emitters_do_not_publish_empty_range_index_placeholders() {
    for path in [
        "anvil-core/src/full_text_segment.rs",
        "anvil-core/src/vector_segment.rs",
        "anvil-core/src/typed_field_segment.rs",
        "anvil-core/src/authz_segment.rs",
        "anvil-core/src/personaldb_segment.rs",
        "anvil-core/src/personaldb_row_index.rs",
        "anvil-core/src/git_source_index.rs",
        "anvil-core/src/metadata_journal/helpers.rs",
    ] {
        let source = workspace_file(path);
        assert!(
            source.contains("single_body_range_index"),
            "{path} must emit a canonical range index when writing writer segments"
        );
        assert!(
            !source.contains("range_index: Vec::new()") && !source.contains("range_index: vec![]"),
            "{path} must not pass an empty placeholder range index to the writer segment builder"
        );
    }
}

#[test]
fn format_aware_segments_are_corestore_writer_outputs_not_json_envelopes() {
    for path in [
        "anvil-core/src/full_text_segment.rs",
        "anvil-core/src/vector_segment.rs",
        "anvil-core/src/typed_field_segment.rs",
        "anvil-core/src/authz_segment.rs",
    ] {
        let source = workspace_file(path);
        for required in [
            "build_writer_segment_logical_file(",
            "decode_writer_segment(",
            "encode_writer_segment_header",
            "write_format_build_output(WriterBuildOutput",
            "written_object_refs",
        ] {
            assert!(
                source.contains(required),
                "{path} must route durable segment bytes through the CoreStore writer pipeline: missing {required}"
            );
        }
        assert_no_legacy_segment_envelopes(path, &source);
    }

    let personaldb = workspace_file("anvil-core/src/personaldb_segment.rs");
    for required in [
        "build_writer_segment_logical_file(",
        "decode_writer_segment(",
        "encode_writer_segment_header",
        "prepare_personaldb_logical_file_as_data_locator(",
        "plan.stage_data_locator_row(&row)?",
        "into_write_logical_file_request()?",
    ] {
        assert!(
            personaldb.contains(required),
            "personaldb segment must route durable segment bytes through the CoreStore writer pipeline: missing {required}"
        );
    }
    assert_no_legacy_segment_envelopes("anvil-core/src/personaldb_segment.rs", &personaldb);
}

fn assert_no_legacy_segment_envelopes(path: &str, source: &str) {
    for forbidden in [
        "BinaryEnvelopeHeader",
        "BinaryFileFooter",
        "COMMON_HEADER_LEN",
        "COMMON_FOOTER_LEN",
        "header_json",
        "serde_json::to_vec(&header)",
        "serde_json::from_slice(&envelope.header_json)",
        "serde_json::to_vec_pretty",
    ] {
        assert!(
            !source.contains(forbidden),
            "{path} must not retain ad-hoc JSON durable segment envelope encoding: {forbidden}"
        );
    }
}

#[test]
fn indexed_application_json_is_allowlisted_inside_binary_segment_records() {
    let full_text = workspace_file("anvil-core/src/full_text_segment.rs");
    let typed = workspace_file("anvil-core/src/typed_field_segment.rs");
    let vector_formats = workspace_file("anvil-core/src/formats/vector.rs");

    assert!(
        full_text.contains("header_field_bytes(\"tokenizer_json\"")
            && full_text.contains("canonical_json_bytes(&header.tokenizer)")
            && full_text.contains("required_header_bytes(header, \"tokenizer_json\")"),
        "full text tokenizer/scorer JSON metadata must be canonical bytes inside the protobuf header"
    );
    assert!(
        typed.contains("StoredJsonValueProto")
            && typed.contains("encode_stored_fields")
            && typed.contains("ensure_stored_json_fields_sorted")
            && typed.contains("canonical_json_string"),
        "typed-field JSON source values must be stored as deterministic protobuf rows, not durable JSON files"
    );
    assert!(
        vector_formats.contains("fn canonical_json_bytes")
            && vector_formats.contains("VectorIndexDefinition::from_json"),
        "vector index-definition JSON support is application/operator JSON, not writer segment JSON"
    );
}

#[test]
fn registry_gateway_records_are_protobuf_not_json_logical_files() {
    let gateway_store = workspace_file("anvil-core/src/gateway_store.rs");
    let codec = workspace_file("anvil-core/src/gateway_store/record_codec.rs");
    let helpers = workspace_file("anvil-core/src/gateway_store/helpers.rs");

    assert!(
        gateway_store.contains("encode_gateway_record")
            && gateway_store.contains("decode_gateway_record"),
        "gateway registry records must be encoded through the deterministic protobuf codec"
    );
    assert!(
        codec.contains("impl GatewayRecordCodec for GatewayRepositoryRecord")
            && codec.contains("impl GatewayRecordCodec for GatewayBlobRecord")
            && codec.contains("impl GatewayRecordCodec for GatewayTagRecord")
            && codec.contains("impl GatewayRecordCodec for GatewayUploadSessionRecord")
            && codec.contains("impl GatewayRecordCodec for GatewayCredentialRecord")
            && codec.contains("impl GatewayRecordCodec for GatewayMountRecord")
            && codec.contains("impl GatewayRecordCodec for GatewayAuditRecord"),
        "all gateway/registry durable record families must use the protobuf codec"
    );
    for forbidden in [
        "serde_json::to_vec_pretty",
        "serde_json::to_vec(&record)",
        "serde_json::from_slice",
        "serde_json::to_value(record)",
    ] {
        assert!(
            !gateway_store.contains(forbidden),
            "gateway_store.rs must not retain JSON durable record encoding: {forbidden}"
        );
        assert!(
            !helpers.contains(forbidden),
            "gateway_store helpers must not retain JSON durable record hashing/encoding: {forbidden}"
        );
    }
}

#[test]
fn format_aware_writer_segments_pass_boundary_values_to_corestore() {
    for path in [
        "anvil-core/src/full_text_segment.rs",
        "anvil-core/src/vector_segment.rs",
        "anvil-core/src/typed_field_segment.rs",
    ] {
        let source = workspace_file(path);
        for required in [
            "pub boundary_values: &'a [CoreBoundaryValue]",
            "boundary_values: write.boundary_values.to_vec()",
            "WriterSegmentBuildInput",
        ] {
            assert!(
                source.contains(required),
                "{path} must pass writer boundary values into the CoreStore byte pipeline: missing {required}"
            );
        }
    }

    let writer = workspace_file("anvil-core/src/formats/writer.rs");
    for required in [
        "WriteLogicalFileRequest",
        "pub fn into_write_logical_file_request(self) -> Result<WriteLogicalFileRequest>",
        "boundary_values: self.boundary_values",
    ] {
        assert!(
            writer.contains(required),
            "central writer segment conversion must preserve boundary values: missing {required}"
        );
    }

    let builder = workspace_file("anvil-core/src/index_builder.rs");
    let helpers = workspace_file("anvil-core/src/index_builder/helpers.rs");
    for required in [
        "boundary_values_for_objects(storage, &objects).await?",
        "boundary_values: &boundary_values",
        "manifest.boundary_values.into_iter()",
    ] {
        assert!(
            format!("{}\n{}", builder, helpers).contains(required),
            "index builders must derive boundary values from source object manifests: missing {required}"
        );
    }
}
