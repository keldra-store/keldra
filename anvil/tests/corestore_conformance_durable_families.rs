#![recursion_limit = "256"]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct DurableFeatureFamily {
    name: &'static str,
    source_files: &'static [&'static str],
    required_terms: &'static [&'static str],
    corestore_terms: &'static [&'static str],
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil crate has workspace parent")
        .to_path_buf()
}

fn read_workspace_file(relative: &str) -> String {
    let path = workspace_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn production_source(relative: &str) -> String {
    strip_cfg_test_modules(&read_workspace_file(relative))
}

fn strip_cfg_test_modules(source: &str) -> String {
    let mut out = String::new();
    let mut pending_cfg_test = false;
    let mut skipping_test_module = false;
    let mut depth: i32 = 0;

    for line in source.lines() {
        let trimmed = line.trim_start();

        if skipping_test_module {
            depth += brace_delta(line);
            if depth <= 0 {
                skipping_test_module = false;
                depth = 0;
            }
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_cfg_test = true;
            continue;
        }

        if pending_cfg_test && trimmed.starts_with("mod tests") {
            skipping_test_module = true;
            depth = brace_delta(line);
            if depth <= 0 {
                skipping_test_module = false;
                depth = 0;
            }
            pending_cfg_test = false;
            continue;
        }

        if pending_cfg_test {
            out.push_str("#[cfg(test)]\n");
            pending_cfg_test = false;
        }

        out.push_str(line);
        out.push('\n');
    }

    out
}

fn brace_delta(line: &str) -> i32 {
    let opens = line.as_bytes().iter().filter(|byte| **byte == b'{').count() as i32;
    let closes = line.as_bytes().iter().filter(|byte| **byte == b'}').count() as i32;
    opens - closes
}

#[test]
fn rfc_0006_no_durable_bypass_feature_families_are_corestore_backed() {
    let families = [
        DurableFeatureFamily {
            name: "object_payload",
            source_files: &["anvil-core/src/object_manager.rs"],
            required_terms: &["put_object", "PutBlob", "anvil.core.object_data_target.v1"],
            corestore_terms: &["CoreStore", ".put_blob"],
        },
        DurableFeatureFamily {
            name: "object_metadata",
            source_files: &[
                "anvil-core/src/metadata_journal.rs",
                "anvil-core/src/metadata_journal/object_mutation.rs",
            ],
            required_terms: &["ObjectMetadataRecord", "append_object_mutation"],
            corestore_terms: &["CoreStore", "CoreMutationBatch", ".commit_mutation_batch"],
        },
        DurableFeatureFamily {
            name: "bucket_metadata",
            source_files: &["anvil-core/src/bucket_journal.rs"],
            required_terms: &["BucketJournalBody", "append_bucket_mutation"],
            corestore_terms: &["CoreStore", "CoreMutationBatch", ".commit_mutation_batch"],
        },
        DurableFeatureFamily {
            name: "object_link",
            source_files: &[
                "anvil-core/src/object_links.rs",
                "anvil-core/src/persistence/objects.rs",
                "anvil-core/src/metadata_journal.rs",
                "anvil-core/src/metadata_journal/object_mutation.rs",
            ],
            required_terms: &[
                "put_object_link",
                "ObjectEntryKind::Link",
                "append_object_mutation_with_permit",
            ],
            corestore_terms: &["CoreStore", "CoreMutationBatch", ".commit_mutation_batch"],
        },
        DurableFeatureFamily {
            name: "append_stream",
            source_files: &["anvil-core/src/append_journal.rs"],
            required_terms: &["AppendStreamRecord", "append_stream_record"],
            corestore_terms: &[
                "CoreStore",
                "CoreMutationBatch",
                ".commit_mutation_batch",
                ".read_stream",
            ],
        },
        DurableFeatureFamily {
            name: "task_lease",
            source_files: &["anvil-core/src/task_lease.rs"],
            required_terms: &["TaskLease", "TaskLeaseOwner", "fence_token"],
            corestore_terms: &[
                "CoreStore",
                "CoreMutationBatch",
                "CoreMutationOperation::CoreMetaPut",
                ".commit_mutation_batch",
                "TABLE_TASK_LEASE_ROW",
            ],
        },
        DurableFeatureFamily {
            name: "authz_schema",
            source_files: &[
                "anvil-core/src/authz_schema.rs",
                "anvil-core/src/authz_realm_schema.rs",
            ],
            required_terms: &["AuthzNamespaceSchemaRecord", "write_authz_namespace_schema"],
            corestore_terms: &[
                "CoreStore",
                "CoreMetaBatchOp",
                "commit_coremeta_batch_for_storage",
            ],
        },
        DurableFeatureFamily {
            name: "authz_tuple",
            source_files: &["anvil-core/src/authz_journal.rs"],
            required_terms: &["AuthzTuple", "write_authz_tuple", "append_authz_tuple"],
            corestore_terms: &[
                "CoreStore",
                "CoreMutationBatch",
                ".commit_mutation_batch",
                ".read_stream",
            ],
        },
        DurableFeatureFamily {
            name: "authz_derived_index",
            source_files: &["anvil-core/src/authz_userset_index.rs"],
            required_terms: &["AuthzDerivedUsersetIndex", "write_derived_userset_index"],
            corestore_terms: &[
                "CoreStore",
                ".put_format_blob(",
                "WriterFamily::Authz",
                "CoreMetaStore",
            ],
        },
        DurableFeatureFamily {
            name: "path_index",
            source_files: &[
                "anvil-core/src/index_builder.rs",
                "anvil-core/src/typed_field_segment.rs",
            ],
            required_terms: &[
                "build_metadata_backed_index",
                "path",
                "write_typed_field_segment",
            ],
            corestore_terms: &[
                "CoreStore",
                "write_index_segment_coremeta_record",
                ".write_format_build_output",
            ],
        },
        DurableFeatureFamily {
            name: "typed_field_index",
            source_files: &[
                "anvil-core/src/index_builder.rs",
                "anvil-core/src/typed_field_segment.rs",
            ],
            required_terms: &[
                "build_typed_json_index",
                "write_typed_field_segment",
                "EncodedTypedValue",
            ],
            corestore_terms: &[
                "CoreStore",
                "write_index_segment_coremeta_record",
                ".write_format_build_output",
            ],
        },
        DurableFeatureFamily {
            name: "full_text_index",
            source_files: &[
                "anvil-core/src/index_builder.rs",
                "anvil-core/src/full_text_segment.rs",
            ],
            required_terms: &[
                "build_full_text_index",
                "write_full_text_segment",
                "FullTextSegment",
            ],
            corestore_terms: &[
                "CoreStore",
                "write_index_segment_coremeta_record",
                ".write_format_build_output",
            ],
        },
        DurableFeatureFamily {
            name: "vector_index",
            source_files: &[
                "anvil-core/src/index_builder.rs",
                "anvil-core/src/vector_segment.rs",
            ],
            required_terms: &[
                "build_vector_index",
                "write_vector_segment",
                "VectorSegment",
            ],
            corestore_terms: &[
                "CoreStore",
                "write_index_segment_coremeta_record",
                ".write_format_build_output",
            ],
        },
        DurableFeatureFamily {
            name: "package_repository",
            source_files: &[
                "anvil-core/src/gateway_store.rs",
                "anvil-core/src/gateway_store/metadata_rows.rs",
                "anvil-core/src/gateway_store/record_codec.rs",
            ],
            required_terms: &[
                "GatewayRepositoryRecord",
                "GatewayRepositoryRecordProto",
                "create_gateway_repository",
            ],
            corestore_terms: &[
                "CoreStore",
                "CoreMetaBatchOp",
                ".commit_coremeta_root_groups",
            ],
        },
        DurableFeatureFamily {
            name: "package_blob",
            source_files: &[
                "anvil-core/src/gateway_store.rs",
                "anvil-core/src/gateway_store/metadata_rows.rs",
            ],
            required_terms: &["GatewayBlobRecord", "put_gateway_blob"],
            corestore_terms: &[
                "CoreStore",
                ".write_logical_file_with_locator",
                "CoreMetaBatchOp",
                ".commit_coremeta_root_groups",
            ],
        },
        DurableFeatureFamily {
            name: "gateway_mount",
            source_files: &[
                "anvil-core/src/gateway_store.rs",
                "anvil-core/src/gateway_store/mount_routes.rs",
            ],
            required_terms: &[
                "GatewayMountRecord",
                "put_gateway_mount_record",
                "resolve_gateway_mount",
            ],
            corestore_terms: &[
                "CoreStore",
                "CoreMutationBatch",
                "CoreMutationOperation::CoreMetaPut",
                ".commit_mutation_batch",
                ".scan_coremeta_prefix_page",
            ],
        },
        DurableFeatureFamily {
            name: "mesh_route",
            source_files: &[
                "anvil-core/src/mesh_directory.rs",
                "anvil-core/src/mesh_directory/helpers.rs",
                "anvil-core/src/mesh_directory/record_proto.rs",
            ],
            required_terms: &[
                "BucketLocatorDescriptor",
                "append_control_mutation",
                "rebuild_routing_record_projection_from_proto",
                "encode_deterministic_proto",
            ],
            corestore_terms: &["CoreStore", "CoreMutationBatch", ".commit_mutation_batch"],
        },
        DurableFeatureFamily {
            name: "node_lifecycle",
            source_files: &["anvil-core/src/mesh_lifecycle.rs"],
            required_terms: &["NodeDescriptor", "NodeDrainDescriptor"],
            corestore_terms: &["CoreStore", "CoreMutationBatch", ".commit_mutation_batch"],
        },
        DurableFeatureFamily {
            name: "region_lifecycle",
            source_files: &[
                "anvil-core/src/mesh_lifecycle.rs",
                "anvil-core/src/mesh_lifecycle/region.rs",
                "anvil-core/src/mesh_lifecycle/topology_mutation.rs",
            ],
            required_terms: &[
                "RegionDescriptor",
                "transition_region_with_control",
                "ensure_region_drain_completion_is_supported",
                "commit_topology_mutation",
            ],
            corestore_terms: &["CoreStore", "CoreMutationBatch", ".commit_mutation_batch"],
        },
        DurableFeatureFamily {
            name: "embedded_database_snapshot",
            source_files: &[
                "anvil-core/src/personaldb_snapshot_store.rs",
                "anvil-core/src/personaldb_snapshot_builder.rs",
                "anvil-core/src/personaldb_coremeta.rs",
            ],
            required_terms: &["write_personaldb_snapshot", "personaldb_snapshot_object"],
            corestore_terms: &[
                "CoreStore",
                ".write_logical_file_with_locator",
                "write_personaldb_data_locator_row",
            ],
        },
        DurableFeatureFamily {
            name: "embedded_database_changeset",
            source_files: &[
                "anvil-core/src/personaldb_commit_store.rs",
                "anvil-core/src/personaldb_segment.rs",
                "anvil-core/src/personaldb_coremeta.rs",
            ],
            required_terms: &[
                "write_personaldb_changeset_payload",
                "personaldb_changeset_payload",
            ],
            corestore_terms: &[
                "CoreStore",
                ".write_logical_file_with_locator",
                "write_personaldb_data_locator_row",
            ],
        },
        DurableFeatureFamily {
            name: "audit_record",
            source_files: &["anvil-core/src/admin_audit.rs"],
            required_terms: &["AdminAuditEvent", "append_audit_event"],
            corestore_terms: &[
                "CoreStore",
                "CoreMutationOperation::StreamAppend",
                ".commit_mutation_batch",
                ".scan_coremeta_prefix_page",
                ".stream_head_sequence",
            ],
        },
    ];

    let expected_names: BTreeSet<_> = [
        "object_payload",
        "object_metadata",
        "bucket_metadata",
        "object_link",
        "append_stream",
        "task_lease",
        "authz_schema",
        "authz_tuple",
        "authz_derived_index",
        "path_index",
        "typed_field_index",
        "full_text_index",
        "vector_index",
        "package_repository",
        "package_blob",
        "gateway_mount",
        "mesh_route",
        "node_lifecycle",
        "region_lifecycle",
        "embedded_database_snapshot",
        "embedded_database_changeset",
        "audit_record",
    ]
    .into_iter()
    .collect();
    let actual_names: BTreeSet<_> = families.iter().map(|family| family.name).collect();
    assert_eq!(
        actual_names, expected_names,
        "RFC 21.1 feature family coverage changed"
    );

    let mut failures = BTreeMap::new();
    for family in families {
        let combined = family
            .source_files
            .iter()
            .map(|file| production_source(file))
            .collect::<Vec<_>>()
            .join("\n");

        let missing_required: Vec<_> = family
            .required_terms
            .iter()
            .copied()
            .filter(|term| !combined.contains(term))
            .collect();
        let missing_corestore: Vec<_> = family
            .corestore_terms
            .iter()
            .copied()
            .filter(|term| !combined.contains(term))
            .collect();

        if !missing_required.is_empty() || !missing_corestore.is_empty() {
            failures.insert(family.name, (missing_required, missing_corestore));
        }
    }

    assert!(
        failures.is_empty(),
        "durable feature families must persist through CoreStore APIs; failures={failures:#?}"
    );
}
