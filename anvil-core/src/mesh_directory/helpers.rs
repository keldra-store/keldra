use super::*;

pub(super) fn descriptor_key_relative_segments(
    descriptor_key: &str,
) -> MeshDirectoryResult<Vec<String>> {
    let relative = descriptor_key
        .strip_prefix(MESH_DIRECTORY_ROOT)
        .and_then(|value| value.strip_prefix('/'))
        .ok_or_else(|| MeshDirectoryError::InvalidIdentifier {
            field: "descriptor key",
            value: descriptor_key.to_string(),
        })?;
    if relative
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field: "descriptor key",
            value: descriptor_key.to_string(),
        });
    }
    Ok(relative.split('/').map(str::to_string).collect())
}

pub(super) fn routing_record_partition_from_descriptor_key(
    descriptor_key: &str,
) -> MeshDirectoryResult<String> {
    descriptor_key_relative_segments(descriptor_key)?
        .get(1)
        .cloned()
        .filter(|partition| partition.len() == 4)
        .ok_or_else(|| MeshDirectoryError::InvalidIdentifier {
            field: "routing record partition",
            value: descriptor_key.to_string(),
        })
}

pub(super) fn routing_record_family_from_descriptor_key(
    descriptor_key: &str,
) -> MeshDirectoryResult<RoutingRecordFamily> {
    let segments = descriptor_key_relative_segments(descriptor_key)?;
    match segments.first().map(String::as_str) {
        Some("tenant-names") => Ok(RoutingRecordFamily::TenantName),
        Some("tenants") => Ok(RoutingRecordFamily::TenantLocator),
        Some("buckets") => Ok(RoutingRecordFamily::BucketLocator),
        Some("host-aliases") => Ok(RoutingRecordFamily::HostAlias),
        _ => Err(MeshDirectoryError::InvalidIdentifier {
            field: "routing record family",
            value: descriptor_key.to_string(),
        }),
    }
}

pub(super) fn routing_record_key_from_descriptor_key(
    family: RoutingRecordFamily,
    descriptor_key: &str,
) -> MeshDirectoryResult<String> {
    let segments = descriptor_key_relative_segments(descriptor_key)?;
    match family {
        RoutingRecordFamily::TenantName
        | RoutingRecordFamily::TenantLocator
        | RoutingRecordFamily::HostAlias => segments
            .get(2)
            .and_then(|file| file.strip_suffix(DESCRIPTOR_FILE_EXTENSION))
            .map(str::to_string)
            .ok_or_else(|| MeshDirectoryError::InvalidIdentifier {
                field: "routing record key",
                value: descriptor_key.to_string(),
            }),
        RoutingRecordFamily::BucketLocator => {
            let tenant_id = segments.get(2);
            let bucket_file = segments.get(3);
            match (
                tenant_id,
                bucket_file.and_then(|file| file.strip_suffix(DESCRIPTOR_FILE_EXTENSION)),
            ) {
                (Some(tenant_id), Some(bucket_name)) => Ok(format!("{tenant_id}/{bucket_name}")),
                _ => Err(MeshDirectoryError::InvalidIdentifier {
                    field: "routing record key",
                    value: descriptor_key.to_string(),
                }),
            }
        }
    }
}

pub(super) fn routing_projection_row_prefix(
    family: RoutingRecordFamily,
) -> MeshDirectoryResult<Vec<u8>> {
    Ok(core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(record_proto::ROUTING_PROJECTION_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(family.stream_family()),
    ])?)
}

pub(super) fn routing_projection_row_key(
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<Vec<u8>> {
    Ok(core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(record_proto::ROUTING_PROJECTION_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(family.stream_family()),
        CoreMetaTuplePart::Utf8(record_key),
    ])?)
}

pub(super) fn descriptor_projection_row_key(
    descriptor_key: &str,
) -> MeshDirectoryResult<(RoutingRecordFamily, String, Vec<u8>)> {
    let family = routing_record_family_from_descriptor_key(descriptor_key)?;
    let record_key = routing_record_key_from_descriptor_key(family, descriptor_key)?;
    let row_key = routing_projection_row_key(family, &record_key)?;
    Ok((family, record_key, row_key))
}

pub(super) async fn stage_descriptor_projection_in_transaction<T: StoredRoutingRecord>(
    storage: &Storage,
    descriptor_key: &str,
    descriptor: &T,
    require_absent: bool,
    transaction_id: &str,
    principal: &str,
) -> MeshDirectoryResult<()> {
    let family = descriptor.routing_family();
    let record_key = descriptor.routing_record_key();
    let expected_descriptor_key = routing_record_descriptor_key_for_key(family, &record_key)?;
    ensure_descriptor_key_matches(descriptor_key, &expected_descriptor_key)?;
    let row_key = routing_projection_row_key(family, &record_key)?;
    let store = CoreStore::new(storage.clone()).await?;
    let current = store
        .read_coremeta_row_visible_to_transaction(
            CF_MESH,
            TABLE_MESH_PARTITION_ROW,
            &row_key,
            transaction_id,
            principal,
        )
        .await?;
    if require_absent && current.is_some() {
        return Err(MeshDirectoryError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("routing descriptor already exists: {descriptor_key}"),
        )));
    }
    let payload = record_proto::encode_routing_projection_row(descriptor_key, descriptor)?;
    store
        .stage_coremeta_put_in_transaction(
            transaction_id,
            principal,
            CF_MESH,
            TABLE_MESH_PARTITION_ROW,
            row_key,
            payload,
            current
                .as_ref()
                .map(|payload| core_meta_payload_digest(TABLE_MESH_PARTITION_ROW, payload)),
            require_absent || current.is_none(),
            !require_absent && current.is_some(),
        )
        .await?;
    Ok(())
}

pub(super) async fn write_descriptor_projection<T: StoredRoutingRecord>(
    storage: &Storage,
    descriptor_key: &str,
    descriptor: &T,
    require_absent: bool,
) -> MeshDirectoryResult<()> {
    let family = descriptor.routing_family();
    let record_key = descriptor.routing_record_key();
    let expected_descriptor_key = routing_record_descriptor_key_for_key(family, &record_key)?;
    ensure_descriptor_key_matches(descriptor_key, &expected_descriptor_key)?;
    let row_key = routing_projection_row_key(family, &record_key)?;
    let store = CoreStore::new(storage.clone()).await?;
    let current = store.read_coremeta_row(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)?;
    if require_absent && current.is_some() {
        return Err(MeshDirectoryError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("routing descriptor already exists: {descriptor_key}"),
        )));
    }
    let payload = record_proto::encode_routing_projection_row(descriptor_key, descriptor)?;
    let root_anchor_key = format!("mesh/directory/{}/{}", family.stream_family(), record_key);
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "mesh-directory-projection:{}:{}",
                family.stream_family(),
                uuid::Uuid::new_v4()
            ),
            scope_partition: MESH_DIRECTORY_PROJECTION_PARTITION_ID.to_string(),
            committed_by_principal: "mesh-directory".to_string(),
            root_publications: mesh_directory_root_publications(root_anchor_key),
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: CF_MESH.to_string(),
                table_id: TABLE_MESH_PARTITION_ROW,
                tuple_key: row_key.clone(),
                expected_payload_hash: current
                    .as_ref()
                    .map(|payload| core_meta_payload_digest(TABLE_MESH_PARTITION_ROW, payload)),
                require_absent: require_absent || current.is_none(),
                require_present: !require_absent && current.is_some(),
            }],
            operations: vec![CoreMutationOperation::CoreMetaPut {
                partition_id: MESH_DIRECTORY_PROJECTION_PARTITION_ID.to_string(),
                cf: CF_MESH.to_string(),
                table_id: TABLE_MESH_PARTITION_ROW,
                tuple_key: row_key,
                payload,
            }],
        })
        .await?;
    Ok(())
}

pub(super) async fn read_descriptor_projection_payload(
    storage: &Storage,
    descriptor_key: &str,
) -> MeshDirectoryResult<Option<String>> {
    let family = routing_record_family_from_descriptor_key(descriptor_key)?;
    let record_key = routing_record_key_from_descriptor_key(family, descriptor_key)?;
    let Some(payload_proto) =
        read_descriptor_projection_payload_proto(storage, descriptor_key).await?
    else {
        return Ok(None);
    };
    Ok(Some(
        routing_record_descriptor_from_proto(family, &record_key, &payload_proto)?.payload_json,
    ))
}

pub(super) async fn read_descriptor_projection_payload_proto(
    storage: &Storage,
    descriptor_key: &str,
) -> MeshDirectoryResult<Option<Vec<u8>>> {
    let (family, record_key, row_key) = descriptor_projection_row_key(descriptor_key)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = store.read_coremeta_row(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)?
    else {
        return Ok(None);
    };
    let row = record_proto::decode_routing_projection_row(&payload)?;
    if row.descriptor.family != family || row.descriptor.record_key != record_key {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field: "mesh directory projection row key",
            value: format!(
                "expected {:?}/{record_key}, got {:?}/{}",
                family, row.descriptor.family, row.descriptor.record_key
            ),
        });
    }
    Ok(Some(row.payload_proto))
}

#[cfg(test)]
pub(super) async fn delete_descriptor_projection(
    storage: &Storage,
    descriptor_key: &str,
) -> MeshDirectoryResult<()> {
    let (family, record_key, row_key) = descriptor_projection_row_key(descriptor_key)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(current) = store.read_coremeta_row(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)?
    else {
        return Ok(());
    };
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "mesh-directory-projection-delete:{}:{}",
                family.stream_family(),
                uuid::Uuid::new_v4()
            ),
            scope_partition: MESH_DIRECTORY_PROJECTION_PARTITION_ID.to_string(),
            committed_by_principal: "mesh-directory-test".to_string(),
            root_publications: mesh_directory_root_publications(format!(
                "mesh/directory/{}/{}",
                family.stream_family(),
                record_key
            )),
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: CF_MESH.to_string(),
                table_id: TABLE_MESH_PARTITION_ROW,
                tuple_key: row_key.clone(),
                expected_payload_hash: Some(core_meta_payload_digest(
                    TABLE_MESH_PARTITION_ROW,
                    &current,
                )),
                require_absent: false,
                require_present: true,
            }],
            operations: vec![CoreMutationOperation::CoreMetaDelete {
                partition_id: MESH_DIRECTORY_PROJECTION_PARTITION_ID.to_string(),
                cf: CF_MESH.to_string(),
                table_id: TABLE_MESH_PARTITION_ROW,
                tuple_key: row_key,
            }],
        })
        .await?;
    Ok(())
}

fn mesh_directory_root_publications(
    data_root: String,
) -> Vec<crate::core_store::CoreMutationRootPublication> {
    let coordinator_root = MESH_DIRECTORY_PROJECTION_PARTITION_ID;
    if coordinator_root == data_root.as_str() {
        return vec![crate::core_store::CoreMutationRootPublication {
            root_anchor_key: data_root,
            writer_families: vec![
                crate::formats::writer::WriterFamily::CoreControl
                    .as_str()
                    .to_string(),
                crate::formats::writer::WriterFamily::MeshControl
                    .as_str()
                    .to_string(),
            ],
            transaction_coordinator: true,
        }];
    }
    vec![
        crate::core_store::CoreMutationRootPublication::new(
            coordinator_root,
            crate::formats::writer::WriterFamily::CoreControl.as_str(),
        )
        .coordinator(),
        crate::core_store::CoreMutationRootPublication::new(
            data_root,
            crate::formats::writer::WriterFamily::MeshControl.as_str(),
        ),
    ]
}

pub(super) fn partition_key_bytes(domain: &str, components: &[&str]) -> Vec<u8> {
    let mut key = domain.as_bytes().to_vec();
    for component in components {
        key.push(0);
        key.extend_from_slice(component.as_bytes());
    }
    key
}

pub(super) fn join_mesh_key(segments: &[&str]) -> String {
    let mut out = String::from(MESH_DIRECTORY_ROOT);
    for segment in segments {
        out.push('/');
        out.push_str(segment);
    }
    out
}

pub(super) fn validate_dns_label_name(value: &str) -> Result<(), ()> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 63 {
        return Err(());
    }
    if !bytes[0].is_ascii_lowercase() {
        return Err(());
    }
    if !bytes[bytes.len() - 1].is_ascii_lowercase() && !bytes[bytes.len() - 1].is_ascii_digit() {
        return Err(());
    }
    if bytes
        .iter()
        .any(|byte| !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && *byte != b'-')
    {
        return Err(());
    }
    Ok(())
}

pub(super) fn require_safe_component(value: &str, field: &'static str) -> MeshDirectoryResult<()> {
    require_nonempty(value, field)?;
    if value.len() > 128
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && byte != b'_' && byte != b'-')
    {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field,
            value: value.to_string(),
        });
    }
    Ok(())
}

pub(super) fn require_control_path_fragment(
    value: &str,
    field: &'static str,
) -> MeshDirectoryResult<()> {
    require_nonempty(value, field)?;
    if value.starts_with('/')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
        || value
            .split('/')
            .any(|segment| segment == "." || segment == "..")
    {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field,
            value: value.to_string(),
        });
    }
    Ok(())
}

pub(super) fn require_nonempty(value: &str, field: &'static str) -> MeshDirectoryResult<()> {
    if value.is_empty() {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field,
            value: value.to_string(),
        });
    }
    Ok(())
}

pub(super) fn parse_rfc3339(
    value: &str,
    field: &'static str,
) -> MeshDirectoryResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| MeshDirectoryError::InvalidTimestamp {
            field,
            value: value.to_string(),
        })
}
