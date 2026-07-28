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

pub(super) async fn write_descriptor_projection_mvcc<T: StoredRoutingRecord>(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    descriptor_key: &str,
    descriptor: &T,
    require_absent: bool,
) -> MeshDirectoryResult<()> {
    let family = descriptor.routing_family();
    let record_key = descriptor.routing_record_key();
    let expected_descriptor_key = routing_record_descriptor_key_for_key(family, &record_key)?;
    ensure_descriptor_key_matches(descriptor_key, &expected_descriptor_key)?;
    let row_key = routing_projection_row_key(family, &record_key)?;
    let key =
        crate::mvcc_product::coremeta_logical_key(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)?;
    let snapshot = mvcc.runtime.applied_version()?;
    let current = mvcc.runtime.read_point_at(&key, snapshot)?;
    let current_value = current.visible().map(|row| row.value.clone());
    if require_absent && current_value.is_some() {
        return Err(MeshDirectoryError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("routing descriptor already exists: {descriptor_key}"),
        )));
    }
    let payload = record_proto::encode_routing_projection_row(descriptor_key, descriptor)?;
    if current_value.as_deref() == Some(payload.as_slice()) {
        return Ok(());
    }
    let predicate = current_value
        .as_ref()
        .map(|payload| {
            crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(payload).as_bytes())
        })
        .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent);
    let transaction_id = format!(
        "mesh-directory-projection-repair:{}:{}:{}",
        family.stream_family(),
        hex::encode(blake3::hash(&payload).as_bytes()),
        current
            .observed_version()
            .map(|version| version.to_string())
            .unwrap_or_else(|| "unwritten".to_string()),
    );
    mvcc.autocommit_product_mutations_with_predicates(
        "mesh-directory",
        &transaction_id,
        vec![crate::mvcc_product::ProductMutation::put(
            key.clone(),
            payload,
        )],
        vec![(key, predicate)],
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        chrono::Utc::now().timestamp_millis().max(0) as u64,
    )
    .await
    .map_err(MeshDirectoryError::Other)?;
    Ok(())
}

#[cfg(test)]
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

#[cfg(test)]
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

pub(super) fn read_descriptor_projection_payload_proto_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    descriptor_key: &str,
) -> MeshDirectoryResult<Option<Vec<u8>>> {
    let (family, record_key, row_key) = descriptor_projection_row_key(descriptor_key)?;
    let key =
        crate::mvcc_product::coremeta_logical_key(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)?;
    let Some(payload) = mvcc.read_latest_value(&key)? else {
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
