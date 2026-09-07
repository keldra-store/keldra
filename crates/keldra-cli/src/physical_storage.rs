use keldra_storage::v1::{
    AccountingByteMeasurement, AccountingCountMeasurement, AccountingMeasurementState,
    ClusterLogicalFileCounts, ClusterPhysicalStorage,
};

pub(crate) fn print(
    physical: Option<&ClusterPhysicalStorage>,
    logical: Option<&ClusterLogicalFileCounts>,
) {
    if let Some(physical) = physical {
        println!(
            "physical_active_nodes={} physical_reported_nodes={} physical_replica_stores={} live_payload_blob_bytes={} garbage_payload_blob_bytes={} payload_sst_bytes={} metadata_index_sst_bytes={} wal_bytes={}",
            physical.active_node_count,
            physical.reported_node_count,
            physical.reported_storage_replica_count,
            format_bytes(physical.live_payload_blob_bytes.as_ref()),
            format_bytes(physical.garbage_payload_blob_bytes.as_ref()),
            format_bytes(physical.payload_sst_bytes.as_ref()),
            format_bytes(physical.metadata_index_sst_bytes.as_ref()),
            format_bytes(physical.wal_bytes.as_ref()),
        );
        for node in &physical.nodes {
            println!(
                "physical_node={} refreshed_at={:?} live_payload_blob_bytes={} garbage_payload_blob_bytes={} payload_sst_bytes={} metadata_index_sst_bytes={} wal_bytes={}",
                node.node_id,
                node.refreshed_at,
                format_bytes(node.live_payload_blob_bytes.as_ref()),
                format_bytes(node.garbage_payload_blob_bytes.as_ref()),
                format_bytes(node.payload_sst_bytes.as_ref()),
                format_bytes(node.metadata_index_sst_bytes.as_ref()),
                format_bytes(node.wal_bytes.as_ref()),
            );
        }
    }
    if let Some(logical) = logical {
        println!(
            "logical_active_sources={} logical_reported_sources={} cluster_visible_files={}",
            logical.active_source_node_count,
            logical.reported_source_node_count,
            format_count(logical.visible_file_count.as_ref()),
        );
        for tenant in &logical.tenants {
            println!(
                "logical_tenant_id={} visible_files={}",
                tenant.tenant_id,
                format_count(tenant.visible_file_count.as_ref()),
            );
        }
        for source in &logical.sources {
            println!(
                "logical_source_node={} refreshed_at={:?} visible_files={}",
                source.node_id,
                source.refreshed_at,
                format_count(source.visible_file_count.as_ref()),
            );
        }
    }
}

fn format_bytes(value: Option<&AccountingByteMeasurement>) -> String {
    let Some(value) = value else {
        return "unspecified".into();
    };
    match AccountingMeasurementState::try_from(value.state)
        .unwrap_or(AccountingMeasurementState::Unspecified)
    {
        AccountingMeasurementState::Present => value.bytes.to_string(),
        AccountingMeasurementState::Unsupported => "unsupported".into(),
        AccountingMeasurementState::Unavailable => "unavailable".into(),
        AccountingMeasurementState::Unspecified => "unspecified".into(),
    }
}

fn format_count(value: Option<&AccountingCountMeasurement>) -> String {
    let Some(value) = value else {
        return "unspecified".into();
    };
    match AccountingMeasurementState::try_from(value.state)
        .unwrap_or(AccountingMeasurementState::Unspecified)
    {
        AccountingMeasurementState::Present => value.count.to_string(),
        AccountingMeasurementState::Unsupported => "unsupported".into(),
        AccountingMeasurementState::Unavailable => "unavailable".into(),
        AccountingMeasurementState::Unspecified => "unspecified".into(),
    }
}
