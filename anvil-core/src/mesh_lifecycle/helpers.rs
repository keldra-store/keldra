use super::record_proto;
use super::*;

const MAX_DRAIN_BLOCKER_DETAILS: usize = 128;
const ROUTING_PAGE_SIZE: usize = 256;

pub(super) fn lifecycle_state_for_host_alias(state: HostAliasState) -> LifecycleState {
    match state {
        HostAliasState::PendingVerification => LifecycleState::Joining,
        HostAliasState::Active => LifecycleState::Active,
        HostAliasState::Suspended => LifecycleState::ReadOnly,
        HostAliasState::Deleted => LifecycleState::Removed,
    }
}

pub fn validate_node_transition(from: LifecycleState, to: LifecycleState) -> LifecycleResult<()> {
    use LifecycleState::*;
    if matches!(
        (from, to),
        (Joining, Active)
            | (Joining, Removed)
            | (Active, Draining)
            | (Active, Offline)
            | (Draining, Drained)
            | (Draining, Offline)
            | (Drained, Active)
            | (Drained, Removed)
            | (Offline, Active)
            | (Offline, Draining)
            | (Offline, Removed)
    ) {
        Ok(())
    } else {
        Err(LifecycleError::LifecycleTransitionDenied {
            resource_kind: "node",
            resource_id: String::new(),
            from,
            to,
        })
    }
}

pub fn validate_region_transition(from: LifecycleState, to: LifecycleState) -> LifecycleResult<()> {
    use LifecycleState::*;
    if matches!(
        (from, to),
        (Joining, Active)
            | (Joining, Removed)
            | (Active, ReadOnly)
            | (Active, Draining)
            | (Active, Offline)
            | (ReadOnly, Active)
            | (ReadOnly, Draining)
            | (Draining, Drained)
            | (Draining, DrainedWithExceptions)
            | (Draining, Offline)
            | (Drained, Active)
            | (DrainedWithExceptions, Active)
            | (DrainedWithExceptions, Draining)
            | (Drained, Removed)
            | (Offline, Active)
            | (Offline, Draining)
            | (Offline, Removed)
    ) {
        Ok(())
    } else {
        Err(LifecycleError::LifecycleTransitionDenied {
            resource_kind: "region",
            resource_id: String::new(),
            from,
            to,
        })
    }
}

pub(super) fn ensure_node_placement_is_active(
    state: &MeshLifecycleState,
    descriptor: &NodeDescriptor,
) -> LifecycleResult<()> {
    let Some(region) = state.regions.get(&descriptor.region) else {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: descriptor.region.clone(),
        });
    };
    let cell_key = cell_key(&descriptor.region, &descriptor.cell_id)?;
    let Some(cell) = state.cells.get(&cell_key) else {
        return Err(LifecycleError::NotFound {
            resource_kind: "cell",
            resource_id: descriptor.cell_id.clone(),
        });
    };
    if !matches!(
        region.state,
        LifecycleState::Joining | LifecycleState::Active
    ) || cell.state != LifecycleState::Active
    {
        return Err(LifecycleError::InvalidArgument(
            "node activation requires a joining or active region and an active cell".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn ensure_region_activation_dependencies(
    state: &MeshLifecycleState,
    region: &str,
) -> LifecycleResult<()> {
    let active_cell_ids = state
        .cells
        .values()
        .filter(|cell| cell.region == region && cell.state == LifecycleState::Active)
        .map(|cell| cell.cell_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if active_cell_ids.is_empty() {
        return Err(LifecycleError::InvalidArgument(format!(
            "region {region} activation requires at least one active cell"
        )));
    }
    let has_active_node = state.nodes.values().any(|node| {
        node.region == region
            && active_cell_ids.contains(node.cell_id.as_str())
            && node.state == LifecycleState::Active
    });
    if !has_active_node {
        return Err(LifecycleError::InvalidArgument(format!(
            "region {region} activation requires at least one active node in an active cell"
        )));
    }
    Ok(())
}

pub(super) fn ensure_region_accepts_new_writes_in_state(
    state: &MeshLifecycleState,
    region: &str,
) -> LifecycleResult<()> {
    let Some(descriptor) = state.regions.get(region) else {
        if state.regions.is_empty() {
            return Ok(());
        }
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: region.to_string(),
        });
    };
    if descriptor.state == LifecycleState::Active {
        return Ok(());
    }
    Err(LifecycleError::InvalidArgument(format!(
        "region {region} is {:?} and cannot accept new writable placement",
        descriptor.state
    )))
}

pub(super) fn ensure_cell_accepts_new_writes_in_state(
    state: &MeshLifecycleState,
    region: &str,
    cell_id: &str,
) -> LifecycleResult<()> {
    let key = cell_key(region, cell_id)?;
    let Some(descriptor) = state.cells.get(&key) else {
        if state.cells.is_empty() {
            return Ok(());
        }
        return Err(LifecycleError::NotFound {
            resource_kind: "cell",
            resource_id: format!("{region}/{cell_id}"),
        });
    };
    if descriptor.state == LifecycleState::Active {
        return Ok(());
    }
    Err(LifecycleError::InvalidArgument(format!(
        "cell {region}/{cell_id} is {:?} and cannot accept new writable placement",
        descriptor.state
    )))
}

pub(super) fn ensure_node_accepts_new_writes_in_state(
    state: &MeshLifecycleState,
    region: &str,
    cell_id: &str,
    node_id: &str,
) -> LifecycleResult<()> {
    let Some(descriptor) = state.nodes.get(node_id) else {
        if state.nodes.is_empty() {
            return Ok(());
        }
        return Err(LifecycleError::NotFound {
            resource_kind: "node",
            resource_id: node_id.to_string(),
        });
    };
    if descriptor.region != region || descriptor.cell_id != cell_id {
        return Err(LifecycleError::InvalidArgument(format!(
            "node {node_id} belongs to {}/{} and cannot accept placement for {region}/{cell_id}",
            descriptor.region, descriptor.cell_id
        )));
    }
    if descriptor.state == LifecycleState::Active {
        return Ok(());
    }
    Err(LifecycleError::InvalidArgument(format!(
        "node {node_id} is {:?} and cannot accept new writable placement",
        descriptor.state
    )))
}

pub(super) async fn ensure_region_drain_completion_is_supported(
    storage: &Storage,
    region: &str,
    target: LifecycleState,
) -> LifecycleResult<()> {
    match target {
        LifecycleState::Drained => {
            let blockers = bucket_locators_blocking_region_drain(storage, region).await?;
            if blockers.is_empty() {
                Ok(())
            } else {
                Err(LifecycleError::InvalidArgument(format!(
                    "region {region} drain cannot complete with block_until_empty: {} bucket locator(s) still name the region as primary: {}",
                    blockers.len(),
                    blockers.join(", ")
                )))
            }
        }
        LifecycleState::DrainedWithExceptions => {
            let blockers = bucket_locators_without_valid_drain_exception(storage, region).await?;
            if blockers.is_empty() {
                Ok(())
            } else {
                Err(LifecycleError::InvalidArgument(format!(
                    "region {region} drain cannot complete with exceptions: {} bucket locator(s) do not have a valid read-only drain exception: {}",
                    blockers.len(),
                    blockers.join(", ")
                )))
            }
        }
        _ => Ok(()),
    }
}

pub(super) async fn bucket_locators_blocking_region_drain(
    storage: &Storage,
    region: &str,
) -> LifecycleResult<Vec<String>> {
    let mut blockers = Vec::new();
    let mut cursor = None;
    loop {
        let page = page_bucket_locators_for_drain(storage, cursor.as_deref()).await?;
        for locator in page.locators {
            if locator.home_region.as_str() == region
                && bucket_locator_blocks_region_drain(locator.status)
            {
                blockers.push(format!(
                    "{}/{}:{:?}",
                    locator.tenant_id.as_str(),
                    locator.bucket_name.as_str(),
                    locator.status
                ));
                if blockers.len() == MAX_DRAIN_BLOCKER_DETAILS {
                    return Ok(blockers);
                }
            }
        }
        let Some(next_cursor) = page.next_tuple_key else {
            break;
        };
        ensure_page_cursor_advanced(cursor.as_deref(), &next_cursor)?;
        cursor = Some(next_cursor);
    }
    Ok(blockers)
}

async fn page_bucket_locators_for_drain(
    storage: &Storage,
    cursor: Option<&[u8]>,
) -> LifecycleResult<mesh_directory::BucketLocatorPage> {
    mesh_directory::page_bucket_locators(storage, cursor, ROUTING_PAGE_SIZE)
        .await
        .map_err(|err| {
            LifecycleError::InvalidArgument(format!(
                "could not inspect bucket locators for region drain: {err}"
            ))
        })
}

fn ensure_page_cursor_advanced(current: Option<&[u8]>, next: &[u8]) -> LifecycleResult<()> {
    if current.is_some_and(|current| current >= next) {
        return Err(LifecycleError::InvalidArgument(
            "bucket locator page cursor did not advance".to_string(),
        ));
    }
    Ok(())
}

fn push_drain_blocker(blockers: &mut Vec<String>, blocker: String) -> bool {
    blockers.push(blocker);
    blockers.len() == MAX_DRAIN_BLOCKER_DETAILS
}

fn bucket_locator_drain_exception_blocker(
    state: &MeshLifecycleState,
    region: &str,
    locator: &BucketLocatorDescriptor,
) -> Option<String> {
    if locator.home_region.as_str() != region || !bucket_locator_blocks_region_drain(locator.status)
    {
        return None;
    }
    let record_key = format!(
        "{}/{}",
        locator.tenant_id.as_str(),
        locator.bucket_name.as_str()
    );
    let exception_key = bucket_drain_exception_key(
        region,
        locator.tenant_id.as_str(),
        locator.bucket_name.as_str(),
    );
    let Some(exception) = state.bucket_drain_exceptions.get(&exception_key) else {
        return Some(format!(
            "{}:{:?}:missing_exception",
            record_key, locator.status
        ));
    };
    if locator.status != BucketLocatorStatus::ReadOnly {
        return Some(format!(
            "{}:{:?}:exception_requires_read_only_locator",
            record_key, locator.status
        ));
    }
    if !exception.disposition.allows_drained_exception() {
        return Some(format!(
            "{}:{:?}:invalid_exception_disposition:{}",
            record_key,
            locator.status,
            exception.disposition.as_str()
        ));
    }
    None
}

pub(super) fn bucket_locator_blocks_region_drain(status: BucketLocatorStatus) -> bool {
    !matches!(status, BucketLocatorStatus::Deleted)
}

pub(super) async fn bucket_locators_without_valid_drain_exception(
    storage: &Storage,
    region: &str,
) -> LifecycleResult<Vec<String>> {
    let state = read_state(storage).await?;
    let mut blockers = Vec::new();
    let mut cursor = None;
    loop {
        let page = page_bucket_locators_for_drain(storage, cursor.as_deref()).await?;
        for locator in page.locators {
            if let Some(blocker) = bucket_locator_drain_exception_blocker(&state, region, &locator)
                && push_drain_blocker(&mut blockers, blocker)
            {
                return Ok(blockers);
            }
        }
        let Some(next_cursor) = page.next_tuple_key else {
            break;
        };
        ensure_page_cursor_advanced(cursor.as_deref(), &next_cursor)?;
        cursor = Some(next_cursor);
    }
    Ok(blockers)
}

pub fn bucket_drain_exception_key(region: &str, tenant_id: &str, bucket_name: &str) -> String {
    format!("{region}/{tenant_id}/{bucket_name}")
}

pub fn lifecycle_control_partition(stream_family: &str, record_key: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(stream_family.as_bytes());
    hasher.update(b":");
    hasher.update(record_key.as_bytes());
    let digest = hasher.finalize();
    let bytes = digest.as_bytes();
    format!("{:02x}{:02x}", bytes[0], bytes[1])
}

pub fn lifecycle_control_stream_families() -> [&'static str; 3] {
    [
        REGION_DESCRIPTOR_STREAM_FAMILY,
        CELL_DESCRIPTOR_STREAM_FAMILY,
        NODE_DESCRIPTOR_STREAM_FAMILY,
    ]
}

pub(super) fn cell_record_key(region: &str, cell_id: &str) -> LifecycleResult<String> {
    require_identifier(region, "cell record region")?;
    require_identifier(cell_id, "cell record cell id")?;
    Ok(format!("{region}/{cell_id}"))
}

pub(super) fn node_record_key(
    region: &str,
    cell_id: &str,
    node_id: &str,
) -> LifecycleResult<String> {
    require_identifier(region, "node record region")?;
    require_identifier(cell_id, "node record cell id")?;
    require_identifier(node_id, "node record node id")?;
    Ok(format!("{region}/{cell_id}/{node_id}"))
}

pub(super) fn parse_node_record_key(record_key: &str) -> LifecycleResult<(&str, &str, &str)> {
    let mut parts = record_key.split('/');
    let region = parts.next().unwrap_or_default();
    let cell_id = parts.next().unwrap_or_default();
    let node_id = parts.next().unwrap_or_default();
    if parts.next().is_some() || region.is_empty() || cell_id.is_empty() || node_id.is_empty() {
        return Err(LifecycleError::InvalidArgument(format!(
            "invalid node record key {record_key}"
        )));
    }
    Ok((region, cell_id, node_id))
}

pub(super) fn require_control_record_key(value: &str) -> LifecycleResult<()> {
    require_nonempty(value, "control record key")?;
    if value.contains("//") || value.chars().any(|ch| ch == '\0' || ch.is_control()) {
        return Err(LifecycleError::InvalidArgument(
            "control record key contains an invalid character".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn ensure_generation(
    resource_kind: &'static str,
    resource_id: &str,
    current: u64,
    expected: u64,
) -> LifecycleResult<()> {
    if current == expected {
        return Ok(());
    }
    Err(LifecycleError::GenerationConflict {
        resource_kind,
        resource_id: resource_id.to_string(),
        expected,
        current,
    })
}

pub(super) fn validate_activation_checkpoint_header(
    checkpoint: &ActivationCheckpoint,
    mesh_id: &str,
    region: &str,
) -> LifecycleResult<()> {
    if checkpoint.schema != ACTIVATION_CHECKPOINT_SCHEMA {
        return Err(LifecycleError::InvalidArgument(format!(
            "activation checkpoint schema must be {ACTIVATION_CHECKPOINT_SCHEMA}"
        )));
    }
    require_identifier(&checkpoint.mesh_id, "activation checkpoint mesh id")?;
    require_identifier(&checkpoint.region, "activation checkpoint region")?;
    require_nonempty(&checkpoint.created_at, "activation checkpoint created_at")?;
    if checkpoint.mesh_id != mesh_id {
        return Err(LifecycleError::InvalidArgument(format!(
            "activation checkpoint mesh_id {} does not match region mesh_id {mesh_id}",
            checkpoint.mesh_id
        )));
    }
    if checkpoint.region != region {
        return Err(LifecycleError::InvalidArgument(format!(
            "activation checkpoint region {} does not match requested region {region}",
            checkpoint.region
        )));
    }
    for stream in &checkpoint.required_streams {
        require_identifier(&stream.stream_family, "activation checkpoint stream family")?;
        require_identifier(&stream.partition, "activation checkpoint partition")?;
    }
    Ok(())
}

pub(super) async fn validate_activation_checkpoint_streams(
    storage: &Storage,
    checkpoint: &ActivationCheckpoint,
) -> LifecycleResult<()> {
    let supplied_streams = checkpoint
        .required_streams
        .iter()
        .map(|stream| (stream.stream_family.as_str(), stream.partition.as_str()))
        .collect::<BTreeSet<_>>();
    for (stream_family, partition) in existing_control_stream_partitions(storage).await? {
        if !supplied_streams.contains(&(stream_family.as_str(), partition.as_str())) {
            return Err(LifecycleError::ActivationCheckpointNotReached {
                stream_family,
                partition,
                sequence: 1,
                expected_digest: "checkpoint-required".to_string(),
                reason: "activation checkpoint omits an existing control stream partition"
                    .to_string(),
            });
        }
    }

    for required in &checkpoint.required_streams {
        let Some(region_checkpoint) = read_control_checkpoint(
            storage,
            &checkpoint.region,
            &required.stream_family,
            &required.partition,
        )
        .await
        .map_err(|err| {
            LifecycleError::InvalidArgument(format!(
                "activation checkpoint could not read regional checkpoint {}/{} for {}: {err}",
                required.stream_family, required.partition, checkpoint.region
            ))
        })?
        else {
            return Err(activation_checkpoint_not_reached(
                required,
                "regional control checkpoint is absent".to_string(),
            ));
        };
        if region_checkpoint.mesh_id != checkpoint.mesh_id {
            return Err(activation_checkpoint_not_reached(
                required,
                format!(
                    "regional checkpoint mesh_id {} does not match activation checkpoint mesh_id {}",
                    region_checkpoint.mesh_id, checkpoint.mesh_id
                ),
            ));
        }
        if region_checkpoint.last_sequence < required.sequence {
            return Err(activation_checkpoint_not_reached(
                required,
                format!(
                    "regional checkpoint latest sequence is {}",
                    region_checkpoint.last_sequence.get()
                ),
            ));
        }
        if region_checkpoint.last_sequence == required.sequence {
            if region_checkpoint.last_digest.as_str() != required.digest.as_str() {
                return Err(activation_checkpoint_not_reached(
                    required,
                    format!(
                        "regional checkpoint digest mismatch at sequence {}",
                        required.sequence.get()
                    ),
                ));
            }
            continue;
        }

        let page = crate::mesh_control_stream::read_control_stream_page(
            storage,
            &required.stream_family,
            &required.partition,
            required.sequence.get().saturating_sub(1),
            1,
        )
        .await
        .map_err(|err| {
            LifecycleError::InvalidArgument(format!(
                "activation checkpoint could not read control stream {}/{}: {err}",
                required.stream_family, required.partition
            ))
        })?;
        let Some(record) = page
            .records
            .into_iter()
            .find(|record| record.metadata.sequence == required.sequence)
        else {
            return Err(activation_checkpoint_not_reached(
                required,
                format!(
                    "regional checkpoint is beyond sequence {}, but the required stream position is not available for digest validation",
                    region_checkpoint.last_sequence.get()
                ),
            ));
        };
        if record.metadata.record_digest.as_str() != required.digest.as_str() {
            return Err(activation_checkpoint_not_reached(
                required,
                format!("digest mismatch at sequence {}", required.sequence.get()),
            ));
        }
    }
    Ok(())
}

pub(super) async fn existing_control_stream_partitions(
    storage: &Storage,
) -> LifecycleResult<Vec<(String, String)>> {
    let mut streams = Vec::new();
    let stream_families = mesh_directory::RoutingRecordFamily::all()
        .into_iter()
        .map(|family| family.stream_family())
        .chain(lifecycle_control_stream_families().into_iter());
    for stream_family in stream_families {
        let mut cursor = None;
        loop {
            let page = crate::mesh_control_stream::list_control_stream_partitions_page(
                storage,
                stream_family,
                cursor.as_deref(),
                256,
            )
            .await
            .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;
            streams.extend(
                page.partitions
                    .into_iter()
                    .map(|partition| (stream_family.to_string(), partition)),
            );
            let Some(next) = page.next_stream_id else {
                break;
            };
            cursor = Some(next);
        }
    }
    Ok(streams)
}

pub(super) fn activation_checkpoint_not_reached(
    required: &ActivationCheckpointStream,
    reason: String,
) -> LifecycleError {
    LifecycleError::ActivationCheckpointNotReached {
        stream_family: required.stream_family.clone(),
        partition: required.partition.clone(),
        sequence: required.sequence.get(),
        expected_digest: required.digest.to_string(),
        reason,
    }
}

pub(super) fn cell_key(region: &str, cell_id: &str) -> LifecycleResult<String> {
    require_identifier(region, "region")?;
    require_identifier(cell_id, "cell id")?;
    Ok(format!("{region}/{cell_id}"))
}

pub(super) fn require_identifier(value: &str, field: &str) -> LifecycleResult<()> {
    require_nonempty(value, field)?;
    if value
        .chars()
        .any(|ch| ch == '/' || ch == '\0' || ch.is_control())
    {
        return Err(LifecycleError::InvalidArgument(format!(
            "{field} contains an invalid character"
        )));
    }
    Ok(())
}

pub(super) fn require_nonempty(value: &str, field: &str) -> LifecycleResult<()> {
    if value.trim().is_empty() {
        return Err(LifecycleError::InvalidArgument(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

pub(crate) fn capacity_json_hash(input: &str) -> LifecycleResult<String> {
    let trimmed = input.trim();
    if trimmed.len() > 64 * 1024 {
        return Err(LifecycleError::InvalidArgument(
            "node capacity JSON exceeds 64 KiB".to_string(),
        ));
    }
    if trimmed.is_empty() {
        return Ok(blake3::hash(b"{}").to_hex().to_string());
    }

    let value: serde_json::Value = serde_json::from_str(trimmed).map_err(|err| {
        LifecycleError::InvalidArgument(format!("node capacity JSON is invalid: {err}"))
    })?;
    if !value.is_object() {
        return Err(LifecycleError::InvalidArgument(
            "node capacity JSON must be an object".to_string(),
        ));
    }
    let canonical = serde_json::to_vec(&value)?;
    Ok(blake3::hash(&canonical).to_hex().to_string())
}

pub(super) fn timestamp_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}
