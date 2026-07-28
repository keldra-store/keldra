use super::*;

pub(super) fn node_capability_from_proto(value: i32) -> Result<CoreNodeCapability, Status> {
    match value {
        1 => Ok(CoreNodeCapability::Object),
        2 => Ok(CoreNodeCapability::Index),
        3 => Ok(CoreNodeCapability::PersonalDb),
        4 => Ok(CoreNodeCapability::Metadata),
        5 => Ok(CoreNodeCapability::Gateway),
        6 => Ok(CoreNodeCapability::Admin),
        _ => Err(Status::invalid_argument("Invalid node capability")),
    }
}

pub(super) fn node_capability_to_proto(value: CoreNodeCapability) -> i32 {
    match value {
        CoreNodeCapability::Object => 1,
        CoreNodeCapability::Index => 2,
        CoreNodeCapability::PersonalDb => 3,
        CoreNodeCapability::Metadata => 4,
        CoreNodeCapability::Gateway => 5,
        CoreNodeCapability::Admin => 6,
    }
}

pub(super) fn lifecycle_state_to_proto(value: CoreLifecycleState) -> i32 {
    match value {
        CoreLifecycleState::Joining => 1,
        CoreLifecycleState::Active => 2,
        CoreLifecycleState::ReadOnly => 3,
        CoreLifecycleState::Draining => 4,
        CoreLifecycleState::Drained => 5,
        CoreLifecycleState::DrainedWithExceptions => 6,
        CoreLifecycleState::Offline => 7,
        CoreLifecycleState::Removed => 8,
    }
}

pub(super) fn routing_record_family_from_proto(
    value: i32,
) -> Result<Option<mesh_directory::RoutingRecordFamily>, Status> {
    match value {
        0 => Ok(None),
        1 => Ok(Some(mesh_directory::RoutingRecordFamily::TenantName)),
        2 => Ok(Some(mesh_directory::RoutingRecordFamily::TenantLocator)),
        3 => Ok(Some(mesh_directory::RoutingRecordFamily::BucketLocator)),
        4 => Ok(Some(mesh_directory::RoutingRecordFamily::HostAlias)),
        _ => Err(Status::invalid_argument("Invalid routing record family")),
    }
}

pub(super) fn routing_record_family_to_proto(value: mesh_directory::RoutingRecordFamily) -> i32 {
    match value {
        mesh_directory::RoutingRecordFamily::TenantName => 1,
        mesh_directory::RoutingRecordFamily::TenantLocator => 2,
        mesh_directory::RoutingRecordFamily::BucketLocator => 3,
        mesh_directory::RoutingRecordFamily::HostAlias => 4,
    }
}

pub(super) fn routing_record_descriptor_to_proto(
    value: mesh_directory::RoutingRecordDescriptor,
) -> RoutingRecordDescriptor {
    RoutingRecordDescriptor {
        family: routing_record_family_to_proto(value.family),
        record_key: value.record_key,
        partition: value.partition,
        descriptor_key: value.descriptor_key,
        generation: value.generation,
        payload_json: value.payload_json,
    }
}

pub(super) fn host_alias_state_to_proto(value: CoreHostAliasState) -> i32 {
    match value {
        CoreHostAliasState::PendingVerification => 1,
        CoreHostAliasState::Active => 2,
        CoreHostAliasState::Suspended => 3,
        CoreHostAliasState::Deleted => 4,
    }
}

pub(super) fn host_alias_descriptor_to_proto(
    value: CoreHostAliasDescriptor,
) -> crate::anvil_api::HostAliasDescriptor {
    let verification_challenge = host_alias_verification_challenge(&value);
    crate::anvil_api::HostAliasDescriptor {
        schema: value.schema,
        hostname: value.hostname,
        tenant_id: value.tenant_id,
        bucket_name: value.bucket_name,
        region: value.region,
        prefix: value.prefix,
        state: host_alias_state_to_proto(value.state),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
        verification_challenge,
    }
}

pub(super) fn host_alias_verification_challenge(value: &CoreHostAliasDescriptor) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(value.hostname.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.tenant_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.bucket_name.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.region.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.prefix.as_bytes());
    format!("anvil-host-alias={}", hasher.finalize().to_hex())
}

pub(super) fn node_descriptor_to_proto(value: mesh_lifecycle::NodeDescriptor) -> NodeDescriptor {
    NodeDescriptor {
        schema: value.schema,
        mesh_id: value.mesh_id,
        node_id: value.node_id,
        region: value.region,
        cell_id: value.cell_id,
        receipt_signing_public_key: value.receipt_signing_public_key,
        public_api_addr: value.public_api_addr,
        capabilities: value
            .capabilities
            .into_iter()
            .map(node_capability_to_proto)
            .collect(),
        capacity_json_hash: value.capacity_json_hash,
        state: lifecycle_state_to_proto(value.state),
        drain: value.drain.map(node_drain_descriptor_to_proto),
        last_heartbeat_at: value.last_heartbeat_at.unwrap_or_default(),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
    }
}

pub(super) fn node_drain_descriptor_to_proto(
    value: NodeDrainDescriptor,
) -> crate::anvil_api::NodeDrainDescriptor {
    crate::anvil_api::NodeDrainDescriptor {
        started_at: value.started_at,
        graceful_timeout_ms: value.graceful_timeout_ms,
        force_after_timeout: value.force_after_timeout,
    }
}

pub(super) fn region_descriptor_to_proto(
    value: mesh_lifecycle::RegionDescriptor,
) -> RegionDescriptor {
    RegionDescriptor {
        schema: value.schema,
        mesh_id: value.mesh_id,
        region: value.region,
        state: lifecycle_state_to_proto(value.state),
        public_base_url: value.public_base_url,
        virtual_host_suffix: value.virtual_host_suffix,
        placement_weight: value.placement_weight,
        default_cell: value.default_cell.unwrap_or_default(),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
    }
}

pub(super) fn cell_descriptor_to_proto(value: mesh_lifecycle::CellDescriptor) -> CellDescriptor {
    CellDescriptor {
        schema: value.schema,
        mesh_id: value.mesh_id,
        region: value.region,
        cell_id: value.cell_id,
        state: lifecycle_state_to_proto(value.state),
        placement_weight: value.placement_weight,
        failure_domain: value.failure_domain,
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
    }
}

pub(super) fn empty_to_none(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

pub(super) fn none_if_empty(value: &str) -> Option<&str> {
    if value.is_empty() { None } else { Some(value) }
}

pub(super) fn parse_personaldb_key_status(
    value: &str,
    allow_active: bool,
) -> Result<PublicKeyStatus, Status> {
    let status = match value {
        "active" if allow_active => PublicKeyStatus::Active,
        "retiring" => PublicKeyStatus::Retiring,
        "revoked_future" => PublicKeyStatus::RevokedFuture,
        "compromised" => PublicKeyStatus::Compromised,
        "active" => {
            return Err(Status::invalid_argument(
                "A non-active PersonalDB signing key cannot be reactivated",
            ));
        }
        _ => {
            return Err(Status::invalid_argument(
                "PersonalDB signing key status must be active, retiring, revoked_future, or compromised",
            ));
        }
    };
    Ok(status)
}

pub(super) fn personaldb_signing_key_to_proto(
    record: PersonalDbSigningKeyPublicRecord,
) -> PersonalDbSigningKeyRecord {
    PersonalDbSigningKeyRecord {
        key_id: record.trust_record.key_id.to_string(),
        key_generation: record.trust_record.key_generation.get(),
        purpose: record.trust_record.purpose.as_str().to_string(),
        database_scopes: record
            .trust_record
            .database_scopes
            .iter()
            .map(|database_id| database_id.0.clone())
            .collect(),
        group_scopes: record.trust_record.group_scopes.clone(),
        valid_from_log_index: record.trust_record.valid_from_log_index,
        valid_until_log_index: record.trust_record.valid_until_log_index,
        status: record.trust_record.status.as_str().to_string(),
        public_key: record.trust_record.public_key.as_bytes().to_vec(),
        created_at_unix_nanos: record.created_at_unix_nanos,
        updated_at_unix_nanos: record.updated_at_unix_nanos,
        created_by: record.created_audit.actor_id,
        updated_by: record.updated_audit.actor_id,
        record_revision: record.record_revision,
    }
}

pub(super) fn storage_class_to_proto(
    class: &crate::core_store::CoreStorageClass,
    default_class_id: &str,
) -> StorageClassDescriptor {
    StorageClassDescriptor {
        class_id: class.class_id.clone(),
        description: class.description.clone(),
        metadata_profile_id: class.metadata_profile.profile_id.clone(),
        metadata_replica_count: u32::from(class.metadata_profile.replica_count),
        metadata_prepare_quorum: u32::from(class.metadata_profile.prepare_quorum),
        metadata_certificate_persist_quorum: u32::from(
            class.metadata_profile.certificate_persist_quorum,
        ),
        metadata_fsync_mode: class.metadata_profile.fsync_mode.clone(),
        byte_profile_id: class.byte_profile.profile_id.clone(),
        byte_codec_id: class.byte_profile.codec_id.clone(),
        data_shards: u32::from(class.byte_profile.data_shards),
        parity_shards: u32::from(class.byte_profile.parity_shards),
        read_quorum: u32::from(class.byte_profile.read_quorum),
        write_publish_threshold: u32::from(class.byte_profile.write_publish_threshold),
        target_block_bytes: class.byte_profile.target_block_bytes,
        max_shard_bytes: class.byte_profile.max_shard_bytes,
        compression: class.byte_profile.compression.clone(),
        encryption: class.byte_profile.encryption.clone(),
        inline_payload_enabled: class.inline_payload_policy.enabled,
        max_inline_payload_bytes: class.inline_payload_policy.max_raw_payload_bytes,
        absolute_inline_record_max_bytes: class
            .inline_payload_policy
            .absolute_encoded_record_max_bytes,
        min_cell_spread: u32::from(class.min_cell_spread),
        tenant_selectable: class.tenant_selectable,
        is_default: class.class_id == default_class_id,
    }
}
