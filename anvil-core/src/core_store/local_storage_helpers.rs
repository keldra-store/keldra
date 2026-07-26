use super::*;

pub(super) fn current_unix_nanos_u64() -> Result<u64> {
    let nanos = Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("current time exceeds supported range"))?;
    u64::try_from(nanos).map_err(|_| anyhow!("current time is before unix epoch"))
}

pub(super) fn local_control_node_id(index: usize) -> String {
    format!("{LOCAL_CONTROL_NODE_ID_PREFIX}-{index}")
}

pub(super) fn local_control_node_ids() -> Vec<String> {
    (1..=LOCAL_CONTROL_REPLICA_COUNT)
        .map(local_control_node_id)
        .collect()
}

pub(super) fn local_erasure_profile(id: &str) -> Result<LocalErasureProfile> {
    match id {
        "ec-4-2" => Ok(LOCAL_EC_4_2_PROFILE),
        "ec-8-3" => Ok(LOCAL_EC_8_3_PROFILE),
        "replicated-3" => Ok(LOCAL_REPLICATED_3_PROFILE),
        _ => bail!("CoreStore unsupported erasure profile {id}"),
    }
}

pub(super) fn local_erasure_profile_from_byte_profile(
    profile: &CoreByteStorageProfile,
) -> Result<LocalErasureProfile> {
    profile.validate()?;
    let local = local_erasure_profile(&profile.profile_id)?;
    if local.codec_id != profile.codec_id
        || local.data_shards != usize::from(profile.data_shards)
        || local.parity_shards != usize::from(profile.parity_shards)
        || local.minimum_read_shards != usize::from(profile.read_quorum)
        || local.minimum_write_ack_shards != usize::from(profile.write_publish_threshold)
        || local.logical_block_target_bytes != profile.target_block_bytes
        || local.max_shard_size_bytes != profile.max_shard_bytes
    {
        bail!(
            "CoreStore byte profile {} does not match a compiled erasure profile",
            profile.profile_id
        );
    }
    Ok(local)
}

pub(super) fn core_pipeline_policy_from_storage_class(
    storage_class: &CoreStorageClass,
) -> Result<CorePipelinePolicy> {
    storage_class.validate()?;
    let profile = local_erasure_profile_from_byte_profile(&storage_class.byte_profile)?;
    Ok(CorePipelinePolicy {
        compression: storage_class.byte_profile.compression.clone(),
        encryption: storage_class.byte_profile.encryption.clone(),
        erasure_profile_id: profile.id.to_string(),
        placement_scope: format!("min-cell-spread:{}", storage_class.min_cell_spread),
        target_block_size: profile.logical_block_target_bytes,
        boundary_mode: "honour".to_string(),
    })
}

pub(super) fn coremeta_quorum_profile_from_metadata_profile(
    profile: &CoreMetadataProfile,
) -> Result<CoreMetaQuorumProfile> {
    profile.validate()?;
    Ok(CoreMetaQuorumProfile {
        profile_id: profile.profile_id.clone(),
        replica_count: usize::from(profile.replica_count),
        prepare_quorum: usize::from(profile.prepare_quorum),
        certificate_persist_quorum: usize::from(profile.certificate_persist_quorum),
    })
}

pub(super) fn selected_storage_class<'a>(
    catalog: &'a CoreStorageClassCatalog,
    requested: Option<&str>,
) -> Result<&'a CoreStorageClass> {
    catalog.select(requested)
}

pub(super) fn local_erasure_profile_for_counts(
    profile_id: &str,
    data_shards: usize,
    parity_shards: usize,
) -> Result<LocalErasureProfile> {
    let profile = local_erasure_profile(profile_id)?;
    if profile.data_shards != data_shards || profile.parity_shards != parity_shards {
        bail!(
            "CoreStore erasure profile {} count mismatch: expected {}+{}, got {}+{}",
            profile.id,
            profile.data_shards,
            profile.parity_shards,
            data_shards,
            parity_shards
        );
    }
    Ok(profile)
}

pub(super) fn plan_local_shard_placements(
    profile: LocalErasureProfile,
) -> Result<Vec<LocalShardPlacement>> {
    let placements = (0..profile.total_shards())
        .map(|shard_index| LocalShardPlacement {
            node_id: format!("{LOCAL_NODE_ID_PREFIX}-{}", shard_index + 1),
            region_id: "local".to_string(),
            cell_id: local_cell_id_for_shard(profile, shard_index),
            failure_domain: local_cell_id_for_shard(profile, shard_index),
            region_weight: 100,
            cell_weight: 100,
            public_api_addr: String::new(),
            is_local: true,
        })
        .collect::<Vec<_>>();
    validate_local_publish_placements(profile, &placements)?;
    Ok(placements)
}

pub(super) fn local_cell_count_for_profile(profile: LocalErasureProfile) -> usize {
    match profile.id {
        "ec-8-3" => 4,
        _ => 3,
    }
}

pub(super) fn local_cell_id_for_shard(profile: LocalErasureProfile, shard_index: usize) -> String {
    format!(
        "local-cell-{}",
        (shard_index % local_cell_count_for_profile(profile)) + 1
    )
}

pub(super) fn validate_local_publish_placements(
    profile: LocalErasureProfile,
    placements: &[LocalShardPlacement],
) -> Result<()> {
    if placements.len() != profile.total_shards() {
        bail!(
            "CoreStore placement for {} expected {} shards, got {}",
            profile.id,
            profile.total_shards(),
            placements.len()
        );
    }
    let unique_nodes = placements
        .iter()
        .map(|placement| placement.node_id.as_str())
        .collect::<BTreeSet<_>>();
    if unique_nodes.len() != placements.len() {
        bail!("CoreStore placement must put at most one shard on each node");
    }
    let mut failure_domain_counts = BTreeMap::<&str, usize>::new();
    for placement in placements {
        *failure_domain_counts
            .entry(placement.failure_domain.as_str())
            .or_default() += 1;
    }
    match profile.id {
        "ec-4-2" => {
            if failure_domain_counts.len() < 3
                || failure_domain_counts.values().any(|count| *count > 2)
            {
                bail!(
                    "CoreStore ec-4-2 placement requires at least 3 failure domains and at most 2 shards per failure domain"
                );
            }
        }
        "ec-8-3" => {
            if failure_domain_counts.len() < 4
                || failure_domain_counts.values().any(|count| *count > 3)
            {
                bail!(
                    "CoreStore ec-8-3 placement requires at least 4 failure domains and at most 3 shards per failure domain"
                );
            }
        }
        "replicated-3" => {
            if placements.len() < 3 || unique_nodes.len() < 3 {
                bail!("CoreStore replicated-3 placement requires at least 3 distinct nodes");
            }
        }
        _ => bail!("CoreStore unsupported erasure profile {}", profile.id),
    }
    Ok(())
}

pub(super) fn boundary_schema_ref_name(bucket: &str) -> String {
    format!("boundary_schema/bucket/{bucket}/current")
}

impl CoreStore {
    pub(super) fn default_storage_class(&self) -> Result<&CoreStorageClass> {
        selected_storage_class(&self.storage_classes, None)
    }

    pub(super) fn select_storage_class(
        &self,
        requested: Option<&str>,
    ) -> Result<&CoreStorageClass> {
        selected_storage_class(&self.storage_classes, requested)
    }

    pub fn storage_class_catalog(&self) -> &CoreStorageClassCatalog {
        &self.storage_classes
    }

    pub fn list_storage_classes(&self) -> Vec<CoreStorageClass> {
        self.storage_classes.classes.values().cloned().collect()
    }

    pub fn get_storage_class(&self, class_id: &str) -> Result<CoreStorageClass> {
        Ok(self.select_storage_class(Some(class_id))?.clone())
    }

    pub fn pipeline_policy_for_storage_class(
        &self,
        requested: Option<&str>,
    ) -> Result<CorePipelinePolicy> {
        core_pipeline_policy_from_storage_class(self.select_storage_class(requested)?)
    }

    pub fn resolve_storage_class_id(&self, requested: Option<&str>) -> Result<String> {
        Ok(self.select_storage_class(requested)?.class_id.clone())
    }

    pub(crate) fn default_coremeta_quorum_profile(&self) -> Result<CoreMetaQuorumProfile> {
        coremeta_quorum_profile_from_metadata_profile(
            &self.default_storage_class()?.metadata_profile,
        )
    }
}
