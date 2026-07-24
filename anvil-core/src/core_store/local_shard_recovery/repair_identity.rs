use super::*;

pub(super) fn physical_shard_repair_operation_id(
    task: &crate::tasks::RebalanceShardTaskPayload,
    expected: &CoreObjectPlacement,
    target: &LocalShardPlacement,
    placement_epoch: u64,
    placement_generation: u64,
) -> Result<String> {
    task.validate()?;
    for (value, label) in [
        (&target.node_id, "repair target node id"),
        (&target.region_id, "repair target region id"),
        (&target.cell_id, "repair target cell id"),
    ] {
        validate_logical_id(value, label)?;
    }
    if placement_epoch <= expected.placement_epoch {
        bail!("CoreStore physical shard repair epoch must advance");
    }
    if placement_generation <= expected.generation {
        bail!("CoreStore physical shard repair generation must advance");
    }

    let mut identity = b"anvil.physical_shard_repair_operation.v1".to_vec();
    append_repair_identity_component(&mut identity, &task.immutable_identity_bytes());
    append_repair_identity_component(&mut identity, task.block_id.as_bytes());
    identity.extend_from_slice(&expected.shard_index.to_be_bytes());
    append_repair_identity_component(&mut identity, expected.node_id.as_bytes());
    identity.extend_from_slice(&expected.placement_epoch.to_be_bytes());
    identity.extend_from_slice(&expected.generation.to_be_bytes());
    append_repair_identity_component(&mut identity, target.node_id.as_bytes());
    append_repair_identity_component(&mut identity, target.region_id.as_bytes());
    append_repair_identity_component(&mut identity, target.cell_id.as_bytes());
    identity.extend_from_slice(&placement_epoch.to_be_bytes());
    identity.extend_from_slice(&placement_generation.to_be_bytes());

    Ok(format!("shard-repair-operation-{}", sha256_hex(&identity)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_repair_operation_identity_is_stable_across_lease_retries() {
        let task = task();
        let expected = expected();
        let target = target("node-b");

        let first = physical_shard_repair_operation_id(&task, &expected, &target, 8, 12)
            .expect("first repair operation identity");
        let retried = physical_shard_repair_operation_id(&task, &expected, &target, 8, 12)
            .expect("retried repair operation identity");

        assert_eq!(first, retried);
        assert!(first.starts_with("shard-repair-operation-"));
        assert_eq!(first.len(), "shard-repair-operation-".len() + 64);
    }

    #[test]
    fn physical_repair_operation_identity_binds_task_target_and_generation() {
        let task = task();
        let expected = expected();
        let repair_target = target("node-b");
        let baseline = physical_shard_repair_operation_id(&task, &expected, &repair_target, 8, 12)
            .expect("baseline repair operation identity");

        let mut changed_task = task.clone();
        changed_task.manifest_root_generation += 1;
        for changed in [
            physical_shard_repair_operation_id(&changed_task, &expected, &repair_target, 8, 12),
            physical_shard_repair_operation_id(&task, &expected, &target("node-c"), 8, 12),
            physical_shard_repair_operation_id(&task, &expected, &repair_target, 9, 12),
            physical_shard_repair_operation_id(&task, &expected, &repair_target, 8, 13),
        ] {
            assert_ne!(
                baseline,
                changed.expect("changed repair operation identity")
            );
        }
    }

    fn task() -> crate::tasks::RebalanceShardTaskPayload {
        crate::tasks::RebalanceShardTaskPayload {
            object_hash: format!("sha256:{}", "12".repeat(32)),
            logical_size: 8_192,
            manifest_ref: format!("core-manifest-sha256:{}:profile:ec-4-2", "12".repeat(32)),
            block_id: "block-a".to_string(),
            manifest_root_key_hash: format!("sha256:{}", "34".repeat(32)),
            manifest_root_generation: 7,
            manifest_transaction_id: "manifest-mutation-a".to_string(),
            manifest_payload_digest: format!("blake3:{}", "56".repeat(32)),
        }
    }

    fn expected() -> CoreObjectPlacement {
        CoreObjectPlacement {
            shard_index: 3,
            node_id: "node-a".to_string(),
            region_id: "region-a".to_string(),
            cell_id: "cell-a".to_string(),
            shard_hash: format!("sha256:{}", "78".repeat(32)),
            stored_size: 2_048,
            generation: 11,
            placement_epoch: 7,
            fsync_sequence: 19,
            written_at_unix_nanos: 100,
            signed_payload_hash: format!("sha256:{}", "9a".repeat(32)),
            signature_algorithm: "ed25519".to_string(),
            receipt_signature: vec![1, 2, 3],
        }
    }

    fn target(node_id: &str) -> LocalShardPlacement {
        LocalShardPlacement {
            node_id: node_id.to_string(),
            region_id: "region-b".to_string(),
            cell_id: "cell-b".to_string(),
            failure_domain: "region-b/cell-b".to_string(),
            region_weight: 1,
            cell_weight: 1,
            public_api_addr: format!("http://{node_id}:50051"),
            is_local: false,
        }
    }
}
