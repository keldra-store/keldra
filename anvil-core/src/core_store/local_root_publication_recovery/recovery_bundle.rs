use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::core_store::local) struct CoreMetaRecoveryPublicationBundle {
    pub(in crate::core_store::local) transaction_id: String,
    pub(in crate::core_store::local) publisher_node_id: String,
    pub(in crate::core_store::local) scopes: Vec<(String, u64)>,
    pub(in crate::core_store::local) coordinator_scope: (String, u64),
    pub(in crate::core_store::local) guard_context_hash: Option<String>,
    pub(in crate::core_store::local) transaction_expires_at_unix_nanos: u64,
    pub(in crate::core_store::local) guard_visible_update_count: u64,
    pub(in crate::core_store::local) guard_precondition_count: u64,
}

pub(in crate::core_store::local) fn decode_coremeta_recovery_publication_bundle(
    encoded: &[u8],
) -> Result<CoreMetaRecoveryPublicationBundle> {
    let proto = decode_canonical::<ReplicaPublicationIntentProto>(
        encoded,
        "CoreMeta recovery publication bundle",
    )?;
    if proto.schema != REPLICA_INTENT_SCHEMA
        || proto.roots.is_empty()
        || proto.roots.len() > MAX_PUBLICATION_ROOTS
    {
        bail!("CoreMeta recovery publication bundle is invalid");
    }
    validate_logical_id(
        &proto.transaction_id,
        "CoreMeta recovery publication transaction id",
    )?;
    match proto.guard_context_hash.as_deref() {
        Some(hash) => {
            validate_hash(hash, "CoreMeta recovery publication guard hash")?;
            if proto.guard_visible_update_count > MAX_PUBLICATION_ROWS as u64
                || proto.guard_precondition_count > MAX_PUBLICATION_ROWS as u64
            {
                bail!("CoreMeta recovery publication guard count exceeds its bound");
            }
        }
        None if proto.transaction_expires_at_unix_nanos == 0
            && proto.guard_visible_update_count == 0
            && proto.guard_precondition_count == 0 => {}
        None => bail!("CoreMeta recovery publication guard summary is incomplete"),
    }
    validate_logical_id(
        &proto.publisher_node_id,
        "CoreMeta recovery publication publisher node id",
    )?;
    if proto.local_rows.len() > MAX_PUBLICATION_ROWS {
        bail!("CoreMeta recovery publication local row count exceeds its bound");
    }
    for row in proto.local_rows.iter().cloned() {
        validate_local_intent_row(&owned_row_from_proto(row)?)?;
    }
    let mut scopes = Vec::with_capacity(proto.roots.len());
    let mut coordinators = Vec::new();
    for root in &proto.roots {
        if root.transaction_id != proto.transaction_id
            || root.root_key_hash != root_key_hash(&root.root_anchor_key)
            || root.post_root_generation == 0
            || root.expected_root_generation.saturating_add(1) != root.post_root_generation
            || root.certificate_hash.is_some()
        {
            bail!("CoreMeta recovery publication root scope is invalid");
        }
        let scope = (root.root_key_hash.clone(), root.post_root_generation);
        if root.transaction_coordinator {
            coordinators.push(scope.clone());
        }
        scopes.push(scope);
    }
    let coordinator_scope = match coordinators.as_slice() {
        [scope] => scope.clone(),
        [] if scopes.len() == 1 => scopes[0].clone(),
        [] => bail!("CoreMeta recovery publication bundle has no coordinator"),
        _ => bail!("CoreMeta recovery publication bundle has multiple coordinators"),
    };
    scopes.sort();
    if scopes.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("CoreMeta recovery publication bundle contains duplicate roots");
    }
    Ok(CoreMetaRecoveryPublicationBundle {
        transaction_id: proto.transaction_id,
        publisher_node_id: proto.publisher_node_id,
        scopes,
        coordinator_scope,
        guard_context_hash: proto.guard_context_hash,
        transaction_expires_at_unix_nanos: proto.transaction_expires_at_unix_nanos,
        guard_visible_update_count: proto.guard_visible_update_count,
        guard_precondition_count: proto.guard_precondition_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded_bundle(coordinators: &[bool]) -> Vec<u8> {
        let transaction_id = "recovery-transaction";
        let roots = coordinators
            .iter()
            .enumerate()
            .map(|(index, transaction_coordinator)| {
                let root_anchor_key = format!("recovery/root/{index}");
                PublicationRootProto {
                    transaction_id: transaction_id.to_string(),
                    root_key_hash: root_key_hash(&root_anchor_key),
                    root_anchor_key,
                    expected_root_generation: 0,
                    post_root_generation: 1,
                    transaction_coordinator: *transaction_coordinator,
                    ..Default::default()
                }
            })
            .collect();
        encode_deterministic_proto(&ReplicaPublicationIntentProto {
            schema: REPLICA_INTENT_SCHEMA.to_string(),
            transaction_id: transaction_id.to_string(),
            publisher_node_id: "node-a".to_string(),
            roots,
            ..Default::default()
        })
    }

    fn encoded_guarded_bundle() -> Vec<u8> {
        let mut bundle = ReplicaPublicationIntentProto::decode(encoded_bundle(&[false]).as_slice())
            .expect("decode test publication bundle");
        bundle.guard_context_hash = Some(format!("sha256:{}", "1".repeat(64)));
        bundle.transaction_expires_at_unix_nanos = 42;
        bundle.guard_visible_update_count = 3;
        bundle.guard_precondition_count = 2;
        encode_deterministic_proto(&bundle)
    }

    #[test]
    fn recovery_bundle_retains_the_declared_coordinator() {
        let bundle = decode_coremeta_recovery_publication_bundle(&encoded_bundle(&[false, true]))
            .expect("decode publication bundle");

        assert_eq!(bundle.scopes.len(), 2);
        assert_eq!(
            bundle.coordinator_scope,
            (root_key_hash("recovery/root/1"), 1)
        );
    }

    #[test]
    fn singleton_recovery_bundle_is_its_implicit_coordinator() {
        let bundle = decode_coremeta_recovery_publication_bundle(&encoded_bundle(&[false]))
            .expect("decode singleton publication bundle");

        assert_eq!(bundle.coordinator_scope, bundle.scopes[0]);
    }

    #[test]
    fn grouped_recovery_bundle_requires_exactly_one_coordinator() {
        let missing = decode_coremeta_recovery_publication_bundle(&encoded_bundle(&[false, false]))
            .expect_err("grouped bundle without coordinator must fail");
        assert!(missing.to_string().contains("no coordinator"));

        let duplicate = decode_coremeta_recovery_publication_bundle(&encoded_bundle(&[true, true]))
            .expect_err("grouped bundle with multiple coordinators must fail");
        assert!(duplicate.to_string().contains("multiple coordinators"));
    }

    #[test]
    fn recovery_bundle_retains_the_exact_publication_guard_summary() {
        let bundle = decode_coremeta_recovery_publication_bundle(&encoded_guarded_bundle())
            .expect("decode guarded publication bundle");
        let expected_hash = format!("sha256:{}", "1".repeat(64));

        assert_eq!(
            bundle.guard_context_hash.as_deref(),
            Some(expected_hash.as_str())
        );
        assert_eq!(bundle.transaction_expires_at_unix_nanos, 42);
        assert_eq!(bundle.guard_visible_update_count, 3);
        assert_eq!(bundle.guard_precondition_count, 2);
    }
}
