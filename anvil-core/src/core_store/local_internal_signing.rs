use super::*;

impl CoreStore {
    pub fn register_node_receipt_signing_public_key(
        &self,
        node_id: &str,
        public_key_bytes: &[u8],
    ) -> Result<String> {
        // Receipt keys bootstrap node identity verification and intentionally
        // remain available before root publication.
        store_node_receipt_signing_public_key(&self.meta, node_id, public_key_bytes)
    }

    pub fn sign_internal_core_receipt(&self, signed_payload_hash: &str) -> Result<Vec<u8>> {
        self.sign_core_receipt(signed_payload_hash)
    }

    pub fn verify_internal_core_receipt_signature(
        &self,
        node_id: &str,
        signed_payload_hash: &str,
        receipt_signature: &[u8],
    ) -> Result<()> {
        if node_id.trim().is_empty() {
            bail!("CoreStore internal receipt node id must not be empty");
        }
        let public_key = if node_id == self.node_identity.node_id {
            self.node_signing_keypair.public_key()
        } else if let Some(public_key) = load_node_receipt_signing_public_key(&self.meta, node_id)?
        {
            public_key
        } else if is_local_shard_node_id(node_id) || is_local_control_node_id(node_id) {
            // Standalone mode models local replicas with synthetic identities.
            // A received receipt for one of those identities must use an
            // explicitly registered key instead of silently trusting this
            // process's key.
            self.node_signing_keypair.public_key()
        } else {
            // The lifecycle descriptor is the canonical binding between a
            // node id and its receipt key. Materialise that binding on demand
            // so a projection refresh cannot leave verification behind.
            let nodes =
                crate::mesh_lifecycle::list_node_projections_with_core_store(self, None, None)?;
            if nodes.is_empty() {
                return Err(CoreStoreAvailabilityError::MeshTopologyUnavailable {
                    node_id: node_id.to_string(),
                }
                .into());
            }
            let descriptor = nodes
                .into_iter()
                .find(|node| node.node_id == node_id)
                .ok_or_else(|| {
                    anyhow!("CoreStore internal receipt references unknown node {node_id}")
                })?;
            self.register_node_receipt_signing_public_key(
                &descriptor.node_id,
                &descriptor.receipt_signing_public_key,
            )?;
            load_node_receipt_signing_public_key(&self.meta, node_id)?.ok_or_else(|| {
                anyhow!("CoreStore receipt key materialisation failed for node {node_id}")
            })?
        };
        public_key
            .verify(signed_payload_hash.as_bytes(), receipt_signature)
            .with_context(|| {
                format!(
                    "CoreStore internal receipt signature verification failed for node {node_id}"
                )
            })
    }
}
