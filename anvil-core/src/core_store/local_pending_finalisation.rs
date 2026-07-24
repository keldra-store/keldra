use super::local_admission::same_pending_mutation_finalisation;
use super::*;
use crate::anvil_api::{
    PublishPendingMutationFinalisationRequest,
    core_meta_replication_internal_client::CoreMetaReplicationInternalClient,
};
use tonic::metadata::MetadataValue;

const PUBLISH_FINALISATION_OPERATION: &str = "coremeta.publish_pending_mutation_finalisation";
const MAX_FINALISATION_IDENTITY_BYTES: usize = 1024;
const MAX_FINALISATION_COLLECTION_ENTRIES: usize = 4096;

impl CoreStore {
    pub(in crate::core_store::local) async fn publish_pending_mutation_finalisation_transaction_record(
        &self,
        finalisation: &CorePendingMutationFinalisationRecord,
    ) -> Result<CorePendingMutationFinalisationRecord> {
        validate_pending_mutation_finalisation(
            finalisation,
            &self.node_identity.node_id,
            FinalisationTimestamp::Proposal,
        )?;
        match self.pending_mutation_finalisation_write_route().await? {
            CoreMetaWriteRoute::Local => {
                self.publish_pending_mutation_finalisation_transaction_record_locally(finalisation)
                    .await
            }
            CoreMetaWriteRoute::Remote(target) => {
                self.publish_pending_mutation_finalisation_remotely(finalisation, target)
                    .await
            }
        }
    }

    pub(crate) async fn pending_mutation_finalisation_write_route(
        &self,
    ) -> Result<CoreMetaWriteRoute> {
        let root_key_hash =
            local_roots_layout::stream_coremeta_root_key_hash(CORE_TRANSACTION_STREAM_ID);
        self.coremeta_write_route(&root_key_hash).await
    }

    pub(crate) fn validate_pending_mutation_finalisation_proposal_bytes(
        bytes: &[u8],
        expected_source_node_id: &str,
    ) -> Result<()> {
        let finalisation = decode_pending_mutation_finalisation_record(bytes)?;
        validate_pending_mutation_finalisation(
            &finalisation,
            expected_source_node_id,
            FinalisationTimestamp::Proposal,
        )
    }

    pub(crate) async fn publish_pending_mutation_finalisation_proposal_as_owner(
        &self,
        bytes: &[u8],
        expected_source_node_id: &str,
    ) -> Result<Vec<u8>> {
        let finalisation = decode_pending_mutation_finalisation_record(bytes)?;
        validate_pending_mutation_finalisation(
            &finalisation,
            expected_source_node_id,
            FinalisationTimestamp::Proposal,
        )?;
        if let CoreMetaWriteRoute::Remote(target) =
            self.pending_mutation_finalisation_write_route().await?
        {
            bail!(
                "CoreStore pending mutation finalisation owner changed to {}",
                target.node_id
            );
        }
        let canonical = self
            .publish_pending_mutation_finalisation_transaction_record_locally(&finalisation)
            .await?;
        validate_pending_mutation_finalisation(
            &canonical,
            expected_source_node_id,
            FinalisationTimestamp::Canonical,
        )?;
        encode_pending_mutation_finalisation_record(&canonical)
    }

    pub(crate) fn pending_mutation_finalisation_rpc_payload_hash(bytes: &[u8]) -> String {
        let domain = b"anvil.internal.pending_mutation_finalisation.v1";
        let mut input = Vec::with_capacity(16 + domain.len() + bytes.len());
        input.extend_from_slice(&(domain.len() as u64).to_le_bytes());
        input.extend_from_slice(domain);
        input.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        input.extend_from_slice(bytes);
        format!("sha256:{}", sha256_hex(&input))
    }

    async fn publish_pending_mutation_finalisation_remotely(
        &self,
        finalisation: &CorePendingMutationFinalisationRecord,
        target: CoreMetaPeerTarget,
    ) -> Result<CorePendingMutationFinalisationRecord> {
        let bearer = self
            .node_identity
            .internal_bearer_token
            .as_deref()
            .ok_or_else(|| {
                anyhow!(
                    "CoreStore pending mutation finalisation selected {}, but no internal bearer token is configured",
                    target.node_id
                )
            })?;
        let finalisation_record = encode_pending_mutation_finalisation_record(finalisation)?;
        let payload_hash =
            Self::pending_mutation_finalisation_rpc_payload_hash(&finalisation_record);
        let request_body = PublishPendingMutationFinalisationRequest {
            header: Some(self.internal_request_header(PUBLISH_FINALISATION_OPERATION)?),
            finalisation_record,
            source_signature: self.sign_internal_core_receipt(&payload_hash)?,
            payload_hash,
        };
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode pending mutation finalisation bearer token")?;
        let response = self
            .internal_grpc_request(
                &target.public_api_addr,
                "publish pending mutation finalisation",
                move |channel| {
                    let mut client = CoreMetaReplicationInternalClient::new(channel);
                    let mut request = tonic::Request::new(request_body.clone());
                    request
                        .metadata_mut()
                        .insert("authorization", authorization.clone());
                    async move {
                        client
                            .publish_pending_mutation_finalisation(request)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
            )
            .await
            .with_context(|| {
                format!(
                    "publish pending mutation finalisation through {}",
                    target.node_id
                )
            })?;
        let canonical = decode_pending_mutation_finalisation_record(&response.finalisation_record)?;
        validate_pending_mutation_finalisation(
            &canonical,
            &self.node_identity.node_id,
            FinalisationTimestamp::Canonical,
        )?;
        if !same_pending_mutation_finalisation(finalisation, &canonical) {
            bail!("CoreStore pending mutation finalisation owner returned a conflicting record");
        }
        Ok(canonical)
    }
}

#[derive(Clone, Copy)]
enum FinalisationTimestamp {
    Proposal,
    Canonical,
}

fn validate_pending_mutation_finalisation(
    finalisation: &CorePendingMutationFinalisationRecord,
    expected_source_node_id: &str,
    timestamp: FinalisationTimestamp,
) -> Result<()> {
    if finalisation.node_id != expected_source_node_id {
        bail!("CoreStore pending mutation finalisation source node mismatch");
    }
    for (value, label) in [
        (&finalisation.node_id, "source node id"),
        (&finalisation.mutation_id, "mutation id"),
        (&finalisation.operation_family, "operation family"),
        (&finalisation.writer_family, "writer family"),
        (&finalisation.state, "state"),
    ] {
        validate_finalisation_identity(value, label)?;
    }
    if finalisation.mutation_epoch == 0 || finalisation.mutation_sequence == 0 {
        bail!("CoreStore pending mutation finalisation mutation position must be non-zero");
    }
    match timestamp {
        FinalisationTimestamp::Proposal if finalisation.finalised_at_unix_nanos != 0 => {
            bail!("CoreStore pending mutation finalisation proposal has a timestamp");
        }
        FinalisationTimestamp::Canonical if finalisation.finalised_at_unix_nanos == 0 => {
            bail!("CoreStore canonical pending mutation finalisation has no timestamp");
        }
        _ => {}
    }
    if !matches!(
        finalisation.state.as_str(),
        "committed"
            | "finalisation_failed"
            | "aborted"
            | "rolled_back"
            | "expired"
            | "failed"
            | "superseded"
    ) {
        bail!("CoreStore pending mutation finalisation state is not terminal");
    }
    let expected_family = match &finalisation.target {
        CorePendingMutationTarget::ObjectPut { .. } => "object.put",
        CorePendingMutationTarget::StreamAppend { .. } => "stream.append",
        CorePendingMutationTarget::MutationBatch { .. } => "mutation.batch",
    };
    if finalisation.operation_family != expected_family {
        bail!("CoreStore pending mutation finalisation target/family mismatch");
    }
    validate_writer_family(
        &finalisation.writer_family,
        "pending mutation finalisation writer family",
    )?;
    if finalisation.boundary_values.len() > MAX_FINALISATION_COLLECTION_ENTRIES
        || finalisation.landed_bytes.len() > MAX_FINALISATION_COLLECTION_ENTRIES
    {
        bail!("CoreStore pending mutation finalisation collection exceeds bounded size");
    }
    for landed in &finalisation.landed_bytes {
        validate_hash(&landed.sha256, "pending mutation finalisation landed hash")?;
        validate_finalisation_identity(&landed.landing_id, "landed byte id")?;
        if landed.relative_path.is_empty()
            || landed.relative_path.len() > MAX_FINALISATION_IDENTITY_BYTES * 4
            || landed.relative_path.contains('\0')
        {
            bail!("CoreStore pending mutation finalisation landed path is invalid");
        }
    }
    Ok(())
}

fn validate_finalisation_identity(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_FINALISATION_IDENTITY_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        bail!("CoreStore pending mutation finalisation {label} is invalid");
    }
    Ok(())
}
