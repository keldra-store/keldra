//! Read-only, authenticated proof for a quiescent derived-artifact journal suffix.
//! Uses an existing fixture node identity; never creates credentials or changes checkpoints.

use std::env;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use keldra_consensus::{
    ClusterId, CommittedPeerPinProvider, CommittedPeerPins, NodeId, PeerRpcKind, PeerTlsConfig,
    PeerTlsConnector, PeerTlsIdentity,
};
use keldra_index::v1::{parse_projection_artifact_path, parse_projection_catalog_path};
use keldra_store::{LocalChange, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, SourceId};
use serde::Deserialize;
use tonic::Request;
use tonic::transport::Endpoint;

mod data_wire {
    tonic::include_proto!("keldra.data_peer.v1");
}
mod cluster_wire {
    tonic::include_proto!("keldra.cluster_peer.v1");
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const SCHEMA: u32 = 1;
const MAX_IDENTITY_BYTES: u64 = 1024 * 1024;
const MAX_PAGE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = MAX_PAGE_BYTES as usize + 64 * 1024;
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
struct StoredIdentity {
    format_version: u16,
    cluster_id: [u8; 16],
    node_id: u64,
    presented_peer_identity: StoredPem,
    overlap_peer_identity: Option<StoredPem>,
}

#[derive(Deserialize)]
struct StoredPem {
    certificate_pem: String,
    private_key_pem: String,
}

/// This fixture's copied, existing server certificate is the only allowed pin.
/// Incoming RPC authorization remains the actual server's committed-state check.
struct FixturePins {
    node: NodeId,
    pins: CommittedPeerPins,
}

impl CommittedPeerPinProvider for FixturePins {
    fn connection_pins(&self, node_id: NodeId) -> Option<CommittedPeerPins> {
        (node_id == self.node).then_some(self.pins)
    }

    fn authorized_rpc_pins(
        &self,
        _: ClusterId,
        _: NodeId,
        _: PeerRpcKind,
    ) -> Option<CommittedPeerPins> {
        // This helper never accepts connections or authorizes callers.
        None
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    tokio::time::timeout(TOTAL_TIMEOUT, prove_suffix())
        .await
        .map_err(|_| "journal suffix proof exceeded its whole-operation deadline")??;
    Ok(())
}

async fn prove_suffix() -> TestResult<()> {
    let expected_node = unsigned("SOURCE_NODE_ID")?;
    let expected_tail = unsigned("EXPECTED_TAIL")?;
    let index_safe = unsigned("INDEX_SAFE_THROUGH")?;
    let fence_term = unsigned("FENCE_TERM")?;
    let fence_index = unsigned("FENCE_INDEX")?;
    if expected_node == 0
        || expected_node > u64::from(u16::MAX)
        || index_safe > expected_tail
        || fence_term == 0
        || fence_index == 0
    {
        return Err("invalid suffix source, cut, or captured fence".into());
    }
    let identity = load_identity(&required("IDENTITY_PATH")?)?;
    if identity.format_version != 1
        || identity.node_id != expected_node
        || identity.cluster_id == [0; 16]
    {
        return Err("existing identity does not match the expected fixture node".into());
    }
    let tls_identity = Arc::new(
        PeerTlsIdentity::from_pem(
            identity.presented_peer_identity.certificate_pem.as_bytes(),
            identity.presented_peer_identity.private_key_pem.as_bytes(),
        )
        .map_err(|_| "invalid existing fixture peer TLS identity")?,
    );
    let overlap = identity
        .overlap_peer_identity
        .as_ref()
        .map(|pem| {
            PeerTlsIdentity::from_pem(
                pem.certificate_pem.as_bytes(),
                pem.private_key_pem.as_bytes(),
            )
            .map(|tls| tls.spki_sha256())
            .map_err(|_| "invalid existing fixture overlap identity")
        })
        .transpose()?;
    let connector = PeerTlsConnector::new(
        tls_identity.clone(),
        Arc::new(FixturePins {
            node: NodeId(expected_node),
            pins: CommittedPeerPins {
                current: tls_identity.spki_sha256(),
                overlap,
            },
        }),
        PeerTlsConfig {
            handshake_timeout: RPC_TIMEOUT,
        },
    )
    .map_err(|_| "could not configure existing pinned peer TLS connector")?;
    let address = required("PEER_ADDRESS")?;
    let channel = Endpoint::from_static("http://keldra-peer.invalid")
        .connect_with_connector(tower::service_fn(move |_| {
            let connector = connector.clone();
            let address = address.clone();
            async move {
                connector
                    .connect(NodeId(expected_node), &address)
                    .await
                    .map(|connected| TokioIo::new(connected.stream))
                    .map_err(|_| std::io::Error::other("pinned peer TLS connection failed"))
            }
        }))
        .await?;
    let mut data = data_wire::data_peer_client::DataPeerClient::new(channel.clone())
        .max_decoding_message_size(MAX_RESPONSE_BYTES)
        .max_encoding_message_size(MAX_RESPONSE_BYTES);
    let mut cluster = cluster_wire::cluster_peer_client::ClusterPeerClient::new(channel)
        .max_decoding_message_size(MAX_RESPONSE_BYTES)
        .max_encoding_message_size(MAX_RESPONSE_BYTES);
    let peer = data_wire::PeerContext {
        schema_version: SCHEMA,
        cluster_id: identity.cluster_id.to_vec(),
        source_node_id: expected_node,
    };
    let fence_peer = cluster_wire::PeerContext {
        schema_version: SCHEMA,
        cluster_id: identity.cluster_id.to_vec(),
        source_node_id: expected_node,
        placement_term: fence_term,
        placement_index: fence_index,
        hop_count: 0,
        remaining_deadline_millis: RPC_TIMEOUT.as_millis() as u32,
    };
    require_fence(&mut cluster, &fence_peer, expected_node).await?;
    let before = data
        .get_source_journal_status(timed(data_wire::SourceJournalStatusRequest {
            peer: Some(peer.clone()),
        }))
        .await?
        .into_inner();
    let source = validate_status(&before, expected_node, expected_tail, index_safe)?;
    let mut after = index_safe;
    let mut pages = 0_u64;
    let mut records = 0_u64;
    let mut encoded_bytes = 0_u64;
    while after < expected_tail {
        let response = data
            .read_source_journal(timed(data_wire::SourceJournalReadRequest {
                peer: Some(peer.clone()),
                after_offset: after,
                limit: MAX_LOCAL_INVALIDATION_SCAN_RECORDS as u32,
                max_bytes: MAX_PAGE_BYTES,
            }))
            .await?
            .into_inner();
        let count = response.changes_json.len();
        let bytes = response.encoded_bytes;
        after = validate_page(&response, source, after, expected_tail)?;
        pages = pages.checked_add(1).ok_or("page count overflow")?;
        records = records
            .checked_add(count as u64)
            .ok_or("record count overflow")?;
        encoded_bytes = encoded_bytes
            .checked_add(bytes)
            .ok_or("byte count overflow")?;
    }
    let final_status = data
        .get_source_journal_status(timed(data_wire::SourceJournalStatusRequest {
            peer: Some(peer),
        }))
        .await?
        .into_inner();
    if validate_status(&final_status, expected_node, expected_tail, index_safe)? != source {
        return Err("source identity changed during the suffix proof".into());
    }
    require_fence(&mut cluster, &fence_peer, expected_node).await?;
    println!(
        "{}",
        serde_json::json!({
            "proof": "complete_reserved_derived_artifact_suffix",
            "source_node_id": source.node_id,
            "source_epoch": source.source_epoch,
            "captured_fence": {"term": fence_term, "index": fence_index},
            "index_safe_through": index_safe,
            "stable_tail": expected_tail,
            "settled_through": final_status.settled_through,
            "retention_floor_before": before.retention_floor,
            "retention_floor_after": final_status.retention_floor,
            "verified_records": records,
            "verified_pages": pages,
            "verified_encoded_bytes": encoded_bytes,
            "all_records_are_reserved_derived_artifacts": true
        })
    );
    Ok(())
}

async fn require_fence(
    client: &mut cluster_wire::cluster_peer_client::ClusterPeerClient<tonic::transport::Channel>,
    peer: &cluster_wire::PeerContext,
    expected_node: u64,
) -> TestResult<()> {
    // Admission checks exact applied ACTIVE membership and this captured fence.
    // This read changes neither journal nor checkpoint nor authorization state.
    let response = client
        .get_node_storage_observation(timed(cluster_wire::NodeStorageObservationRequest {
            peer: Some(peer.clone()),
        }))
        .await?
        .into_inner();
    if response.schema_version != SCHEMA || response.node_id != expected_node {
        return Err("fence-bound observation came from an unexpected node or schema".into());
    }
    Ok(())
}

fn timed<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request.set_timeout(RPC_TIMEOUT);
    request
}

fn load_identity(path: &str) -> TestResult<StoredIdentity> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() > MAX_IDENTITY_BYTES
    {
        return Err("fixture identity must be a bounded owner-only regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(MAX_IDENTITY_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_IDENTITY_BYTES {
        return Err("fixture identity exceeded the bounded read".into());
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid existing fixture identity document".into())
}

fn validate_status(
    status: &data_wire::SourceJournalStatus,
    expected_node: u64,
    expected_tail: u64,
    index_safe: u64,
) -> TestResult<SourceId> {
    let source: SourceId = serde_json::from_slice(&status.source_id_json)
        .map_err(|_| "invalid peer source identity encoding")?;
    if status.schema_version != SCHEMA
        || u64::from(source.node_id) != expected_node
        || source.source_epoch == [0; 32]
        || status.tail != expected_tail
        || status.settled_through != expected_tail
        || status.retention_floor > index_safe
        || status.retained_entries != status.tail.saturating_sub(status.retention_floor)
    {
        return Err("source status is not the exact stable retained suffix".into());
    }
    Ok(source)
}

fn validate_page(
    page: &data_wire::SourceJournalPage,
    expected_source: SourceId,
    after: u64,
    target: u64,
) -> TestResult<u64> {
    let source: SourceId = serde_json::from_slice(&page.source_id_json)
        .map_err(|_| "invalid page source identity encoding")?;
    let measured = page
        .changes_json
        .iter()
        .try_fold(0_u64, |sum, bytes| sum.checked_add(bytes.len() as u64))
        .ok_or("journal page byte count overflow")?;
    if page.schema_version != SCHEMA
        || source != expected_source
        || page.changes_json.is_empty()
        || page.changes_json.len() > MAX_LOCAL_INVALIDATION_SCAN_RECORDS
        || measured != page.encoded_bytes
        || measured > MAX_PAGE_BYTES
        || page.oversize_offset != 0
        || page.oversize_encoded_bytes != 0
    {
        return Err("journal page has invalid bounds, identity, or progress".into());
    }
    let mut next = after;
    for bytes in &page.changes_json {
        let change: LocalChange =
            serde_json::from_slice(bytes).map_err(|_| "invalid typed journal change")?;
        next = next.checked_add(1).ok_or("journal offset overflow")?;
        if change.offset() != next || next > target || !is_reserved_artifact_change(&change) {
            return Err("journal suffix is noncontiguous or contains a non-artifact change".into());
        }
    }
    Ok(next)
}

fn is_reserved_artifact_change(change: &LocalChange) -> bool {
    match change {
        LocalChange::ObjectHead(head) => {
            head.program_commit_cursor.is_none()
                && head.canonical_path.is_none()
                && head.definition_transition.is_none()
                && artifact_path(&head.exact_path)
        }
        LocalChange::RetainedVersionDeleted(deletion) => {
            deletion.canonical_path.is_none() && artifact_path(&deletion.exact_path)
        }
        // In particular, never hide an AtomicBatchPublished or a new variant.
        _ => false,
    }
}

fn artifact_path(path: &str) -> bool {
    if parse_projection_artifact_path(path).is_ok() || parse_projection_catalog_path(path).is_ok() {
        return true;
    }
    let Some(rest) = path.strip_prefix("_keldra/accounting/") else {
        return false;
    };
    let fields = rest.split('/').collect::<Vec<_>>();
    match fields.as_slice() {
        [id, "current"] => canonical_nonzero(id),
        [id, "sources", node] => canonical_nonzero(id) && canonical_nonzero(node),
        // Accounting definitions are not disposable publication artifacts.
        _ => false,
    }
}

fn canonical_nonzero(value: &str) -> bool {
    value
        .parse::<u64>()
        .is_ok_and(|id| id != 0 && id.to_string() == value)
}

fn required(suffix: &str) -> TestResult<String> {
    let name = format!("KELDRA_CUTOVER_JOURNAL_{suffix}");
    let value = env::var(&name).map_err(|_| format!("{name} is required"))?;
    if value.is_empty() {
        return Err(format!("{name} must not be empty").into());
    }
    Ok(value)
}

fn unsigned(suffix: &str) -> TestResult<u64> {
    required(suffix)?
        .parse()
        .map_err(|_| format!("invalid unsigned cutover field {suffix}").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use keldra_store::{ObjectHeadChange, ObjectHeadChangeKind, VersionId};

    fn head(offset: u64, path: &str) -> LocalChange {
        LocalChange::ObjectHead(ObjectHeadChange {
            offset,
            tenant_id: 1,
            bucket_id: 1,
            exact_path: path.into(),
            canonical_path: None,
            path_version: VersionId(7),
            kind: ObjectHeadChangeKind::Put,
            program_commit_cursor: None,
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
        })
    }

    fn page(source: SourceId, changes: Vec<LocalChange>) -> data_wire::SourceJournalPage {
        let changes_json = changes
            .iter()
            .map(|change| serde_json::to_vec(change).unwrap())
            .collect::<Vec<_>>();
        data_wire::SourceJournalPage {
            schema_version: SCHEMA,
            encoded_bytes: changes_json.iter().map(|b| b.len() as u64).sum(),
            changes_json,
            source_id_json: serde_json::to_vec(&source).unwrap(),
            oversize_offset: 0,
            oversize_encoded_bytes: 0,
        }
    }

    #[test]
    fn artifact_allowlist_is_canonical_and_excludes_definitions_and_user_paths() {
        let generation = keldra_index::v1::projection_generation_path(
            keldra_index::v1::ProjectionPartitionIdentity {
                family_id: [7; 32],
                source_node: 2,
                source_epoch: [5; 32],
                producer_node: 2,
                placement_term: 1,
                placement_index: 12,
            },
            [9; 32],
        );
        assert!(artifact_path(&generation));
        assert!(is_reserved_artifact_change(&head(1, &generation)));
        assert!(artifact_path("_keldra/accounting/7/current"));
        assert!(artifact_path("_keldra/accounting/7/sources/2"));
        for path in [
            "objects/7.json",
            "objects/_keldra/7",
            "_keldra/accounting/definitions/7",
            "_keldra/accounting/07/current",
            "_keldra/accounting/7/sources/0",
            "_keldra/index-projections/v1/malformed",
            "_keldra/other/7",
        ] {
            assert!(!artifact_path(path), "accepted {path}");
        }
        let mut change = head(1, "_keldra/accounting/7/current");
        assert!(is_reserved_artifact_change(&change));
        if let LocalChange::ObjectHead(head) = &mut change {
            head.program_commit_cursor = Some(3);
        }
        assert!(!is_reserved_artifact_change(&change));
        assert!(!is_reserved_artifact_change(&LocalChange::SequenceGap(
            keldra_store::SourceSequenceGap { offset: 2 },
        )));
    }

    #[test]
    fn complete_suffix_rejects_gaps_user_rows_epoch_changes_and_misreported_bytes() {
        let source = SourceId {
            node_id: 2,
            source_epoch: [9; 32],
        };
        let good = page(
            source,
            vec![
                head(8, "_keldra/accounting/7/current"),
                head(9, "_keldra/accounting/7/sources/2"),
            ],
        );
        assert_eq!(validate_page(&good, source, 7, 9).unwrap(), 9);
        assert!(validate_page(&good, source, 6, 9).is_err());
        assert!(validate_page(&good, source, 7, 8).is_err());
        assert!(
            validate_page(
                &good,
                SourceId {
                    source_epoch: [8; 32],
                    ..source
                },
                7,
                9
            )
            .is_err()
        );
        assert!(
            validate_page(&page(source, vec![head(8, "objects/8.json")]), source, 7, 8).is_err()
        );
        assert!(validate_page(&page(source, Vec::new()), source, 7, 8).is_err());
        let mut bad_bytes = good;
        bad_bytes.encoded_bytes += 1;
        assert!(validate_page(&bad_bytes, source, 7, 9).is_err());
    }

    #[test]
    fn stable_status_and_page_envelopes_fail_closed() {
        let source = SourceId {
            node_id: 2,
            source_epoch: [9; 32],
        };
        let good = data_wire::SourceJournalStatus {
            schema_version: SCHEMA,
            source_id_json: serde_json::to_vec(&source).unwrap(),
            tail: 9,
            settled_through: 9,
            retention_floor: 7,
            retained_entries: 2,
            retained_bytes: 256,
        };
        assert_eq!(validate_status(&good, 2, 9, 7).unwrap(), source);
        assert!(validate_status(&good, 1, 9, 7).is_err());
        assert!(validate_status(&good, 2, 8, 7).is_err());
        assert!(validate_status(&good, 2, 9, 6).is_err());
        for bad in [
            data_wire::SourceJournalStatus {
                schema_version: SCHEMA + 1,
                ..good.clone()
            },
            data_wire::SourceJournalStatus {
                settled_through: 8,
                ..good.clone()
            },
            data_wire::SourceJournalStatus {
                retained_entries: 3,
                ..good.clone()
            },
            data_wire::SourceJournalStatus {
                source_id_json: serde_json::to_vec(&SourceId {
                    source_epoch: [0; 32],
                    ..source
                })
                .unwrap(),
                ..good.clone()
            },
        ] {
            assert!(validate_status(&bad, 2, 9, 7).is_err());
        }
        let good_page = page(source, vec![head(8, "_keldra/accounting/7/current")]);
        for bad in [
            data_wire::SourceJournalPage {
                schema_version: SCHEMA + 1,
                ..good_page.clone()
            },
            data_wire::SourceJournalPage {
                oversize_offset: 9,
                ..good_page.clone()
            },
            data_wire::SourceJournalPage {
                oversize_encoded_bytes: MAX_PAGE_BYTES + 1,
                ..good_page
            },
        ] {
            assert!(validate_page(&bad, source, 7, 9).is_err());
        }
    }
}
