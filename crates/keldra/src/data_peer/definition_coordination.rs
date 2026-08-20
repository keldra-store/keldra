use keldra_consensus::PeerRpcKind;
use keldra_store::{
    DefinitionAssignment, DefinitionAssignmentCursor, DefinitionAssignmentMutation,
    DefinitionCheckpoint, DefinitionConsumerKind, DefinitionDeletion, DefinitionKind,
    DefinitionLocator, DefinitionLocatorCursor, DefinitionOperation, DefinitionStateError,
    JournalRoute, MAX_DEFINITION_STATE_SCAN_RECORDS, PlacementLogId, RoutedJournalError, SourceId,
    VersionId,
};
use tonic::{Request, Response, Status};

use super::{
    DATA_PEER_SCHEMA_VERSION, DataPeerService, MAX_TYPED_MUTATION_BYTES, encode_page, wire,
};

#[cfg(test)]
macro_rules! denied_test_calls {
    ($client:ident, $peer:ident, $require_denied:ident) => {
        $require_denied!(
            $client.read_routed_source_journal(wire::RoutedSourceJournalReadRequest {
                peer: Some($peer.clone()),
                route: wire::RoutedJournalKind::IndexDefinitions as i32,
                tenant_id: 0,
                bucket_id: 0,
                source_id_json: Vec::new(),
                after_offset: 0,
                target_offset: 0,
                limit: 1,
                max_bytes: 1,
            }),
            "ReadRoutedSourceJournal"
        );
        $require_denied!(
            $client.apply_definition_assignment_page(wire::ApplyDefinitionAssignmentPageRequest {
                peer: Some($peer.clone()),
                mutations: Vec::new(),
                consumer_kind: wire::PrivateDefinitionConsumerKind::IndexAssignments as i32,
                source_node_id: 2,
                source_epoch: vec![2; 32],
                next_offset: 1,
                observed_fence_term: 1,
                observed_fence_index: 1,
            },),
            "ApplyDefinitionAssignmentPage"
        );
        $require_denied!(
            $client.get_definition_checkpoint(wire::DefinitionCheckpointRequest {
                peer: Some($peer.clone()),
                consumer_kind: wire::PrivateDefinitionConsumerKind::IndexAssignments as i32,
                source_node_id: 2,
            }),
            "GetDefinitionCheckpoint"
        );
        $require_denied!(
            $client.apply_definition_assignments(wire::ApplyDefinitionAssignmentsRequest {
                peer: Some($peer.clone()),
                mutations: Vec::new(),
            }),
            "ApplyDefinitionAssignments"
        );
        $require_denied!(
            $client.scan_definition_locators_by_bucket(wire::DefinitionLocatorScanRequest {
                peer: Some($peer.clone()),
                kind: wire::PrivateDefinitionKind::Index as i32,
                tenant_id: 1,
                bucket_id: 2,
                cursor: Vec::new(),
                limit: 1,
            }),
            "ScanDefinitionLocatorsByBucket"
        );
        $require_denied!(
            $client.scan_definition_locators_by_kind(wire::DefinitionLocatorKindScanRequest {
                peer: Some($peer.clone()),
                kind: wire::PrivateDefinitionKind::Index as i32,
                cursor: Vec::new(),
                limit: 1,
            }),
            "ScanDefinitionLocatorsByKind"
        );
        $require_denied!(
            $client.scan_definition_assignments_by_kind(wire::DefinitionAssignmentScanRequest {
                peer: Some($peer.clone()),
                kind: wire::PrivateDefinitionKind::Index as i32,
                cursor: Vec::new(),
                limit: 1,
            }),
            "ScanDefinitionAssignmentsByKind"
        );
    };
}

#[cfg(test)]
pub(super) use denied_test_calls;

pub(super) async fn read_routed_source_journal(
    service: &DataPeerService,
    mut request: Request<wire::RoutedSourceJournalReadRequest>,
) -> Result<Response<wire::RoutedSourceJournalPage>, Status> {
    let peer = request.get_ref().peer.clone();
    service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let value = request.into_inner();
    let route = decode_route(&value)?;
    let source_id: SourceId = super::decode_typed(&value.source_id_json)?;
    let limit = usize::try_from(value.limit)
        .unwrap_or(usize::MAX)
        .min(keldra_store::MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
    if value.max_bytes == 0 || value.max_bytes > MAX_TYPED_MUTATION_BYTES as u64 {
        return Err(Status::invalid_argument(
            "routed source-journal byte limit is outside the private peer bound",
        ));
    }
    let store = service.store.clone();
    let page = tokio::task::spawn_blocking(move || {
        store.scan_routed_local_changes(
            route,
            source_id,
            value.after_offset,
            value.target_offset,
            limit,
            value.max_bytes,
        )
    })
    .await
    .map_err(|error| Status::internal(format!("routed journal read task failed: {error}")))?
    .map_err(map_routed_journal_error)?;
    let changes_json = encode_page(page.changes)?;
    let measured = changes_json
        .iter()
        .try_fold(0_u64, |total, value| total.checked_add(value.len() as u64));
    if measured != Some(page.encoded_bytes) {
        return Err(Status::internal(
            "routed source-journal byte accounting is inconsistent",
        ));
    }
    let (oversize_offset, oversize_encoded_bytes) = page
        .oversize
        .map_or((0, 0), |value| (value.offset, value.encoded_bytes));
    Ok(Response::new(wire::RoutedSourceJournalPage {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        changes_json,
        encoded_bytes: page.encoded_bytes,
        source_node_id: u64::from(page.source_id.node_id),
        source_epoch: page.source_id.source_epoch.to_vec(),
        through_offset: page.through_offset,
        oversize_offset,
        oversize_encoded_bytes,
    }))
}

pub(super) async fn apply_definition_assignment_page(
    service: &DataPeerService,
    mut request: Request<wire::ApplyDefinitionAssignmentPageRequest>,
) -> Result<Response<wire::DefinitionAssignmentPageApplied>, Status> {
    let peer = request.get_ref().peer.clone();
    let authenticated = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
    let metadata = request.metadata().clone();
    let value = request.into_inner();
    if value.mutations.len() > MAX_DEFINITION_STATE_SCAN_RECORDS as usize {
        return Err(Status::resource_exhausted(
            "definition assignment page exceeds the private peer item limit",
        ));
    }
    let checkpoint = decode_checkpoint(&value)?;
    let fence = service.mutation_admission.definition_assignment_page(
        authenticated,
        checkpoint.source_id.node_id,
        checkpoint.observed_fence,
    )?;
    let mutations = value
        .mutations
        .iter()
        .map(decode_assignment_mutation)
        .collect::<Result<Vec<_>, Status>>()?;
    let mutation_count = mutations.len() as u32;
    let store = service.store.clone();
    service
        .bounded(&metadata, async move {
            tokio::task::spawn_blocking(move || {
                store.apply_definition_assignment_page(&mutations, &checkpoint)
            })
            .await
            .map_err(|error| Status::internal(format!("assignment apply task failed: {error}")))?
            .map_err(map_definition_state_error)
        })
        .await?;
    service.mutation_admission.require_fence(fence)?;
    Ok(Response::new(wire::DefinitionAssignmentPageApplied {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        mutation_count,
    }))
}

pub(super) async fn get_definition_checkpoint(
    service: &DataPeerService,
    mut request: Request<wire::DefinitionCheckpointRequest>,
) -> Result<Response<wire::DefinitionCheckpointState>, Status> {
    let peer = request.get_ref().peer.clone();
    let authenticated =
        service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let metadata = request.metadata().clone();
    let value = request.into_inner();
    let consumer_kind = decode_consumer_kind(value.consumer_kind)?;
    let source_node_id = u16::try_from(value.source_node_id)
        .map_err(|_| Status::invalid_argument("definition checkpoint source node is invalid"))?;
    let fence = service
        .mutation_admission
        .definition_checkpoint(authenticated, source_node_id)?;
    let store = service.store.clone();
    let checkpoint = service
        .bounded(&metadata, async move {
            tokio::task::spawn_blocking(move || {
                store.definition_checkpoint(consumer_kind, source_node_id)
            })
            .await
            .map_err(|error| Status::internal(format!("checkpoint read task failed: {error}")))?
            .map_err(map_definition_state_error)
        })
        .await?;
    service.mutation_admission.require_fence(fence)?;
    let (present, source_epoch, next_offset, observed_fence_term, observed_fence_index) =
        checkpoint.map_or((false, Vec::new(), 0, 0, 0), |checkpoint| {
            (
                true,
                checkpoint.source_id.source_epoch.to_vec(),
                checkpoint.next_offset,
                checkpoint.observed_fence.term,
                checkpoint.observed_fence.index,
            )
        });
    Ok(Response::new(wire::DefinitionCheckpointState {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        present,
        consumer_kind: value.consumer_kind,
        source_node_id: value.source_node_id,
        source_epoch,
        next_offset,
        observed_fence_term,
        observed_fence_index,
    }))
}

pub(super) async fn apply_definition_assignments(
    service: &DataPeerService,
    mut request: Request<wire::ApplyDefinitionAssignmentsRequest>,
) -> Result<Response<wire::DefinitionAssignmentPageApplied>, Status> {
    let peer = request.get_ref().peer.clone();
    let authenticated = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
    let metadata = request.metadata().clone();
    let value = request.into_inner();
    if value.mutations.is_empty()
        || value.mutations.len() > MAX_DEFINITION_STATE_SCAN_RECORDS as usize
    {
        return Err(Status::invalid_argument(
            "membership assignment transfer must contain a bounded non-empty page",
        ));
    }
    let mutations = value
        .mutations
        .iter()
        .map(decode_assignment_mutation)
        .collect::<Result<Vec<_>, Status>>()?;
    let expected_fence = mutations[0].observed_fence();
    if mutations
        .iter()
        .any(|mutation| mutation.observed_fence() != expected_fence)
    {
        return Err(Status::invalid_argument(
            "membership assignment transfer mixes placement fences",
        ));
    }
    let fence = service
        .mutation_admission
        .definition_assignments(authenticated, expected_fence)?;
    let mutation_count = mutations.len() as u32;
    let store = service.store.clone();
    service
        .bounded(&metadata, async move {
            tokio::task::spawn_blocking(move || {
                store.apply_definition_assignment_mutations(&mutations)
            })
            .await
            .map_err(|error| Status::internal(format!("assignment transfer task failed: {error}")))?
            .map_err(map_definition_state_error)
        })
        .await?;
    service.mutation_admission.require_fence(fence)?;
    Ok(Response::new(wire::DefinitionAssignmentPageApplied {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        mutation_count,
    }))
}

pub(super) async fn scan_definition_locators_by_bucket(
    service: &DataPeerService,
    mut request: Request<wire::DefinitionLocatorScanRequest>,
) -> Result<Response<wire::DefinitionLocatorScanPage>, Status> {
    let peer = request.get_ref().peer.clone();
    service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let metadata = request.metadata().clone();
    let value = request.into_inner();
    let kind = decode_kind(value.kind)?;
    if value.tenant_id == 0
        || value.bucket_id == 0
        || value.limit == 0
        || value.limit > MAX_DEFINITION_STATE_SCAN_RECORDS
    {
        return Err(Status::invalid_argument(
            "definition locator scan identity or limit is invalid",
        ));
    }
    let cursor = (!value.cursor.is_empty())
        .then(|| DefinitionLocatorCursor::from_bytes(value.cursor))
        .transpose()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let store = service.store.clone();
    let page = service
        .bounded(&metadata, async move {
            tokio::task::spawn_blocking(move || {
                store.scan_definition_locators_by_bucket(
                    kind,
                    value.tenant_id,
                    value.bucket_id,
                    cursor.as_ref(),
                    value.limit,
                )
            })
            .await
            .map_err(|error| Status::internal(format!("locator scan task failed: {error}")))?
            .map_err(map_definition_state_error)
        })
        .await?;
    let has_more = page.next_cursor.is_some();
    Ok(Response::new(wire::DefinitionLocatorScanPage {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        locators: page.locators.into_iter().map(encode_locator).collect(),
        next_cursor: page
            .next_cursor
            .map_or_else(Vec::new, |cursor| cursor.as_bytes().to_vec()),
        has_more,
    }))
}

pub(super) async fn scan_definition_locators_by_kind(
    service: &DataPeerService,
    mut request: Request<wire::DefinitionLocatorKindScanRequest>,
) -> Result<Response<wire::DefinitionLocatorScanPage>, Status> {
    let peer = request.get_ref().peer.clone();
    service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let metadata = request.metadata().clone();
    let value = request.into_inner();
    let kind = decode_kind(value.kind)?;
    if value.limit == 0 || value.limit > MAX_DEFINITION_STATE_SCAN_RECORDS {
        return Err(Status::invalid_argument(
            "definition locator kind scan limit is invalid",
        ));
    }
    let cursor = (!value.cursor.is_empty())
        .then(|| DefinitionLocatorCursor::from_bytes(value.cursor))
        .transpose()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let store = service.store.clone();
    let page = service
        .bounded(&metadata, async move {
            tokio::task::spawn_blocking(move || {
                store.scan_definition_locators(Some(kind), cursor.as_ref(), value.limit)
            })
            .await
            .map_err(|error| Status::internal(format!("locator scan task failed: {error}")))?
            .map_err(map_definition_state_error)
        })
        .await?;
    Ok(Response::new(encode_locator_page(page)))
}

pub(super) async fn scan_definition_assignments_by_kind(
    service: &DataPeerService,
    mut request: Request<wire::DefinitionAssignmentScanRequest>,
) -> Result<Response<wire::DefinitionAssignmentScanPage>, Status> {
    let peer = request.get_ref().peer.clone();
    service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let metadata = request.metadata().clone();
    let value = request.into_inner();
    let kind = decode_kind(value.kind)?;
    if value.limit == 0 || value.limit > MAX_DEFINITION_STATE_SCAN_RECORDS {
        return Err(Status::invalid_argument(
            "definition assignment kind scan limit is invalid",
        ));
    }
    let cursor = (!value.cursor.is_empty())
        .then(|| DefinitionAssignmentCursor::from_bytes(value.cursor))
        .transpose()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let store = service.store.clone();
    let page = service
        .bounded(&metadata, async move {
            tokio::task::spawn_blocking(move || {
                store.scan_definition_assignments_by_kind(kind, cursor.as_ref(), value.limit)
            })
            .await
            .map_err(|error| Status::internal(format!("assignment scan task failed: {error}")))?
            .map_err(map_definition_state_error)
        })
        .await?;
    let has_more = page.next_cursor.is_some();
    Ok(Response::new(wire::DefinitionAssignmentScanPage {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        assignments: page
            .assignments
            .into_iter()
            .map(encode_assignment)
            .collect(),
        next_cursor: page
            .next_cursor
            .map_or_else(Vec::new, |cursor| cursor.as_bytes().to_vec()),
        has_more,
    }))
}

fn encode_locator_page(
    page: keldra_store::DefinitionLocatorPage,
) -> wire::DefinitionLocatorScanPage {
    let has_more = page.next_cursor.is_some();
    wire::DefinitionLocatorScanPage {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        locators: page.locators.into_iter().map(encode_locator).collect(),
        next_cursor: page
            .next_cursor
            .map_or_else(Vec::new, |cursor| cursor.as_bytes().to_vec()),
        has_more,
    }
}

fn encode_locator(locator: DefinitionLocator) -> wire::DefinitionLocatorRecord {
    let state = match locator.operation {
        DefinitionOperation::Upsert => wire::PrivateDefinitionObjectState::Live,
        DefinitionOperation::Delete => wire::PrivateDefinitionObjectState::Deleted,
    };
    wire::DefinitionLocatorRecord {
        kind: encode_kind(locator.kind) as i32,
        tenant_id: locator.tenant_id,
        bucket_id: locator.bucket_id,
        definition_id: locator.definition_id,
        definition_path: locator.path,
        object_version: locator.object_version.0,
        state: state as i32,
    }
}

fn encode_assignment(value: DefinitionAssignment) -> wire::DefinitionAssignmentMutation {
    wire::DefinitionAssignmentMutation {
        operation: wire::DefinitionAssignmentOperation::Upsert as i32,
        kind: encode_kind(value.kind) as i32,
        tenant_id: value.tenant_id,
        bucket_id: value.bucket_id,
        definition_id: value.definition_id,
        definition_path: value.definition_path,
        object_version: value.object_version.0,
        observed_fence_term: value.observed_fence.term,
        observed_fence_index: value.observed_fence.index,
        rank: u32::from(value.rank),
    }
}

fn decode_route(value: &wire::RoutedSourceJournalReadRequest) -> Result<JournalRoute, Status> {
    match wire::RoutedJournalKind::try_from(value.route) {
        Ok(wire::RoutedJournalKind::IndexDefinitions) => {
            Ok(JournalRoute::Definition(DefinitionKind::Index))
        }
        Ok(wire::RoutedJournalKind::AccountingDefinitions) => {
            Ok(JournalRoute::Definition(DefinitionKind::Accounting))
        }
        Ok(wire::RoutedJournalKind::Bucket) if value.tenant_id != 0 && value.bucket_id != 0 => {
            Ok(JournalRoute::Bucket {
                tenant_id: value.tenant_id,
                bucket_id: value.bucket_id,
            })
        }
        _ => Err(Status::invalid_argument(
            "routed source-journal route is invalid",
        )),
    }
}

fn decode_checkpoint(
    value: &wire::ApplyDefinitionAssignmentPageRequest,
) -> Result<DefinitionCheckpoint, Status> {
    let consumer_kind = decode_consumer_kind(value.consumer_kind)?;
    let source_id = source_id(value.source_node_id, &value.source_epoch)?;
    let checkpoint = DefinitionCheckpoint {
        consumer_kind,
        source_id,
        next_offset: value.next_offset,
        observed_fence: PlacementLogId {
            term: value.observed_fence_term,
            index: value.observed_fence_index,
        },
    };
    checkpoint
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(checkpoint)
}

fn decode_consumer_kind(value: i32) -> Result<DefinitionConsumerKind, Status> {
    match wire::PrivateDefinitionConsumerKind::try_from(value) {
        Ok(wire::PrivateDefinitionConsumerKind::IndexAssignments) => {
            Ok(DefinitionConsumerKind::IndexAssignments)
        }
        Ok(wire::PrivateDefinitionConsumerKind::AccountingAssignments) => {
            Ok(DefinitionConsumerKind::AccountingAssignments)
        }
        _ => Err(Status::invalid_argument(
            "definition consumer kind is invalid",
        )),
    }
}

fn decode_assignment_mutation(
    value: &wire::DefinitionAssignmentMutation,
) -> Result<DefinitionAssignmentMutation, Status> {
    let kind = match wire::PrivateDefinitionKind::try_from(value.kind) {
        Ok(wire::PrivateDefinitionKind::Index) => DefinitionKind::Index,
        Ok(wire::PrivateDefinitionKind::Accounting) => DefinitionKind::Accounting,
        _ => {
            return Err(Status::invalid_argument(
                "definition assignment kind is invalid",
            ));
        }
    };
    let fence = PlacementLogId {
        term: value.observed_fence_term,
        index: value.observed_fence_index,
    };
    let operation = match wire::DefinitionAssignmentOperation::try_from(value.operation) {
        Ok(wire::DefinitionAssignmentOperation::Upsert) => {
            let rank = u8::try_from(value.rank)
                .map_err(|_| Status::invalid_argument("definition assignment rank is invalid"))?;
            DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
                kind,
                tenant_id: value.tenant_id,
                bucket_id: value.bucket_id,
                definition_id: value.definition_id,
                definition_path: value.definition_path.clone(),
                object_version: VersionId(value.object_version),
                observed_fence: fence,
                rank,
            })
        }
        Ok(wire::DefinitionAssignmentOperation::Delete) => {
            let rank = u8::try_from(value.rank)
                .map_err(|_| Status::invalid_argument("definition deletion rank is invalid"))?;
            DefinitionAssignmentMutation::Delete(DefinitionDeletion {
                kind,
                tenant_id: value.tenant_id,
                bucket_id: value.bucket_id,
                definition_id: value.definition_id,
                definition_path: value.definition_path.clone(),
                object_version: VersionId(value.object_version),
                observed_fence: fence,
                rank,
            })
        }
        Ok(wire::DefinitionAssignmentOperation::Remove)
            if value.definition_path.is_empty() && value.rank == 0 =>
        {
            DefinitionAssignmentMutation::Remove {
                kind,
                tenant_id: value.tenant_id,
                bucket_id: value.bucket_id,
                definition_id: value.definition_id,
                object_version: VersionId(value.object_version),
                observed_fence: fence,
            }
        }
        _ => {
            return Err(Status::invalid_argument(
                "definition assignment operation is invalid",
            ));
        }
    };
    operation
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(operation)
}

fn decode_kind(value: i32) -> Result<DefinitionKind, Status> {
    match wire::PrivateDefinitionKind::try_from(value) {
        Ok(wire::PrivateDefinitionKind::Index) => Ok(DefinitionKind::Index),
        Ok(wire::PrivateDefinitionKind::Accounting) => Ok(DefinitionKind::Accounting),
        _ => Err(Status::invalid_argument("definition kind is invalid")),
    }
}

fn encode_kind(value: DefinitionKind) -> wire::PrivateDefinitionKind {
    match value {
        DefinitionKind::Index => wire::PrivateDefinitionKind::Index,
        DefinitionKind::Accounting => wire::PrivateDefinitionKind::Accounting,
    }
}

fn source_id(node_id: u64, epoch: &[u8]) -> Result<SourceId, Status> {
    let node_id = u16::try_from(node_id)
        .map_err(|_| Status::invalid_argument("source node ID is invalid"))?;
    let source_epoch: [u8; 32] = epoch
        .try_into()
        .map_err(|_| Status::invalid_argument("source epoch must contain 32 bytes"))?;
    Ok(SourceId {
        node_id,
        source_epoch,
    })
}

fn map_definition_state_error(error: DefinitionStateError) -> Status {
    match error {
        DefinitionStateError::CheckpointRegression
        | DefinitionStateError::ReconciliationFenceRegression
        | DefinitionStateError::AssignmentCheckpointMismatch => {
            Status::failed_precondition(error.to_string())
        }
        DefinitionStateError::Malformed(_) | DefinitionStateError::InvalidScanLimit => {
            Status::invalid_argument(error.to_string())
        }
        DefinitionStateError::InvalidCursor | DefinitionStateError::Storage(_) => {
            Status::internal(error.to_string())
        }
    }
}

fn map_routed_journal_error(error: RoutedJournalError) -> Status {
    match error {
        RoutedJournalError::InvalidLimits
        | RoutedJournalError::TargetBeforeCursor { .. }
        | RoutedJournalError::TargetFuture { .. } => Status::invalid_argument(error.to_string()),
        RoutedJournalError::SourceNodeMismatch | RoutedJournalError::SourceEpochMismatch => {
            Status::failed_precondition(error.to_string())
        }
        RoutedJournalError::CursorExpired { .. }
        | RoutedJournalError::CursorFuture { .. }
        | RoutedJournalError::MissingPrimary { .. }
        | RoutedJournalError::RouteMismatch { .. } => Status::out_of_range(error.to_string()),
        RoutedJournalError::Storage(_) => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    #[test]
    fn peer_wire_codes_distinguish_history_gaps_from_transient_storage() {
        for error in [
            RoutedJournalError::CursorExpired {
                cursor: 4,
                retention_floor: 5,
            },
            RoutedJournalError::CursorFuture { cursor: 8, tail: 7 },
            RoutedJournalError::MissingPrimary { offset: 6 },
            RoutedJournalError::RouteMismatch { offset: 6 },
        ] {
            assert_eq!(map_routed_journal_error(error).code(), Code::OutOfRange);
        }
        assert_eq!(
            map_routed_journal_error(RoutedJournalError::SourceEpochMismatch).code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            map_routed_journal_error(RoutedJournalError::Storage("temporary".into())).code(),
            Code::Internal
        );
    }
}
