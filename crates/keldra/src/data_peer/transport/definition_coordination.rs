//! Wire validation for sparse definition coordination transport.

use super::*;
use keldra_store::{
    DefinitionAssignment, DefinitionLocator, DefinitionOperation, PlacementLogId, VersionId,
};

pub(super) fn decode_routed_source_journal_page(
    response: wire::RoutedSourceJournalPage,
    expected_source: SourceId,
    after_offset: u64,
    target_offset: u64,
    max_bytes: u64,
) -> Result<RoutedLocalChangePage, Status> {
    require_response_schema(response.schema_version)?;
    require_source_page_limit(max_bytes)?;
    let node_id = u16::try_from(response.source_node_id)
        .map_err(|_| Status::data_loss("routed journal source node ID is invalid"))?;
    let source_epoch: [u8; 32] = response
        .source_epoch
        .as_slice()
        .try_into()
        .map_err(|_| Status::data_loss("routed journal source epoch is invalid"))?;
    let source_id = SourceId {
        node_id,
        source_epoch,
    };
    if source_id != expected_source
        || response.through_offset < after_offset
        || response.through_offset > target_offset
    {
        return Err(Status::data_loss(
            "routed journal page identity or advancement is invalid",
        ));
    }
    let actual_bytes = response
        .changes_json
        .iter()
        .try_fold(0_u64, |total, encoded| {
            total.checked_add(encoded.len() as u64)
        })
        .ok_or_else(|| Status::data_loss("routed journal byte count overflow"))?;
    if actual_bytes != response.encoded_bytes || actual_bytes > max_bytes {
        return Err(Status::data_loss(
            "routed journal page exceeded or misstated its byte bound",
        ));
    }
    let oversize = match (response.oversize_offset, response.oversize_encoded_bytes) {
        (0, 0) => None,
        (offset, bytes)
            if offset > after_offset
                && offset <= target_offset
                && bytes > max_bytes
                && response.changes_json.is_empty()
                && response.encoded_bytes == 0 =>
        {
            Some(OversizeLocalChange {
                offset,
                encoded_bytes: bytes,
            })
        }
        _ => {
            return Err(Status::data_loss(
                "routed journal oversize-record evidence is malformed",
            ));
        }
    };
    let changes = response
        .changes_json
        .iter()
        .map(|encoded| decode_typed(encoded))
        .collect::<Result<Vec<LocalChange>, _>>()?;
    let mut previous = after_offset;
    for change in &changes {
        let offset = change.offset();
        if offset <= previous || offset > response.through_offset {
            return Err(Status::data_loss(
                "routed journal page offsets are not strictly increasing",
            ));
        }
        previous = offset;
    }
    Ok(RoutedLocalChangePage {
        source_id,
        changes,
        encoded_bytes: response.encoded_bytes,
        through_offset: response.through_offset,
        oversize,
    })
}

pub(super) fn encode_assignment_mutation(
    mutation: &DefinitionAssignmentMutation,
) -> Result<wire::DefinitionAssignmentMutation, Status> {
    mutation
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let (operation, kind, tenant_id, bucket_id, definition_id, path, version, fence, rank) =
        match mutation {
            DefinitionAssignmentMutation::Upsert(value) => (
                wire::DefinitionAssignmentOperation::Upsert,
                value.kind,
                value.tenant_id,
                value.bucket_id,
                value.definition_id,
                value.definition_path.clone(),
                value.object_version,
                value.observed_fence,
                u32::from(value.rank),
            ),
            DefinitionAssignmentMutation::Delete(value) => (
                wire::DefinitionAssignmentOperation::Delete,
                value.kind,
                value.tenant_id,
                value.bucket_id,
                value.definition_id,
                value.definition_path.clone(),
                value.object_version,
                value.observed_fence,
                u32::from(value.rank),
            ),
            DefinitionAssignmentMutation::Remove {
                kind,
                tenant_id,
                bucket_id,
                definition_id,
                object_version,
                observed_fence,
            } => (
                wire::DefinitionAssignmentOperation::Remove,
                *kind,
                *tenant_id,
                *bucket_id,
                *definition_id,
                String::new(),
                *object_version,
                *observed_fence,
                0,
            ),
        };
    Ok(wire::DefinitionAssignmentMutation {
        operation: operation as i32,
        kind: encode_wire_definition_kind(kind) as i32,
        tenant_id,
        bucket_id,
        definition_id,
        definition_path: path,
        object_version: version.0,
        observed_fence_term: fence.term,
        observed_fence_index: fence.index,
        rank,
    })
}

pub(super) fn wire_consumer_kind(
    value: DefinitionConsumerKind,
) -> Result<wire::PrivateDefinitionConsumerKind, Status> {
    match value {
        DefinitionConsumerKind::IndexAssignments => {
            Ok(wire::PrivateDefinitionConsumerKind::IndexAssignments)
        }
        DefinitionConsumerKind::AccountingAssignments => {
            Ok(wire::PrivateDefinitionConsumerKind::AccountingAssignments)
        }
        DefinitionConsumerKind::V6IndexCatalog
        | DefinitionConsumerKind::AccountingDelivery
        | DefinitionConsumerKind::IndexRetention
        | DefinitionConsumerKind::AccountingRetention => Err(Status::invalid_argument(
            "source-local delivery checkpoints cannot cross the peer assignment API",
        )),
    }
}

pub(super) fn encode_wire_definition_kind(value: DefinitionKind) -> wire::PrivateDefinitionKind {
    match value {
        DefinitionKind::Index => wire::PrivateDefinitionKind::Index,
        DefinitionKind::Accounting => wire::PrivateDefinitionKind::Accounting,
    }
}

fn decode_wire_definition_kind(value: i32) -> Result<DefinitionKind, Status> {
    match wire::PrivateDefinitionKind::try_from(value) {
        Ok(wire::PrivateDefinitionKind::Index) => Ok(DefinitionKind::Index),
        Ok(wire::PrivateDefinitionKind::Accounting) => Ok(DefinitionKind::Accounting),
        _ => Err(Status::data_loss(
            "definition locator response carries an invalid kind",
        )),
    }
}

pub(super) fn decode_locator_page(
    response: wire::DefinitionLocatorScanPage,
    expected_kind: DefinitionKind,
    expected_bucket: Option<(u64, u64)>,
) -> Result<DefinitionLocatorPage, Status> {
    require_response_schema(response.schema_version)?;
    let locators = response
        .locators
        .into_iter()
        .map(|value| {
            let locator = DefinitionLocator {
                kind: decode_wire_definition_kind(value.kind)?,
                tenant_id: value.tenant_id,
                bucket_id: value.bucket_id,
                definition_id: value.definition_id,
                path: value.definition_path,
                object_version: VersionId(value.object_version),
                operation: decode_locator_state(value.state)?,
            };
            locator
                .validate()
                .map_err(|error| Status::data_loss(error.to_string()))?;
            if locator.kind != expected_kind
                || expected_bucket.is_some_and(|(tenant_id, bucket_id)| {
                    locator.tenant_id != tenant_id || locator.bucket_id != bucket_id
                })
            {
                return Err(Status::data_loss(
                    "definition locator response escaped its requested scope",
                ));
            }
            Ok(locator)
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let next_cursor = if response.has_more {
        if response.next_cursor.is_empty() {
            return Err(Status::data_loss(
                "definition locator response omitted its continuation",
            ));
        }
        Some(
            DefinitionLocatorCursor::from_bytes(response.next_cursor)
                .map_err(|error| Status::data_loss(error.to_string()))?,
        )
    } else {
        if !response.next_cursor.is_empty() {
            return Err(Status::data_loss(
                "terminal definition locator response carries a continuation",
            ));
        }
        None
    };
    Ok(DefinitionLocatorPage {
        locators,
        next_cursor,
    })
}

fn decode_locator_state(value: i32) -> Result<DefinitionOperation, Status> {
    match wire::PrivateDefinitionObjectState::try_from(value) {
        Ok(wire::PrivateDefinitionObjectState::Live) => Ok(DefinitionOperation::Upsert),
        Ok(wire::PrivateDefinitionObjectState::Deleted) => Ok(DefinitionOperation::Delete),
        _ => Err(Status::data_loss(
            "definition locator response carries an invalid object state",
        )),
    }
}

pub(super) fn decode_assignment_upsert(
    value: &wire::DefinitionAssignmentMutation,
) -> Result<DefinitionAssignment, Status> {
    if wire::DefinitionAssignmentOperation::try_from(value.operation)
        != Ok(wire::DefinitionAssignmentOperation::Upsert)
    {
        return Err(Status::data_loss(
            "definition assignment scan returned a non-assignment record",
        ));
    }
    let assignment = DefinitionAssignment {
        kind: decode_wire_definition_kind(value.kind)?,
        tenant_id: value.tenant_id,
        bucket_id: value.bucket_id,
        definition_id: value.definition_id,
        definition_path: value.definition_path.clone(),
        object_version: VersionId(value.object_version),
        observed_fence: PlacementLogId {
            term: value.observed_fence_term,
            index: value.observed_fence_index,
        },
        rank: u8::try_from(value.rank)
            .map_err(|_| Status::data_loss("definition assignment rank is invalid"))?,
    };
    assignment
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    Ok(assignment)
}

pub(super) fn decode_assignment_cursor(
    has_more: bool,
    encoded: Vec<u8>,
) -> Result<Option<DefinitionAssignmentCursor>, Status> {
    match (has_more, encoded.is_empty()) {
        (true, false) => DefinitionAssignmentCursor::from_bytes(encoded)
            .map(Some)
            .map_err(|error| Status::data_loss(error.to_string())),
        (false, true) => Ok(None),
        _ => Err(Status::data_loss(
            "definition assignment scan continuation is inconsistent",
        )),
    }
}
