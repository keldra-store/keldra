use super::*;
use crate::core_store::{CoreMutationBatchReceipt, CoreTransactionState};
use crate::mesh_control_stream::PreparedControlStreamAppend;

pub(super) struct LifecycleControlMutation<'a> {
    stream_family: &'a str,
    partition: String,
    record_key: String,
    operation: &'a str,
    expected_generation: Option<u64>,
    new_generation: u64,
    mesh_id: &'a str,
    payload_proto: Vec<u8>,
    writer: LifecycleControlWriter<'a>,
}

enum LifecycleControlWriter<'a> {
    Fenced(LifecycleControlWriteAuthority<'a>),
    Transaction { principal: &'a str },
}

struct PreparedTopologyMutation {
    batch: CoreMutationBatch,
    control_append: Option<PreparedControlStreamAppend>,
}

pub(super) fn fenced_control_mutation<'a, T>(
    stream_family: &'a str,
    record_key: String,
    operation: &'a str,
    expected_generation: Option<u64>,
    new_generation: u64,
    mesh_id: &'a str,
    payload: &T,
    authority: LifecycleControlWriteAuthority<'a>,
) -> LifecycleResult<LifecycleControlMutation<'a>>
where
    T: record_proto::LifecycleControlPayload,
{
    lifecycle_control_mutation(
        stream_family,
        record_key,
        operation,
        expected_generation,
        new_generation,
        mesh_id,
        payload,
        LifecycleControlWriter::Fenced(authority),
    )
}

pub(super) fn transactional_control_mutation<'a, T>(
    stream_family: &'a str,
    record_key: String,
    operation: &'a str,
    expected_generation: Option<u64>,
    new_generation: u64,
    mesh_id: &'a str,
    payload: &T,
    principal: &'a str,
) -> LifecycleResult<LifecycleControlMutation<'a>>
where
    T: record_proto::LifecycleControlPayload,
{
    lifecycle_control_mutation(
        stream_family,
        record_key,
        operation,
        expected_generation,
        new_generation,
        mesh_id,
        payload,
        LifecycleControlWriter::Transaction { principal },
    )
}

fn lifecycle_control_mutation<'a, T>(
    stream_family: &'a str,
    record_key: String,
    operation: &'a str,
    expected_generation: Option<u64>,
    new_generation: u64,
    mesh_id: &'a str,
    payload: &T,
    writer: LifecycleControlWriter<'a>,
) -> LifecycleResult<LifecycleControlMutation<'a>>
where
    T: record_proto::LifecycleControlPayload,
{
    require_identifier(stream_family, "control stream family")?;
    require_control_record_key(&record_key)?;
    require_identifier(mesh_id, "control mutation mesh id")?;
    require_nonempty(operation, "control mutation operation")?;
    let partition = lifecycle_control_partition(stream_family, &record_key);
    let payload_proto = record_proto::encode_lifecycle_control_payload(payload, stream_family)?;
    Ok(LifecycleControlMutation {
        stream_family,
        partition,
        record_key,
        operation,
        expected_generation,
        new_generation,
        mesh_id,
        payload_proto,
        writer,
    })
}

pub(super) async fn commit_topology_mutation(
    storage: &Storage,
    row: record_proto::EncodedLifecycleProjectionRow,
    control: Option<LifecycleControlMutation<'_>>,
) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let prepared = prepare_topology_mutation(storage, &store, row, control, None).await?;
    let receipt = store.commit_mutation_batch(prepared.batch).await?;
    ensure_committed(&receipt)?;
    if let Some(control) = prepared.control_append.as_ref() {
        crate::mesh_control_stream::finish_control_stream_append(storage, control, &receipt)
            .await
            .map_err(|error| LifecycleError::InvalidArgument(error.to_string()))?;
    }
    Ok(())
}

pub(super) async fn stage_topology_mutation_in_transaction(
    storage: &Storage,
    row: record_proto::EncodedLifecycleProjectionRow,
    control: LifecycleControlMutation<'_>,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let transaction = store
        .read_explicit_transaction_for_principal(transaction_id, principal)
        .await?;
    if transaction.root_anchor_key != LIFECYCLE_TOPOLOGY_ROOT_ANCHOR_KEY
        || transaction.scope_partition != LIFECYCLE_TOPOLOGY_ROOT_ANCHOR_KEY
    {
        return Err(LifecycleError::InvalidArgument(format!(
            "lifecycle topology transactions must use root anchor {LIFECYCLE_TOPOLOGY_ROOT_ANCHOR_KEY}"
        )));
    }
    let mutation_identity = topology_mutation_identity(&row, Some(&control), Some(&transaction));
    if transaction_contains_complete_topology_stage(&transaction, &row, &mutation_identity)? {
        return Ok(());
    }
    let prepared =
        prepare_topology_mutation(storage, &store, row, Some(control), Some(&transaction)).await?;
    let receipt = store
        .stage_explicit_transaction_batch(prepared.batch)
        .await?;
    if receipt.state != CoreTransactionState::Open {
        return Err(LifecycleError::InvalidArgument(
            "lifecycle topology transaction was not left open after staging".to_string(),
        ));
    }
    Ok(())
}

fn transaction_contains_complete_topology_stage(
    transaction: &CoreTransaction,
    row: &record_proto::EncodedLifecycleProjectionRow,
    mutation_identity: &str,
) -> LifecycleResult<bool> {
    let table_id = lifecycle_projection_table_id(row.kind)?;
    let tuple_key = lifecycle_projection_row_key(row.kind, &row.record_key)?;
    let projection_is_staged = transaction.visible_updates.iter().any(|update| {
        matches!(
            update,
            CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id: update_table_id,
                tuple_key: update_tuple_key,
                payload,
                ..
            } if cf == CF_MESH
                && *update_table_id == table_id
                && update_tuple_key == &tuple_key
                && payload == &row.payload
        )
    });
    if !projection_is_staged {
        return Ok(false);
    }

    let topology_head_is_staged = transaction.visible_updates.iter().any(|update| {
        let CoreTransactionUpdate::CoreMetaPut {
            cf,
            table_id,
            payload,
            ..
        } = update
        else {
            return false;
        };
        cf == CF_MESH
            && *table_id == TABLE_MESH_PARTITION_ROW
            && matches!(
                record_proto::decode_lifecycle_projection_row(payload),
                Ok(record_proto::LifecycleProjectionDescriptor::TopologyHead(_))
            )
    });
    let matching_control_records = transaction
        .visible_updates
        .iter()
        .filter(|update| {
            let CoreTransactionUpdate::StreamAppend { payload, .. } = update else {
                return false;
            };
            let Ok((frame, used)) = ControlStreamFrame::decode(payload) else {
                return false;
            };
            if used != payload.len() {
                return false;
            }
            crate::mesh_control_stream::decode_control_mutation_header(&frame.header_proto)
                .ok()
                .and_then(|header| header.idempotency_key)
                .as_deref()
                == Some(mutation_identity)
        })
        .count();
    if topology_head_is_staged && matching_control_records == 2 {
        return Ok(true);
    }
    Err(LifecycleError::InvalidArgument(
        "explicit lifecycle transaction contains a partial topology mutation stage".to_string(),
    ))
}

async fn prepare_topology_mutation(
    storage: &Storage,
    store: &CoreStore,
    row: record_proto::EncodedLifecycleProjectionRow,
    control: Option<LifecycleControlMutation<'_>>,
    transaction: Option<&CoreTransaction>,
) -> LifecycleResult<PreparedTopologyMutation> {
    if !matches!(
        row.kind,
        record_proto::LIFECYCLE_PROJECTION_REGION_KIND
            | record_proto::LIFECYCLE_PROJECTION_CELL_KIND
            | record_proto::LIFECYCLE_PROJECTION_NODE_KIND
    ) {
        return Err(LifecycleError::InvalidArgument(format!(
            "{} is not a topology descriptor projection",
            row.kind
        )));
    }

    let mutation_created_at = topology_mutation_created_at(&row)?;
    let activated_at_unix_nanos = topology_mutation_timestamp_nanos(&mutation_created_at)?;
    let mutation_identity = topology_mutation_identity(&row, control.as_ref(), transaction);

    let mut state = topology_state_visible_to_transaction(store, transaction)?;
    match state.topology_head.as_ref() {
        Some(head) => {
            record_proto::validate_topology_head(head)?;
            if head.mesh_id != topology_activation::canonical_mesh_id(&state)? {
                return Err(LifecycleError::InvalidArgument(
                    "lifecycle topology head mesh does not match the committed topology"
                        .to_string(),
                ));
            }
            if head.topology_hash != topology_activation::topology_state_hash(&state)? {
                return Err(LifecycleError::InvalidArgument(
                    "lifecycle topology head does not describe the committed topology".to_string(),
                ));
            }
            if let Some(activation) = state.canonical_topology_activation.as_ref() {
                topology_activation::validate_activation_against_topology_head(
                    &state, activation, head,
                )?;
            }
        }
        None if !state.regions.is_empty()
            || !state.cells.is_empty()
            || !state.nodes.is_empty()
            || state.canonical_topology_activation.is_some() =>
        {
            return Err(LifecycleError::InvalidArgument(
                "lifecycle topology exists without its durable topology head".to_string(),
            ));
        }
        None => {}
    }
    let existing_activation = state.canonical_topology_activation.clone();
    let pre_activation_head = state.topology_head.clone();
    apply_lifecycle_projection_row(
        &mut state,
        lifecycle_projection_table_id(row.kind)?,
        &row.payload,
    )?;

    if let Some(existing) = existing_activation.as_ref() {
        topology_activation::validate_canonical_topology_activation_for_state(&state, existing)?;
        state.canonical_topology_activation = Some(existing.clone());
    } else {
        state.canonical_topology_activation =
            topology_activation::build_canonical_topology_activation(
                &state,
                pre_activation_head.as_ref(),
                activated_at_unix_nanos,
            )?;
    }
    ensure_canonical_topology_activation_is_preserved(
        existing_activation.as_ref(),
        state.canonical_topology_activation.as_ref(),
    )?;

    let mesh_id = topology_activation::canonical_mesh_id(&state)?;
    let next_generation = state.topology_head.as_ref().map_or(Ok(1), |head| {
        head.generation.checked_add(1).ok_or_else(|| {
            LifecycleError::InvalidArgument(
                "lifecycle topology head generation overflow".to_string(),
            )
        })
    })?;
    let next_head = LifecycleTopologyHead {
        schema: LIFECYCLE_TOPOLOGY_HEAD_SCHEMA.to_string(),
        mesh_id: mesh_id.clone(),
        topology_hash: topology_activation::topology_state_hash(&state)?,
        generation: next_generation,
    };
    record_proto::validate_topology_head(&next_head)?;
    if let Some(activation) = state.canonical_topology_activation.as_ref() {
        topology_activation::validate_activation_against_topology_head(
            &state, activation, &next_head,
        )?;
    }

    let mut rows = vec![row];
    if existing_activation.is_none()
        && let Some(activation) = state.canonical_topology_activation.as_ref()
    {
        rows.push(record_proto::encode_topology_activation_projection_row(
            activation,
        )?);
    }
    rows.push(record_proto::encode_topology_head_projection_row(
        &next_head,
    )?);

    let mut preconditions = Vec::new();
    let mut operations = Vec::new();
    for row in rows {
        let table_id = lifecycle_projection_table_id(row.kind)?;
        let tuple_key = lifecycle_projection_row_key(row.kind, &row.record_key)?;
        let current =
            lifecycle_payload_visible_to_transaction(store, transaction, table_id, &tuple_key)?;
        preconditions.push(CoreMutationPrecondition::CoreMetaRow {
            cf: CF_MESH.to_string(),
            table_id,
            tuple_key: tuple_key.clone(),
            expected_payload_hash: current
                .as_ref()
                .map(|payload| core_meta_payload_digest(table_id, payload)),
            require_absent: current.is_none(),
            require_present: current.is_some(),
        });
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: LIFECYCLE_TOPOLOGY_ROOT_ANCHOR_KEY.to_string(),
            cf: CF_MESH.to_string(),
            table_id,
            tuple_key,
            payload: row.payload,
        });
    }

    let (control_append, committed_by_principal) = if let Some(control) = control {
        let (partition_precondition, writer_node_id, writer_fence, principal) = match control.writer
        {
            LifecycleControlWriter::Fenced(authority) => {
                validate_control_authority(&control, authority)?;
                let partition_precondition = partition_fence::partition_write_precondition(
                    storage,
                    authority.permit,
                    authority.signing_key,
                )
                .await
                .map_err(|rejection| {
                    LifecycleError::InvalidArgument(format!(
                        "lifecycle control write fence rejected for {}/{}: {}: {}",
                        control.stream_family,
                        control.partition,
                        rejection.code.as_str(),
                        rejection.reason
                    ))
                })?;
                (
                    Some(partition_precondition),
                    authority.permit.owner_node_id.as_str(),
                    authority.permit.fence_token,
                    format!("partition-owner:{}", authority.permit.owner_node_id),
                )
            }
            LifecycleControlWriter::Transaction { principal } => {
                (None, principal, 0, principal.to_string())
            }
        };
        let cursor = if let Some(transaction) = transaction {
            crate::mesh_control_stream::control_stream_append_cursor_visible_to_transaction(
                storage,
                control.stream_family,
                &control.partition,
                transaction,
            )
            .await
        } else {
            crate::mesh_control_stream::control_stream_append_cursor(
                storage,
                control.stream_family,
                &control.partition,
            )
            .await
        }
        .map_err(|error| LifecycleError::InvalidArgument(error.to_string()))?;
        let digest = ControlRecordDigest::blake3(&control.payload_proto);
        let frame = ControlStreamFrame::new(
            crate::mesh_control_stream::encode_control_mutation_header(
                ControlMutationHeaderInput {
                    schema: CONTROL_MUTATION_SCHEMA,
                    mesh_id: control.mesh_id,
                    stream_family: control.stream_family,
                    partition: &control.partition,
                    sequence: cursor.sequence,
                    record_key: &control.record_key,
                    operation: control.operation,
                    expected_generation: control.expected_generation,
                    new_generation: control.new_generation,
                    writer_node_id,
                    writer_fence,
                    idempotency_key: Some(&mutation_identity),
                    record_digest: &digest,
                    created_at: &mutation_created_at,
                    byte_offset: cursor.byte_offset,
                },
            ),
            control.payload_proto,
        );
        let prepared = crate::mesh_control_stream::prepare_control_stream_append(
            storage,
            control.stream_family,
            &control.partition,
            &frame,
            partition_precondition,
            transaction,
            LIFECYCLE_TOPOLOGY_ROOT_ANCHOR_KEY,
        )
        .await
        .map_err(|error| LifecycleError::InvalidArgument(error.to_string()))?;
        preconditions.extend(prepared.preconditions.iter().cloned());
        operations.extend(prepared.operations.iter().cloned());
        (Some(prepared), principal)
    } else {
        (None, "mesh-lifecycle-internal".to_string())
    };

    let batch_transaction_id = transaction.map_or_else(
        || mutation_identity.clone(),
        |transaction| transaction.transaction_id.clone(),
    );
    Ok(PreparedTopologyMutation {
        batch: CoreMutationBatch {
            transaction_id: batch_transaction_id,
            scope_partition: LIFECYCLE_TOPOLOGY_ROOT_ANCHOR_KEY.to_string(),
            committed_by_principal,
            root_publications: vec![CoreMutationRootPublication {
                root_anchor_key: LIFECYCLE_TOPOLOGY_ROOT_ANCHOR_KEY.to_string(),
                writer_families: vec![
                    WriterFamily::CoreControl.as_str().to_string(),
                    WriterFamily::MeshControl.as_str().to_string(),
                ],
                transaction_coordinator: true,
            }],
            preconditions,
            operations,
        },
        control_append,
    })
}

fn topology_mutation_created_at(
    row: &record_proto::EncodedLifecycleProjectionRow,
) -> LifecycleResult<String> {
    let updated_at = match record_proto::decode_lifecycle_projection_row(&row.payload)? {
        record_proto::LifecycleProjectionDescriptor::Region(descriptor) => descriptor.updated_at,
        record_proto::LifecycleProjectionDescriptor::Cell(descriptor) => descriptor.updated_at,
        record_proto::LifecycleProjectionDescriptor::Node(descriptor) => descriptor.updated_at,
        _ => {
            return Err(LifecycleError::InvalidArgument(
                "topology mutation timestamp requires a region, cell, or node descriptor"
                    .to_string(),
            ));
        }
    };
    let parsed = chrono::DateTime::parse_from_rfc3339(&updated_at).map_err(|error| {
        LifecycleError::InvalidArgument(format!(
            "topology descriptor updated_at is not RFC3339: {error}"
        ))
    })?;
    Ok(parsed
        .with_timezone(&chrono::Utc)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

fn topology_mutation_timestamp_nanos(created_at: &str) -> LifecycleResult<u64> {
    chrono::DateTime::parse_from_rfc3339(created_at)
        .map_err(|error| {
            LifecycleError::InvalidArgument(format!(
                "topology mutation timestamp is not RFC3339: {error}"
            ))
        })?
        .timestamp_nanos_opt()
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| {
            LifecycleError::InvalidArgument(
                "canonical topology activation time is outside the supported range".to_string(),
            )
        })
}

fn topology_mutation_identity(
    row: &record_proto::EncodedLifecycleProjectionRow,
    control: Option<&LifecycleControlMutation<'_>>,
    transaction: Option<&CoreTransaction>,
) -> String {
    let mut bytes = Vec::new();
    append_identity_part(&mut bytes, b"anvil.mesh.lifecycle_topology_mutation.v1");
    append_identity_part(&mut bytes, row.kind.as_bytes());
    append_identity_part(&mut bytes, row.record_key.as_bytes());
    append_identity_part(&mut bytes, &row.payload);
    if let Some(transaction) = transaction {
        append_identity_part(&mut bytes, transaction.transaction_id.as_bytes());
    }
    if let Some(control) = control {
        append_identity_part(&mut bytes, control.stream_family.as_bytes());
        append_identity_part(&mut bytes, control.partition.as_bytes());
        append_identity_part(&mut bytes, control.record_key.as_bytes());
        append_identity_part(&mut bytes, control.operation.as_bytes());
        append_identity_part(
            &mut bytes,
            &control
                .expected_generation
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        append_identity_part(&mut bytes, &control.new_generation.to_le_bytes());
        append_identity_part(&mut bytes, control.mesh_id.as_bytes());
        append_identity_part(&mut bytes, &control.payload_proto);
        match &control.writer {
            LifecycleControlWriter::Fenced(authority) => {
                append_identity_part(&mut bytes, authority.permit.owner_node_id.as_bytes());
                append_identity_part(&mut bytes, &authority.permit.fence_token.to_le_bytes());
            }
            LifecycleControlWriter::Transaction { principal } => {
                append_identity_part(&mut bytes, principal.as_bytes());
            }
        }
    }
    format!(
        "mesh-lifecycle-topology:{}",
        crate::core_store::sha256_hex(&bytes)
    )
}

fn append_identity_part(bytes: &mut Vec<u8>, part: &[u8]) {
    bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
    bytes.extend_from_slice(part);
}

fn topology_state_visible_to_transaction(
    store: &CoreStore,
    transaction: Option<&CoreTransaction>,
) -> LifecycleResult<MeshLifecycleState> {
    let mut state = read_lifecycle_state_projection_with_core_store(store)?;
    let Some(transaction) = transaction else {
        return Ok(state);
    };
    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::CoreMetaPut {
            cf,
            table_id,
            payload,
            ..
        } = update
        else {
            continue;
        };
        if cf == CF_MESH && is_lifecycle_projection_table(*table_id) {
            apply_lifecycle_projection_row(&mut state, *table_id, payload)?;
        }
    }
    Ok(state)
}

fn lifecycle_payload_visible_to_transaction(
    store: &CoreStore,
    transaction: Option<&CoreTransaction>,
    table_id: u16,
    tuple_key: &[u8],
) -> LifecycleResult<Option<Vec<u8>>> {
    if let Some(transaction) = transaction {
        for update in transaction.visible_updates.iter().rev() {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id: update_table_id,
                    tuple_key: update_tuple_key,
                    payload,
                    ..
                } if cf == CF_MESH
                    && *update_table_id == table_id
                    && update_tuple_key == tuple_key =>
                {
                    return Ok(Some(payload.clone()));
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id: update_table_id,
                    tuple_key: update_tuple_key,
                    ..
                } if cf == CF_MESH
                    && *update_table_id == table_id
                    && update_tuple_key == tuple_key =>
                {
                    return Ok(None);
                }
                _ => {}
            }
        }
    }
    Ok(store.read_coremeta_row(CF_MESH, table_id, tuple_key)?)
}

fn validate_control_authority(
    control: &LifecycleControlMutation<'_>,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<()> {
    let expected_partition_id =
        mesh_directory::control_partition_id(control.stream_family, &control.partition);
    if authority.permit.partition_family != mesh_directory::CONTROL_PARTITION_FAMILY {
        return Err(LifecycleError::InvalidArgument(format!(
            "invalid lifecycle control write permit: expected partition family {}, got {}",
            mesh_directory::CONTROL_PARTITION_FAMILY,
            authority.permit.partition_family
        )));
    }
    if authority.permit.partition_id != expected_partition_id {
        return Err(LifecycleError::InvalidArgument(
            "invalid lifecycle control write permit: partition id does not match stream"
                .to_string(),
        ));
    }
    Ok(())
}

fn ensure_committed(receipt: &CoreMutationBatchReceipt) -> LifecycleResult<()> {
    if receipt.state == CoreTransactionState::Committed {
        return Ok(());
    }
    Err(LifecycleError::InvalidArgument(format!(
        "lifecycle topology mutation did not commit: {}",
        receipt
            .finalisation_error
            .as_deref()
            .unwrap_or("unknown finalisation failure")
    )))
}

#[cfg(test)]
pub(super) async fn prepare_topology_batch_for_test(
    storage: &Storage,
    row: record_proto::EncodedLifecycleProjectionRow,
) -> LifecycleResult<CoreMutationBatch> {
    let store = CoreStore::new(storage.clone()).await?;
    Ok(prepare_topology_mutation(storage, &store, row, None, None)
        .await?
        .batch)
}

#[cfg(test)]
pub(super) async fn commit_topology_mutation_with_failure_injection_for_test(
    storage: &Storage,
    row: record_proto::EncodedLifecycleProjectionRow,
    control: LifecycleControlMutation<'_>,
) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut prepared = prepare_topology_mutation(storage, &store, row, Some(control), None).await?;
    let state = read_lifecycle_state_projection_with_core_store(&store)?;
    let head = state.topology_head.ok_or_else(|| {
        LifecycleError::InvalidArgument(
            "failure injection requires an existing topology head".to_string(),
        )
    })?;
    prepared
        .batch
        .preconditions
        .push(CoreMutationPrecondition::CoreMetaRow {
            cf: CF_MESH.to_string(),
            table_id: TABLE_MESH_PARTITION_ROW,
            tuple_key: lifecycle_projection_row_key(
                record_proto::LIFECYCLE_PROJECTION_TOPOLOGY_HEAD_KIND,
                &head.mesh_id,
            )?,
            expected_payload_hash: None,
            require_absent: true,
            require_present: false,
        });
    let receipt = store.commit_mutation_batch(prepared.batch).await?;
    ensure_committed(&receipt)
}
