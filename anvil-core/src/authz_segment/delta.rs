use super::*;
use crate::writer_segment_catalog::page_writer_segment_catalog_records;

pub(crate) async fn write_authz_tuple_delta_segment(
    storage: &Storage,
    tenant_id: i64,
    previous: &DecodedAuthzSegment,
    mutations: &[AuthzTupleRecord],
    target_revision: u64,
    source_stream_cursor: u64,
    source_fence_token: u64,
) -> Result<String> {
    let staged = stage_authz_tuple_delta_segment(
        storage,
        tenant_id,
        previous,
        mutations,
        target_revision,
        source_stream_cursor,
        source_fence_token,
    )
    .await?;
    publish_staged_authz_tuple_segment(storage, staged, &[]).await
}

pub(crate) async fn stage_authz_tuple_delta_segment(
    storage: &Storage,
    tenant_id: i64,
    previous: &DecodedAuthzSegment,
    mutations: &[AuthzTupleRecord],
    target_revision: u64,
    source_stream_cursor: u64,
    source_fence_token: u64,
) -> Result<StagedAuthzTupleSegment> {
    if target_revision == 0
        || previous.header.generation.saturating_add(1) != target_revision
        || source_stream_cursor < previous.header.source_stream_cursor
    {
        bail!("authorization delta does not advance its materialized predecessor");
    }
    let current_active =
        apply_active_tuple_mutations(tenant_id, &previous.records, mutations, target_revision)?;
    let segment_records = segment_records_from_authz_records(mutations)?;

    let head = crate::authz_head::read(storage, tenant_id).await?.head;
    let schema_changed = head.schema_revision == target_revision;
    if head.schema_revision > previous.header.generation && head.schema_revision < target_revision {
        bail!("AuthzRevisionUnavailable: missing materialized schema revision");
    }
    let (schema_rows, relation_rule_rows) = if schema_changed {
        let schema_rows =
            materialized_state::schema_descriptor_rows(storage, tenant_id, &current_active).await?;
        let bound =
            materialized_state::bound_relation_rule_rows(storage, tenant_id, &current_active)
                .await?;
        let relation_rows =
            materialized_state::all_relation_rule_rows(storage, tenant_id, &bound).await?;
        (schema_rows, relation_rows)
    } else {
        (
            previous.schema_descriptors.clone(),
            previous.relation_rules.clone(),
        )
    };
    let schema_replacement = schema_rows != previous.schema_descriptors;
    let relation_rule_replacement = relation_rule_rows != previous.relation_rules;

    let current_view = tuple_view_from_active_records(&current_active);
    let derived_usersets = materialized_state::derived_userset_entries(
        &current_active,
        &schema_rows,
        &relation_rule_rows,
        &current_view,
    )?;
    let current_userset_edges = canonical_rows_by_key(
        materialized_state::userset_edge_rows(&current_active, &derived_usersets, target_revision)?,
        userset_edge_key,
    )?;
    let current_list_objects = canonical_rows_by_key(
        materialized_state::list_object_rows(
            &current_active,
            &derived_usersets,
            &schema_rows,
            &relation_rule_rows,
            &current_view,
            target_revision,
        )?,
        list_object_key,
    )?;
    let current_list_subjects = canonical_rows_by_key(
        materialized_state::list_subject_rows(
            &current_active,
            &derived_usersets,
            &schema_rows,
            &relation_rule_rows,
            &current_view,
            target_revision,
        )?,
        list_subject_key,
    )?;
    let derived_userset_count = current_userset_edges
        .iter()
        .filter(|row| row.source == "derived_userset")
        .count() as u64;
    let list_objects_count = current_list_objects.len() as u64;
    let list_subjects_count = current_list_subjects.len() as u64;

    let userset_edge_deltas = userset_edge_delta_rows(
        previous.userset_edges.clone(),
        current_userset_edges,
        target_revision,
    )?;
    let list_object_deltas = list_object_delta_rows(
        previous.list_objects.clone(),
        current_list_objects,
        target_revision,
    )?;
    let list_subject_deltas = list_subject_delta_rows(
        previous.list_subjects.clone(),
        current_list_subjects,
        target_revision,
    )?;
    let checkpoint_rows = vec![AuthzRevisionCheckpointRow {
        tenant_id,
        revision: target_revision,
        source_fence_token,
        tuple_record_count: current_active.len() as u64,
        active_tuple_count: current_active.len() as u64,
        derived_userset_count,
        list_objects_count,
        list_subjects_count,
        tuple_records_hash: hex::encode(tuple_records_hash(&current_active)?),
    }];
    let segment_tables = vec![
        WriterBodyTable {
            table_id: TABLE_AUTHZ_SCHEMA_DESCRIPTOR,
            row_type_id: TABLE_AUTHZ_SCHEMA_DESCRIPTOR,
            rows: if schema_replacement {
                table_rows_from(
                    schema_rows,
                    schema_descriptor_key,
                    encode_schema_descriptor_row,
                )?
            } else {
                Vec::new()
            },
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_TUPLE,
            row_type_id: TABLE_AUTHZ_TUPLE,
            rows: segment_records
                .iter()
                .map(|record| TableRow {
                    key: record.key.clone(),
                    value: record.value.clone(),
                })
                .collect(),
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_RELATION_RULE,
            row_type_id: TABLE_AUTHZ_RELATION_RULE,
            rows: if relation_rule_replacement {
                table_rows_from(
                    relation_rule_rows,
                    relation_rule_key,
                    encode_relation_rule_row,
                )?
            } else {
                Vec::new()
            },
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_USERSET_EDGE,
            row_type_id: TABLE_AUTHZ_USERSET_EDGE,
            rows: table_rows_from(
                userset_edge_deltas,
                userset_edge_key,
                encode_userset_edge_row,
            )?,
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_CAVEAT_DESCRIPTOR,
            row_type_id: TABLE_AUTHZ_CAVEAT_DESCRIPTOR,
            rows: Vec::new(),
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_REVISION_LOG,
            row_type_id: TABLE_AUTHZ_REVISION_LOG,
            rows: table_rows_from(
                checkpoint_rows,
                revision_checkpoint_key,
                encode_revision_checkpoint_row,
            )?,
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_LIST_OBJECTS,
            row_type_id: TABLE_AUTHZ_LIST_OBJECTS,
            rows: table_rows_from(list_object_deltas, list_object_key, encode_list_objects_row)?,
        },
        WriterBodyTable {
            table_id: TABLE_AUTHZ_LIST_SUBJECTS,
            row_type_id: TABLE_AUTHZ_LIST_SUBJECTS,
            rows: table_rows_from(
                list_subject_deltas,
                list_subject_key,
                encode_list_subjects_row,
            )?,
        },
    ];
    stage_authz_tuple_segment_tables(
        storage,
        tenant_id,
        target_revision - 1,
        target_revision,
        "delta",
        schema_replacement,
        relation_rule_replacement,
        source_stream_cursor,
        source_fence_token,
        &segment_records,
        segment_tables,
    )
    .await
}

pub(crate) async fn read_authz_tuple_segment_at_revision(
    storage: &Storage,
    tenant_id: i64,
    revision: u64,
) -> Result<Option<DecodedAuthzSegment>> {
    if revision == 0 {
        return Ok(None);
    }
    let checkpoint_generation = if revision % AUTHZ_DELTA_CHECKPOINT_INTERVAL == 0 {
        revision
    } else {
        (revision / AUTHZ_DELTA_CHECKPOINT_INTERVAL) * AUTHZ_DELTA_CHECKPOINT_INTERVAL
    };
    let scope = authz_tuple_segment_scope(tenant_id)?;
    let page = page_writer_segment_catalog_records(
        storage,
        AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY,
        &scope,
        checkpoint_generation.saturating_sub(1),
        revision,
        usize::try_from(AUTHZ_DELTA_CHECKPOINT_INTERVAL)?,
    )
    .await?;
    if page.next_generation.is_some() {
        bail!("AuthzRevisionUnavailable: authorization segment window exceeds checkpoint bound");
    }

    let mut active_records = BTreeMap::<TupleIdentity, AuthzTupleRecord>::new();
    let mut schema_descriptors = Vec::new();
    let mut relation_rules = Vec::new();
    let mut userset_edges = BTreeMap::<Vec<u8>, AuthzUsersetEdgeRow>::new();
    let mut list_objects = BTreeMap::<Vec<u8>, AuthzListObjectsRow>::new();
    let mut list_subjects = BTreeMap::<Vec<u8>, AuthzListSubjectsRow>::new();
    let mut revision_checkpoints = Vec::new();
    let mut final_header = None;
    let mut checkpoint_seen = false;
    let mut last_generation = 0_u64;
    let mut source_stream_cursor = 0_u64;

    for record in page.records {
        let Some(segment) = read_authz_tuple_segment_ref(
            storage,
            tenant_id,
            record.generation,
            &record.segment_ref,
        )
        .await?
        else {
            bail!("AuthzCandidateSetStale");
        };
        if segment.header.generation != record.generation
            || segment.header.source_stream_cursor != record.source_cursor
        {
            bail!("AuthzCandidateSetStale");
        }
        match segment.header.segment_kind.as_str() {
            "checkpoint" => {
                if segment.header.base_revision != 0
                    || !segment.header.schema_replacement
                    || !segment.header.relation_rule_replacement
                    || (checkpoint_seen
                        && (segment.header.generation <= last_generation
                            || segment.header.source_stream_cursor < source_stream_cursor))
                {
                    bail!("AuthzCandidateSetStale");
                }
                checkpoint_seen = true;
                active_records.clear();
                userset_edges.clear();
                list_objects.clear();
                list_subjects.clear();
                revision_checkpoints.clear();
                for active in &segment.records {
                    if active.operation != "add"
                        || u64::try_from(active.revision)? > segment.header.generation
                    {
                        bail!("authorization checkpoint contains invalid active tuple state");
                    }
                    active_records.insert(TupleIdentity::from(active), active.clone());
                }
                schema_descriptors = segment.schema_descriptors.clone();
                relation_rules = segment.relation_rules.clone();
            }
            "delta" => {
                if !checkpoint_seen {
                    continue;
                }
                if segment.header.source_stream_cursor < source_stream_cursor
                    || segment.header.base_revision != last_generation
                    || segment.header.generation != last_generation.saturating_add(1)
                {
                    bail!("AuthzCandidateSetStale");
                }
                for mutation in &segment.records {
                    if u64::try_from(mutation.revision)? != segment.header.generation {
                        bail!("authorization delta tuple revision mismatch");
                    }
                    match mutation.operation.as_str() {
                        "add" => {
                            active_records.insert(TupleIdentity::from(mutation), mutation.clone());
                        }
                        "remove" => {
                            active_records.remove(&TupleIdentity::from(mutation));
                        }
                        _ => bail!("authz delta tuple operation is invalid"),
                    }
                }
                if segment.header.schema_replacement {
                    schema_descriptors = segment.schema_descriptors.clone();
                }
                if segment.header.relation_rule_replacement {
                    relation_rules = segment.relation_rules.clone();
                }
            }
            _ => bail!("authz segment has unsupported segment kind"),
        }
        apply_userset_edge_deltas(&mut userset_edges, segment.userset_edges)?;
        apply_list_object_deltas(&mut list_objects, segment.list_objects)?;
        apply_list_subject_deltas(&mut list_subjects, segment.list_subjects)?;
        validate_revision_checkpoint(
            &segment.header,
            &segment.revision_checkpoints,
            &active_records,
            &userset_edges,
            &list_objects,
            &list_subjects,
        )?;
        revision_checkpoints.extend(segment.revision_checkpoints);
        last_generation = segment.header.generation;
        source_stream_cursor = segment.header.source_stream_cursor;
        final_header = Some(segment.header);
    }

    if !checkpoint_seen || last_generation != revision {
        return Ok(None);
    }
    let Some(mut header) = final_header else {
        return Ok(None);
    };
    header.base_revision = 0;
    header.segment_kind = "merged".to_string();
    Ok(Some(DecodedAuthzSegment {
        header,
        records: active_records.into_values().collect(),
        schema_descriptors,
        relation_rules,
        userset_edges: userset_edges.into_values().collect(),
        revision_checkpoints,
        list_objects: list_objects.into_values().collect(),
        list_subjects: list_subjects.into_values().collect(),
    }))
}

fn validate_revision_checkpoint(
    header: &AuthzSegmentHeader,
    checkpoints: &[AuthzRevisionCheckpointRow],
    active_records: &BTreeMap<TupleIdentity, AuthzTupleRecord>,
    userset_edges: &BTreeMap<Vec<u8>, AuthzUsersetEdgeRow>,
    list_objects: &BTreeMap<Vec<u8>, AuthzListObjectsRow>,
    list_subjects: &BTreeMap<Vec<u8>, AuthzListSubjectsRow>,
) -> Result<()> {
    let [checkpoint] = checkpoints else {
        bail!("authorization segment must contain exactly one revision checkpoint");
    };
    let active_records = active_records.values().cloned().collect::<Vec<_>>();
    let derived_userset_count = userset_edges
        .values()
        .filter(|row| row.source == "derived_userset")
        .count() as u64;
    if checkpoint.tenant_id.to_string() != header.tenant_id
        || checkpoint.revision != header.generation
        || checkpoint.source_fence_token != header.source_fence_token
        || checkpoint.tuple_record_count != active_records.len() as u64
        || checkpoint.active_tuple_count != active_records.len() as u64
        || checkpoint.derived_userset_count != derived_userset_count
        || checkpoint.list_objects_count != list_objects.len() as u64
        || checkpoint.list_subjects_count != list_subjects.len() as u64
        || checkpoint.tuple_records_hash != hex::encode(tuple_records_hash(&active_records)?)
    {
        bail!("authorization materialized revision checkpoint mismatch");
    }
    Ok(())
}

pub(super) fn apply_active_tuple_mutations(
    tenant_id: i64,
    previous: &[AuthzTupleRecord],
    mutations: &[AuthzTupleRecord],
    target_revision: u64,
) -> Result<Vec<AuthzTupleRecord>> {
    let mut active = BTreeMap::new();
    for record in previous {
        if record.tenant_id != tenant_id
            || record.operation != "add"
            || u64::try_from(record.revision)? >= target_revision
        {
            bail!("authorization predecessor contains invalid active tuple state");
        }
        active.insert(TupleIdentity::from(record), record.clone());
    }
    let mut ordered = mutations.to_vec();
    ordered.sort_by_key(|record| record.revision_ordinal);
    for record in ordered {
        if record.tenant_id != tenant_id || u64::try_from(record.revision)? != target_revision {
            bail!("authorization delta mutation scope mismatch");
        }
        match record.operation.as_str() {
            "add" => {
                active.insert(TupleIdentity::from(&record), record);
            }
            "remove" => {
                active.remove(&TupleIdentity::from(&record));
            }
            _ => bail!("authorization delta tuple operation is invalid"),
        }
    }
    Ok(active.into_values().collect())
}

fn userset_edge_delta_rows(
    previous: Vec<AuthzUsersetEdgeRow>,
    current: Vec<AuthzUsersetEdgeRow>,
    revision: u64,
) -> Result<Vec<AuthzUsersetEdgeRow>> {
    let previous = rows_by_key(previous, userset_edge_key)?;
    let current = rows_by_key(current, userset_edge_key)?;
    Ok(delta_rows(
        previous,
        current,
        revision,
        |row, revision, operation| {
            row.revision = revision;
            row.operation = operation.to_string();
        },
    ))
}

fn list_object_delta_rows(
    previous: Vec<AuthzListObjectsRow>,
    current: Vec<AuthzListObjectsRow>,
    revision: u64,
) -> Result<Vec<AuthzListObjectsRow>> {
    let previous = rows_by_key(previous, list_object_key)?;
    let current = rows_by_key(current, list_object_key)?;
    Ok(delta_rows(
        previous,
        current,
        revision,
        |row, revision, operation| {
            row.revision = revision;
            row.operation = operation.to_string();
        },
    ))
}

fn list_subject_delta_rows(
    previous: Vec<AuthzListSubjectsRow>,
    current: Vec<AuthzListSubjectsRow>,
    revision: u64,
) -> Result<Vec<AuthzListSubjectsRow>> {
    let previous = rows_by_key(previous, list_subject_key)?;
    let current = rows_by_key(current, list_subject_key)?;
    Ok(delta_rows(
        previous,
        current,
        revision,
        |row, revision, operation| {
            row.revision = revision;
            row.operation = operation.to_string();
        },
    ))
}

fn apply_userset_edge_deltas(
    state: &mut BTreeMap<Vec<u8>, AuthzUsersetEdgeRow>,
    deltas: Vec<AuthzUsersetEdgeRow>,
) -> Result<()> {
    apply_row_deltas(state, deltas, userset_edge_key, |row| &mut row.operation)
}

fn apply_list_object_deltas(
    state: &mut BTreeMap<Vec<u8>, AuthzListObjectsRow>,
    deltas: Vec<AuthzListObjectsRow>,
) -> Result<()> {
    apply_row_deltas(state, deltas, list_object_key, |row| &mut row.operation)
}

fn apply_list_subject_deltas(
    state: &mut BTreeMap<Vec<u8>, AuthzListSubjectsRow>,
    deltas: Vec<AuthzListSubjectsRow>,
) -> Result<()> {
    apply_row_deltas(state, deltas, list_subject_key, |row| &mut row.operation)
}

fn apply_row_deltas<T>(
    state: &mut BTreeMap<Vec<u8>, T>,
    deltas: Vec<T>,
    key_fn: fn(&T) -> Result<Vec<u8>>,
    operation: fn(&mut T) -> &mut String,
) -> Result<()> {
    for mut row in deltas {
        let key = key_fn(&row)?;
        let delta_operation = operation(&mut row).clone();
        match delta_operation.as_str() {
            "add" => {
                *operation(&mut row) = "add".to_string();
                state.insert(key, row);
            }
            "remove" => {
                state.remove(&key);
            }
            _ => bail!("authorization materialized row delta operation is invalid"),
        }
    }
    Ok(())
}

fn rows_by_key<T: Clone>(
    rows: Vec<T>,
    key_fn: fn(&T) -> Result<Vec<u8>>,
) -> Result<BTreeMap<Vec<u8>, T>> {
    let mut by_key = BTreeMap::new();
    for row in rows {
        by_key.insert(key_fn(&row)?, row);
    }
    Ok(by_key)
}

fn delta_rows<T: Clone>(
    previous: BTreeMap<Vec<u8>, T>,
    current: BTreeMap<Vec<u8>, T>,
    revision: u64,
    mut set_delta: impl FnMut(&mut T, u64, &str),
) -> Vec<T> {
    let mut deltas = Vec::new();
    for (key, mut row) in current.iter().map(|(key, row)| (key, row.clone())) {
        if !previous.contains_key(key) {
            set_delta(&mut row, revision, "add");
            deltas.push(row);
        }
    }
    for (key, mut row) in previous {
        if !current.contains_key(&key) {
            set_delta(&mut row, revision, "remove");
            deltas.push(row);
        }
    }
    deltas
}
