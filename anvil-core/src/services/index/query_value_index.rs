use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TypedValueCandidateEntry {
    pub(super) row_ordinal: usize,
    pub(super) source_identity: String,
}

pub(super) async fn typed_json_candidate_entries_from_value_index(
    storage: &crate::storage::Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    segment_ref: &str,
    predicates: &[TypedPredicate],
    row_count: u64,
) -> Result<Vec<TypedValueCandidateEntry>, Status> {
    if predicates.is_empty() {
        return Err(Status::failed_precondition("IndexCapabilityMissing"));
    }

    let mut selected: Option<BTreeMap<usize, String>> = None;
    for predicate in predicates {
        let lookups = typed_json_value_index_lookups_for_predicate(predicate)?;
        if lookups.is_empty() {
            selected = Some(BTreeMap::new());
            break;
        }
        let entries = typed_field_segment::read_typed_field_value_index_entries(
            storage,
            mvcc,
            segment_ref,
            lookups,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let predicate_entries = typed_json_predicate_entries_from_entries(&entries, predicate)?;
        selected = Some(match selected {
            Some(existing) => existing
                .into_iter()
                .filter_map(|(ordinal, source_identity)| {
                    predicate_entries
                        .contains_key(&ordinal)
                        .then_some((ordinal, source_identity))
                })
                .collect(),
            None => predicate_entries,
        });
    }

    let row_count =
        usize::try_from(row_count).map_err(|_| Status::internal("typed index too large"))?;
    Ok(selected
        .unwrap_or_default()
        .into_iter()
        .filter(|(ordinal, _)| *ordinal < row_count)
        .map(|(row_ordinal, source_identity)| TypedValueCandidateEntry {
            row_ordinal,
            source_identity,
        })
        .collect())
}

pub(super) async fn boundary_candidate_entries_from_value_index(
    storage: &crate::storage::Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    segment_ref: &str,
    predicates: &[BoundaryPredicate],
    row_count: u64,
) -> Result<Vec<TypedValueCandidateEntry>, Status> {
    if predicates.is_empty() {
        return Err(Status::failed_precondition("IndexCapabilityMissing"));
    }

    let mut selected: Option<BTreeMap<usize, String>> = None;
    for predicate in predicates {
        let typed = TypedPredicate {
            field: predicate.field.clone(),
            op: predicate.op.clone(),
            values: predicate.values.clone(),
        };
        let lookups = typed_json_value_index_lookups_for_predicate(&typed)?;
        if lookups.is_empty() {
            selected = Some(BTreeMap::new());
            break;
        }
        let entries = typed_field_segment::read_typed_field_value_index_entries(
            storage,
            mvcc,
            segment_ref,
            lookups,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let predicate_entries = typed_json_predicate_entries_from_entries(&entries, &typed)?;
        selected = Some(match selected {
            Some(existing) => existing
                .into_iter()
                .filter_map(|(ordinal, source_identity)| {
                    predicate_entries
                        .contains_key(&ordinal)
                        .then_some((ordinal, source_identity))
                })
                .collect(),
            None => predicate_entries,
        });
    }

    let row_count =
        usize::try_from(row_count).map_err(|_| Status::internal("typed index too large"))?;
    Ok(selected
        .unwrap_or_default()
        .into_iter()
        .filter(|(ordinal, _)| *ordinal < row_count)
        .map(|(row_ordinal, source_identity)| TypedValueCandidateEntry {
            row_ordinal,
            source_identity,
        })
        .collect())
}

pub(super) fn intersect_typed_candidate_entries(
    left: Vec<TypedValueCandidateEntry>,
    right: Vec<TypedValueCandidateEntry>,
) -> Vec<TypedValueCandidateEntry> {
    let right_ordinals = right
        .into_iter()
        .map(|entry| entry.row_ordinal)
        .collect::<BTreeSet<_>>();
    left.into_iter()
        .filter(|entry| right_ordinals.contains(&entry.row_ordinal))
        .collect()
}

pub(super) async fn metadata_candidate_entries_from_value_index(
    storage: &crate::storage::Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    segment_ref: &str,
    path_prefix: &str,
    filters: &QueryFilters,
    row_count: u64,
) -> Result<Vec<TypedValueCandidateEntry>, Status> {
    if filters.metadata.is_empty() && path_prefix.trim().is_empty() {
        return Err(Status::failed_precondition("IndexCapabilityMissing"));
    }

    let mut selected: Option<BTreeMap<usize, String>> = None;
    if !path_prefix.trim().is_empty() {
        let predicate = TypedPredicate {
            field: "object_key".to_string(),
            op: "prefix".to_string(),
            values: vec![JsonValue::String(path_prefix.to_string())],
        };
        let entries = typed_field_segment::read_typed_field_value_index_entries(
            storage,
            mvcc,
            segment_ref,
            [typed_field_segment::TypedFieldValueIndexLookup {
                field_name: "object_key".to_string(),
                encoded_value: None,
            }],
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        selected = Some(typed_json_predicate_entries_from_entries(
            &entries, &predicate,
        )?);
    }

    for filter in &filters.metadata {
        let encoded_value = typed_field_segment::encode_json_value_for_typed_index(
            &filter.expected,
        )
        .map_err(|e| Status::invalid_argument(format!("Invalid metadata filter value: {e}")))?;
        let entries = typed_field_segment::read_typed_field_value_index_entries(
            storage,
            mvcc,
            segment_ref,
            [typed_field_segment::TypedFieldValueIndexLookup {
                field_name: filter.field.clone(),
                encoded_value: Some(encoded_value),
            }],
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let filter_entries = entries
            .into_iter()
            .map(|entry| (entry.row_ordinal, entry.source_identity))
            .collect::<BTreeMap<_, _>>();
        selected = Some(match selected {
            Some(existing) => existing
                .into_iter()
                .filter_map(|(ordinal, source_identity)| {
                    filter_entries
                        .contains_key(&ordinal)
                        .then_some((ordinal, source_identity))
                })
                .collect(),
            None => filter_entries,
        });
    }

    let row_count =
        usize::try_from(row_count).map_err(|_| Status::internal("metadata index too large"))?;
    Ok(selected
        .unwrap_or_default()
        .into_iter()
        .filter(|(ordinal, _)| *ordinal < row_count)
        .map(|(row_ordinal, source_identity)| TypedValueCandidateEntry {
            row_ordinal,
            source_identity,
        })
        .collect())
}
