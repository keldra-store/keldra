use super::*;

#[derive(Default)]
pub(super) struct AlignedGates {
    pub(super) gates: Vec<Option<QueryDocumentGate>>,
    pub(super) resident_bytes: usize,
}

impl AlignedGates {
    pub(super) fn into_parts(self) -> (Vec<Option<QueryDocumentGate>>, usize) {
        (self.gates, self.resident_bytes)
    }
}

pub(super) fn resident_gate_dynamic_bytes(gate: &QueryDocumentGate) -> Result<usize, IndexError> {
    resident_gate_bytes(gate)?
        .checked_sub(
            std::mem::size_of::<StableDocumentKey>()
                .saturating_add(std::mem::size_of::<QueryDocumentGate>()),
        )
        .ok_or(IndexError::Integrity)
}

pub(super) fn candidate_is_current(
    membership: Option<&QueryDocumentGate>,
    presence: Option<&QueryDocumentGate>,
    candidate_material_version: u64,
) -> bool {
    membership.is_some_and(|gate| gate.live)
        && presence.is_some_and(|gate| {
            gate.live && candidate_material_version == gate.material_source_version
        })
}

#[cfg(test)]
mod replacement_tests {
    use super::*;

    fn gate(version: u64, live: bool) -> QueryDocumentGate {
        QueryDocumentGate {
            document: StableDocumentKey::from_bytes([7; 32]).unwrap(),
            material_source_version: version,
            current_source_version: version,
            live,
            source_path: Some("objects/a".into()),
            canonical_source_path: None,
            result_path: Some("objects/a".into()),
            result_version: version,
        }
    }

    #[test]
    fn replacement_rejects_old_postings_without_per_term_subtraction() {
        let membership = gate(2, true);
        let presence = gate(2, true);
        assert!(!candidate_is_current(Some(&membership), Some(&presence), 1));
        assert!(candidate_is_current(Some(&membership), Some(&presence), 2));
        assert!(!candidate_is_current(Some(&membership), Some(&presence), 3));
    }

    #[test]
    fn deletion_and_resurrection_do_not_revive_previous_material() {
        let dead = gate(2, false);
        assert!(!candidate_is_current(Some(&dead), Some(&dead), 1));
        let resurrected = gate(3, true);
        assert!(!candidate_is_current(
            Some(&resurrected),
            Some(&resurrected),
            1
        ));
        assert!(candidate_is_current(
            Some(&resurrected),
            Some(&resurrected),
            3
        ));
        assert!(!candidate_is_current(None, Some(&resurrected), 3));
    }
}

pub(super) fn select_handoff_candidate(
    selected: &mut BTreeMap<StableDocumentKey, QueryAdmissionCandidate>,
    incoming: QueryAdmissionCandidate,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(), IndexError> {
    use std::collections::btree_map::Entry;
    match selected.entry(incoming.document) {
        Entry::Vacant(entry) => {
            budget.reserve_heap(credits, resident_selected_candidate_bytes(&incoming)?)?;
            entry.insert(incoming);
        }
        Entry::Occupied(mut entry) => {
            let current = entry.get().clone();
            if current.handoff_lineage_id != incoming.handoff_lineage_id {
                return Err(IndexError::Integrity);
            }
            match incoming
                .covered_through_source_position
                .cmp(&current.covered_through_source_position)
            {
                Ordering::Greater => {
                    replace_selected_candidate_charge(credits, budget, &current, &incoming)?;
                    entry.insert(incoming);
                }
                Ordering::Equal
                    if incoming.material_source_version != current.material_source_version
                        || incoming.current_source_version != current.current_source_version
                        || incoming.source_path != current.source_path
                        || incoming.result_path != current.result_path
                        || incoming.result_version != current.result_version =>
                {
                    return Err(IndexError::Integrity);
                }
                Ordering::Equal if incoming.partition < current.partition => {
                    replace_selected_candidate_charge(credits, budget, &current, &incoming)?;
                    entry.insert(incoming);
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn replace_selected_candidate_charge(
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
    current: &QueryAdmissionCandidate,
    incoming: &QueryAdmissionCandidate,
) -> Result<(), IndexError> {
    let current = resident_selected_candidate_bytes(current)?;
    let incoming = resident_selected_candidate_bytes(incoming)?;
    if incoming > current {
        budget.reserve_heap(credits, incoming - current)
    } else {
        budget.release_heap(credits, current - incoming)
    }
}
