//! Exact rebasing of an immutable merge result over a newer committed view.

use std::collections::BTreeMap;

use keldra_index::v4::SegmentDescriptor;
use tonic::Status;

use super::super::committed_view::{
    IndexCommitManifest, LocatorRoot, MAX_LOCATOR_ROOTS_PER_COMMIT,
};

pub(super) struct RebasedMergeCandidate {
    pub(super) segments: Vec<SegmentDescriptor>,
    pub(super) locator_roots: Vec<LocatorRoot>,
}

pub(super) fn rebase_merge_candidate(
    base: &IndexCommitManifest,
    candidate_segments: &[SegmentDescriptor],
    candidate_locator_roots: &[LocatorRoot],
    observed: &IndexCommitManifest,
) -> Result<RebasedMergeCandidate, Status> {
    if base.index_id != observed.index_id
        || base.definition_version != observed.definition_version
        || base.kind != observed.kind
        || base.schema_fingerprint != observed.schema_fingerprint
        || base.physical_order != observed.physical_order
    {
        return Err(Status::aborted(
            "merge base cannot be rebased across an index definition change",
        ));
    }
    let (segments, segment_changed) =
        rebase_segments(&base.segments, candidate_segments, &observed.segments)?;
    let (locator_roots, locator_changed) = rebase_locator_roots(
        &base.locator_roots,
        candidate_locator_roots,
        &observed.locator_roots,
    )?;
    if !segment_changed && !locator_changed {
        return Err(Status::failed_precondition(
            "merge candidate does not replace any exact committed input",
        ));
    }
    Ok(RebasedMergeCandidate {
        segments,
        locator_roots,
    })
}

fn rebase_locator_roots(
    base: &[LocatorRoot],
    candidate: &[LocatorRoot],
    observed: &[LocatorRoot],
) -> Result<(Vec<LocatorRoot>, bool), Status> {
    let base_by_sequence = base
        .iter()
        .map(|root| (root.sequence, root))
        .collect::<BTreeMap<_, _>>();
    let candidate_by_sequence = candidate
        .iter()
        .map(|root| (root.sequence, root))
        .collect::<BTreeMap<_, _>>();
    let observed_by_sequence = observed
        .iter()
        .map(|root| (root.sequence, root))
        .collect::<BTreeMap<_, _>>();
    let inputs = base_by_sequence
        .iter()
        .filter(|(sequence, root)| candidate_by_sequence.get(sequence) != Some(root))
        .map(|(sequence, root)| (*sequence, *root))
        .collect::<Vec<_>>();
    for (sequence, input) in &inputs {
        if observed_by_sequence.get(sequence) != Some(input) {
            return Err(Status::aborted(
                "merge input locator root changed before rebase",
            ));
        }
    }

    let mut rebased = observed
        .iter()
        .filter(|root| {
            !inputs
                .iter()
                .any(|(sequence, _)| *sequence == root.sequence)
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut changed = !inputs.is_empty();
    let mut new_outputs = Vec::new();
    for candidate_root in candidate {
        if base_by_sequence.get(&candidate_root.sequence) == Some(&candidate_root) {
            continue;
        }
        changed = true;
        if !base_by_sequence.contains_key(&candidate_root.sequence) {
            new_outputs.push(candidate_root.clone());
            continue;
        }
        // Locator-prefix compaction deliberately replaces its newest input at
        // that input's sequence. Keeping this sequence below concurrently
        // appended roots preserves newer-wins lookup semantics. Only roots
        // whose sequence was absent from the base are segment-merge outputs
        // that need collision-free remapping below.
        let output = candidate_root.clone();
        if let Some(existing) = rebased.iter().find(|root| root.sequence == output.sequence) {
            if existing != &output {
                return Err(Status::data_loss(
                    "merge locator replacement conflicts with a committed root",
                ));
            }
        } else {
            rebased.push(output);
        }
    }
    // Locator sequences are manifest-local, unlike snowflake segment IDs. A
    // concurrent incremental publication may therefore consume the merge
    // candidate's sequence values. Place every genuinely new merge root above
    // the exact maximum observed sequence while preserving output order.
    new_outputs.sort_by_key(|root| root.sequence);
    let mut next_sequence = match (
        observed.iter().map(|root| root.sequence).max(),
        new_outputs.is_empty(),
    ) {
        (_, true) => 1,
        (None, false) => 1,
        (Some(maximum), false) => maximum.checked_add(1).ok_or_else(|| {
            Status::resource_exhausted("merge locator sequence exhausted during rebase")
        })?,
    };
    let output_count = new_outputs.len();
    for (position, mut output) in new_outputs.into_iter().enumerate() {
        output.sequence = next_sequence;
        if position + 1 < output_count {
            next_sequence = next_sequence.checked_add(1).ok_or_else(|| {
                Status::resource_exhausted("merge locator sequence exhausted during rebase")
            })?;
        }
        rebased.push(output);
    }
    if rebased.len() > MAX_LOCATOR_ROOTS_PER_COMMIT {
        return Err(Status::aborted(
            "concurrent publication consumed the merge locator headroom",
        ));
    }
    rebased.sort_by_key(|root| root.sequence);
    Ok((rebased, changed))
}

fn rebase_segments(
    base: &[SegmentDescriptor],
    candidate: &[SegmentDescriptor],
    observed: &[SegmentDescriptor],
) -> Result<(Vec<SegmentDescriptor>, bool), Status> {
    let key = |segment: &SegmentDescriptor| segment.identity.segment_id;
    let base_by_key = base
        .iter()
        .map(|value| (key(value), value))
        .collect::<BTreeMap<_, _>>();
    let candidate_by_key = candidate
        .iter()
        .map(|value| (key(value), value))
        .collect::<BTreeMap<_, _>>();
    let observed_by_key = observed
        .iter()
        .map(|value| (key(value), value))
        .collect::<BTreeMap<_, _>>();

    // A merge creates snowflake-unique output segment IDs. Reusing an input ID
    // with different descriptor/live state is not a merge and must never be
    // mistaken for one during a CAS rebase.
    if base_by_key.iter().any(|(identity, base_value)| {
        candidate_by_key
            .get(identity)
            .is_some_and(|candidate_value| candidate_value != base_value)
    }) {
        return Err(Status::data_loss(
            "merge candidate changed an input segment without allocating a new identity",
        ));
    }
    let inputs = base_by_key
        .iter()
        .filter(|(identity, _)| !candidate_by_key.contains_key(identity))
        .map(|(identity, value)| (*identity, *value))
        .collect::<Vec<_>>();
    if inputs.is_empty() {
        if base == candidate {
            return Ok((observed.to_vec(), false));
        }
        return Err(Status::data_loss(
            "merge segment output has no exact input replacement",
        ));
    }
    for (identity, input) in &inputs {
        if observed_by_key.get(identity) != Some(input) {
            return Err(Status::aborted("merge input segment changed before rebase"));
        }
    }

    let outputs = candidate_by_key
        .iter()
        .filter(|(identity, _)| !base_by_key.contains_key(identity))
        .map(|(identity, value)| (*identity, *value))
        .collect::<Vec<_>>();
    let mut rebased = observed
        .iter()
        .filter(|value| !inputs.iter().any(|(identity, _)| *identity == key(value)))
        .cloned()
        .collect::<Vec<_>>();
    for (identity, output) in outputs {
        if let Some(existing) = rebased.iter().find(|value| key(value) == identity) {
            if existing != output {
                return Err(Status::data_loss(
                    "snowflake merge output identity conflicts with a newer committed segment",
                ));
            }
        } else {
            rebased.push(output.clone());
        }
    }
    rebased.sort_by_key(key);
    Ok((rebased, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use keldra_index::v4::{
        ArtifactDescriptor, COMPONENT_HEADER_BYTES, ComponentKind, SegmentIdentity,
    };

    use crate::index_runtime::committed_view::LocatorPackOwnership;

    fn segment(id: u64, live_documents: u32) -> SegmentDescriptor {
        SegmentDescriptor {
            identity: SegmentIdentity::new(7, 3, [4; 32], id).unwrap(),
            document_count: 1,
            live_document_count: live_documents,
            packs: Vec::new(),
            components: Vec::new(),
            encoded_bytes: 1,
            logical_bytes: 1,
        }
    }

    fn locator(sequence: u64, segment_id: u64) -> LocatorRoot {
        let identity = SegmentIdentity::new(7, 3, [4; 32], segment_id).unwrap();
        LocatorRoot {
            sequence,
            identity,
            artifact: ArtifactDescriptor::new(
                7,
                0,
                0,
                COMPONENT_HEADER_BYTES as u64,
                0,
                ComponentKind::ROUTING_NODE,
                1,
                [8; 32],
            )
            .unwrap(),
            pack_ownership: LocatorPackOwnership::Segment,
            encoded_bytes: 1,
            logical_bytes: 1,
        }
    }

    #[test]
    fn concurrent_incremental_segments_survive_merge_rebase() {
        let base = vec![segment(1, 1), segment(2, 1), segment(9, 1)];
        let candidate = vec![segment(9, 1), segment(10, 1)];
        let observed = vec![segment(1, 1), segment(2, 1), segment(9, 1), segment(11, 1)];

        let (rebased, changed) = rebase_segments(&base, &candidate, &observed).unwrap();

        assert!(changed);
        assert_eq!(
            rebased
                .iter()
                .map(|segment| segment.identity.segment_id)
                .collect::<Vec<_>>(),
            vec![9, 10, 11]
        );
    }

    #[test]
    fn concurrent_incremental_liveness_change_rejects_stale_merge_input() {
        let base = vec![segment(1, 1), segment(2, 1)];
        let candidate = vec![segment(10, 1)];
        let observed = vec![segment(1, 0), segment(2, 1), segment(11, 1)];

        let error = rebase_segments(&base, &candidate, &observed).unwrap_err();
        assert_eq!(error.code(), tonic::Code::Aborted);
    }

    #[test]
    fn concurrent_non_input_liveness_change_survives_merge_rebase() {
        let base = vec![segment(1, 1), segment(2, 1), segment(9, 1)];
        let candidate = vec![segment(9, 1), segment(10, 1)];
        let observed = vec![segment(1, 1), segment(2, 1), segment(9, 0), segment(11, 1)];

        let (rebased, changed) = rebase_segments(&base, &candidate, &observed).unwrap();

        assert!(changed);
        assert_eq!(
            rebased
                .iter()
                .map(|segment| (segment.identity.segment_id, segment.live_document_count))
                .collect::<Vec<_>>(),
            vec![(9, 0), (10, 1), (11, 1)]
        );
    }

    #[test]
    fn merge_cannot_reuse_an_input_segment_identity_for_changed_state() {
        let base = vec![segment(1, 1), segment(2, 1)];
        let candidate = vec![segment(1, 0), segment(10, 1)];

        let error = rebase_segments(&base, &candidate, &base).unwrap_err();

        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn snowflake_output_collision_is_never_falsely_accepted() {
        let base = vec![segment(1, 1), segment(2, 1)];
        let candidate = vec![segment(10, 1)];
        let observed = vec![segment(1, 1), segment(2, 1), segment(10, 0)];

        let error = rebase_segments(&base, &candidate, &observed).unwrap_err();

        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn concurrent_incremental_locator_sequence_is_preserved_and_merge_output_is_renumbered() {
        let candidate = vec![locator(2, 10)];
        let observed = vec![locator(2, 11)];

        let (rebased, changed) = rebase_locator_roots(&[], &candidate, &observed).unwrap();

        assert!(changed);
        assert_eq!(
            rebased
                .iter()
                .map(|root| (root.sequence, root.identity.segment_id))
                .collect::<Vec<_>>(),
            vec![(2, 11), (3, 10)]
        );
    }

    #[test]
    fn merge_output_sequences_follow_the_observed_max_and_preserve_relative_order() {
        let candidate = vec![locator(4, 14), locator(3, 13)];
        let observed = vec![locator(9, 19), locator(2, 12)];

        let (rebased, changed) = rebase_locator_roots(&[], &candidate, &observed).unwrap();

        assert!(changed);
        assert_eq!(
            rebased
                .iter()
                .map(|root| (root.sequence, root.identity.segment_id))
                .collect::<Vec<_>>(),
            vec![(2, 12), (9, 19), (10, 13), (11, 14)]
        );
    }

    #[test]
    fn changed_locator_input_aborts_only_the_stale_merge() {
        let base = vec![locator(1, 1), locator(2, 2)];
        let mut detached = locator(1, 1);
        detached.pack_ownership = LocatorPackOwnership::Standalone(Vec::new());
        let candidate = vec![detached, locator(2, 2), locator(3, 10)];
        let observed = vec![locator(1, 11), locator(2, 2), locator(3, 12)];

        let error = rebase_locator_roots(&base, &candidate, &observed).unwrap_err();

        assert_eq!(error.code(), tonic::Code::Aborted);
    }

    #[test]
    fn locator_prefix_replacement_stays_below_concurrent_appends() {
        let base = vec![locator(1, 1), locator(2, 2)];
        let candidate = vec![locator(2, 10)];
        let observed = vec![locator(1, 1), locator(2, 2), locator(3, 11)];

        let (rebased, changed) = rebase_locator_roots(&base, &candidate, &observed).unwrap();

        assert!(changed);
        assert_eq!(
            rebased
                .iter()
                .map(|root| (root.sequence, root.identity.segment_id))
                .collect::<Vec<_>>(),
            vec![(2, 10), (3, 11)]
        );
    }

    #[test]
    fn concurrent_append_that_consumes_locator_headroom_aborts_merge() {
        let candidate = vec![locator(1, 9_999)];
        let observed = (1..=MAX_LOCATOR_ROOTS_PER_COMMIT as u64)
            .map(|sequence| locator(sequence, sequence))
            .collect::<Vec<_>>();

        let error = rebase_locator_roots(&[], &candidate, &observed).unwrap_err();

        assert_eq!(error.code(), tonic::Code::Aborted);
    }
}
