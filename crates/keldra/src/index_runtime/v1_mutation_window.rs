//! Exact-path coalescing before v1 payload selection.
//!
//! A producer may inspect many journal mutations while building one unpublished
//! generation. Only the newest mutation for each exact source path can affect
//! that generation: preparation compares it directly with the preceding
//! durable `Current`, so intermediate versions must not be loaded or indexed.

use std::collections::BTreeMap;

use tonic::Status;

pub(crate) const MAX_ADVANCE_SLICE_OPERATIONS: u64 = 4_096;
pub(crate) const MAX_ADVANCE_SLICE_BYTES: usize = 4 * 1024 * 1024;

pub(crate) fn bounded_advance_operations(configured: u64) -> u64 {
    configured.max(1).min(MAX_ADVANCE_SLICE_OPERATIONS)
}

pub(crate) fn journal_read_ahead_pages(parallelism: usize) -> usize {
    parallelism.saturating_mul(2).clamp(2, 32)
}

pub(crate) fn split_publication_chunks<T>(
    mutations: Vec<T>,
    safe_next: u64,
    maximum_operations: u64,
    offset: impl Fn(&T) -> u64,
    atomic_group: impl Fn(&T) -> Option<u64>,
) -> Result<Vec<(Vec<T>, u64)>, Status> {
    if mutations.is_empty() {
        return Ok(vec![(Vec::new(), safe_next)]);
    }
    let maximum_operations = usize::try_from(maximum_operations)
        .unwrap_or(usize::MAX)
        .max(1);
    let mut chunks = Vec::new();
    let mut remaining = mutations;
    while remaining.len() > maximum_operations {
        let mut split = maximum_operations;
        loop {
            let crossing_end = remaining[..split]
                .iter()
                .filter_map(&atomic_group)
                .filter_map(|group| {
                    remaining[split..]
                        .iter()
                        .rposition(|mutation| atomic_group(mutation) == Some(group))
                        .map(|position| split + position + 1)
                })
                .max();
            let Some(end) = crossing_end else { break };
            split = end;
            if split == remaining.len() {
                break;
            }
        }
        if split == remaining.len() {
            break;
        }
        let rest = remaining.split_off(split);
        let next = rest
            .first()
            .map(&offset)
            .ok_or_else(|| Status::data_loss("v1 publication split lost its next mutation"))?;
        chunks.push((remaining, next));
        remaining = rest;
    }
    chunks.push((remaining, safe_next));
    Ok(chunks)
}

pub(crate) struct PublicationChunk<T> {
    pub(crate) mutations: Vec<T>,
    pub(crate) next: u64,
    pub(crate) through_atomic: u64,
}

pub(crate) fn publication_chunks<T>(
    mutations: Vec<T>,
    safe_next: u64,
    maximum_operations: u64,
    starting_through_atomic: u64,
    page_through_atomic: u64,
    offset: impl Fn(&T) -> u64,
    atomic_group: impl Fn(&T) -> Option<u64>,
) -> Result<Vec<PublicationChunk<T>>, Status> {
    // New finalized atomic progress makes the complete coalesced page one
    // visibility unit. A later mutation in this page may have superseded one
    // member of an earlier atomic group; splitting the surviving mutations
    // could otherwise publish the group's other members before that
    // superseding state. The journal page is independently byte-bounded.
    if page_through_atomic > starting_through_atomic {
        return Ok(vec![PublicationChunk {
            mutations,
            next: safe_next,
            through_atomic: page_through_atomic,
        }]);
    }
    let chunks = split_publication_chunks(
        mutations,
        safe_next,
        maximum_operations,
        &offset,
        &atomic_group,
    )?;
    let chunk_count = chunks.len();
    let mut through_atomic = starting_through_atomic;
    Ok(chunks
        .into_iter()
        .enumerate()
        .map(|(index, (mutations, next))| {
            through_atomic = mutations
                .iter()
                .filter_map(&atomic_group)
                .fold(through_atomic, u64::max);
            if index + 1 == chunk_count {
                // A finalized group can be absent after newest-wins coalescing,
                // or the page can contain cursor-only progress. Publish that
                // proof only with the final chunk that covers the whole page.
                through_atomic = through_atomic.max(page_through_atomic);
            }
            PublicationChunk {
                mutations,
                next,
                through_atomic,
            }
        })
        .collect())
}

/// Retain the newest mutation for each exact path in one unpublished safe cut.
///
/// Input and output use canonical `(source offset, mutation ordinal)` order.
/// This function does not choose the safe cut or split atomic groups; callers
/// must do that first and must still reject a path repeated within one atomic
/// group. The returned mutations can then be prepared directly against the
/// preceding durable generation while the checkpoint advances across the full
/// inspected input range.
pub(crate) fn coalesce_latest_by_source_path<T>(
    mutations: Vec<T>,
    identity: impl Fn(&T) -> (String, u64, u32),
) -> Result<Vec<T>, Status> {
    let mut previous = None;
    let mut latest = BTreeMap::<String, ((u64, u32), T)>::new();
    for mutation in mutations {
        let (path, offset, ordinal) = identity(&mutation);
        let position = (offset, ordinal);
        if path.is_empty() || previous.is_some_and(|previous| previous >= position) {
            return Err(Status::data_loss(
                "v1 mutation window is not in canonical source order",
            ));
        }
        previous = Some(position);
        latest.insert(path, (position, mutation));
    }
    let mut output = latest.into_values().collect::<Vec<_>>();
    output.sort_unstable_by_key(|(position, _)| *position);
    Ok(output.into_iter().map(|(_, mutation)| mutation).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct Mutation {
        path: String,
        offset: u64,
        ordinal: u32,
        version: u64,
    }

    fn mutation(path: impl Into<String>, offset: u64, version: u64) -> Mutation {
        Mutation {
            path: path.into(),
            offset,
            ordinal: 0,
            version,
        }
    }

    #[test]
    fn pathological_hot_set_retains_one_final_mutation_per_exact_path() {
        let input = (0..10_000_u64)
            .map(|offset| {
                mutation(
                    format!("objects/{:03}", offset % 256),
                    offset + 1,
                    offset + 7,
                )
            })
            .collect::<Vec<_>>();
        let output = coalesce_latest_by_source_path(input, |mutation| {
            (mutation.path.clone(), mutation.offset, mutation.ordinal)
        })
        .unwrap();

        assert_eq!(output.len(), 256);
        assert!(
            output
                .windows(2)
                .all(|pair| pair[0].offset < pair[1].offset)
        );
        for mutation in output {
            assert_eq!(
                (mutation.offset - 1) % 256,
                mutation.path[8..].parse::<u64>().unwrap()
            );
            assert_eq!(mutation.version, mutation.offset + 6);
            assert!(mutation.offset > 9_744);
        }
    }

    #[test]
    fn noncanonical_source_order_fails_closed() {
        let input = vec![mutation("objects/a", 2, 2), mutation("objects/b", 1, 1)];
        let error = coalesce_latest_by_source_path(input, |mutation| {
            (mutation.path.clone(), mutation.offset, mutation.ordinal)
        })
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }
}
