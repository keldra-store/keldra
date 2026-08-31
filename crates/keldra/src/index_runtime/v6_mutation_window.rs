//! Exact-path coalescing before v6 payload selection.
//!
//! A producer may inspect many journal mutations while building one unpublished
//! generation. Only the newest mutation for each exact source path can affect
//! that generation: preparation compares it directly with the preceding
//! durable `Current`, so intermediate versions must not be loaded or indexed.

use std::collections::BTreeMap;

use tonic::Status;

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
                "v6 mutation window is not in canonical source order",
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
                mutation.path[8..].parse().unwrap()
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
