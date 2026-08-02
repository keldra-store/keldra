//! Pure cluster-wide `ListObjects` page merge.
//!
//! Networking obtains one ownership-filtered page from every ACTIVE node. This
//! module only validates those exact source results and performs the bounded
//! lexical merge; it has no transport, membership, or storage side effects.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use anvil_consensus::NodeId;
use anvil_store::{ListObjectsPage, MAX_LIST_OBJECTS};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActiveListSource {
    node_id: NodeId,
    outcome: ActiveListOutcome,
}

impl ActiveListSource {
    pub(crate) fn page(node_id: NodeId, page: ListObjectsPage) -> Self {
        Self {
            node_id,
            outcome: ActiveListOutcome::Page(page),
        }
    }

    pub(crate) fn unavailable(node_id: NodeId) -> Self {
        Self {
            node_id,
            outcome: ActiveListOutcome::Unavailable,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ActiveListOutcome {
    Page(ListObjectsPage),
    Unavailable,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum InvalidSourcePage {
    #[error("source returned more than 1000 paths")]
    TooManyPaths,
    #[error("source paths are not strictly byte-lexical and duplicate-free")]
    NotStrictlySorted,
    #[error("source returned a path at or before the exclusive start_after cursor")]
    BeforeCursor,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum DistributedListError {
    #[error("list limit {requested} is outside 1..=1000")]
    InvalidLimit { requested: usize },
    #[error("ACTIVE membership repeats node {node_id:?}")]
    DuplicateActiveMember { node_id: NodeId },
    #[error("received a list result from non-ACTIVE node {node_id:?}")]
    UnexpectedSource { node_id: NodeId },
    #[error("received more than one list result from ACTIVE node {node_id:?}")]
    DuplicateSource { node_id: NodeId },
    #[error("ACTIVE node {node_id:?} supplied no list result")]
    MissingSource { node_id: NodeId },
    #[error("ACTIVE node {node_id:?} could not produce its list page")]
    SourceUnavailable { node_id: NodeId },
    #[error("ACTIVE node {node_id:?} supplied an invalid list page: {violation}")]
    InvalidSource {
        node_id: NodeId,
        violation: InvalidSourcePage,
    },
    #[error("path {path:?} was claimed by contradictory owners {first:?} and {second:?}")]
    ContradictoryOwners {
        path: String,
        first: NodeId,
        second: NodeId,
    },
}

/// Merge one exact source result per ACTIVE member into one stateless page.
///
/// Every source page is a fresh read-committed observation for this request.
/// The function retains no state between calls, so a continuation deliberately
/// observes commits made after the previous page.
pub(crate) fn merge_active_list_pages(
    active_members: &[NodeId],
    start_after: Option<&str>,
    limit: usize,
    sources: Vec<ActiveListSource>,
) -> Result<ListObjectsPage, DistributedListError> {
    if !(1..=MAX_LIST_OBJECTS).contains(&limit) {
        return Err(DistributedListError::InvalidLimit { requested: limit });
    }

    let mut active = HashSet::with_capacity(active_members.len());
    for node_id in active_members {
        if !active.insert(*node_id) {
            return Err(DistributedListError::DuplicateActiveMember { node_id: *node_id });
        }
    }

    let mut received = HashMap::with_capacity(sources.len());
    for source in sources {
        if !active.contains(&source.node_id) {
            return Err(DistributedListError::UnexpectedSource {
                node_id: source.node_id,
            });
        }
        if received.insert(source.node_id, source.outcome).is_some() {
            return Err(DistributedListError::DuplicateSource {
                node_id: source.node_id,
            });
        }
    }

    let mut pages = Vec::with_capacity(active_members.len());
    for node_id in active_members {
        let outcome = received
            .remove(node_id)
            .ok_or(DistributedListError::MissingSource { node_id: *node_id })?;
        let ActiveListOutcome::Page(page) = outcome else {
            return Err(DistributedListError::SourceUnavailable { node_id: *node_id });
        };
        validate_source_page(*node_id, &page, start_after)?;
        pages.push((*node_id, page));
    }

    reject_contradictory_owners(&pages)?;
    Ok(merge_valid_pages(&pages, limit))
}

fn validate_source_page(
    node_id: NodeId,
    page: &ListObjectsPage,
    start_after: Option<&str>,
) -> Result<(), DistributedListError> {
    let violation = if page.paths.len() > MAX_LIST_OBJECTS {
        Some(InvalidSourcePage::TooManyPaths)
    } else if page
        .paths
        .windows(2)
        .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
    {
        Some(InvalidSourcePage::NotStrictlySorted)
    } else if start_after.is_some_and(|cursor| {
        page.paths
            .iter()
            .any(|path| path.as_bytes() <= cursor.as_bytes())
    }) {
        Some(InvalidSourcePage::BeforeCursor)
    } else {
        None
    };
    match violation {
        Some(violation) => Err(DistributedListError::InvalidSource { node_id, violation }),
        None => Ok(()),
    }
}

fn reject_contradictory_owners(
    pages: &[(NodeId, ListObjectsPage)],
) -> Result<(), DistributedListError> {
    let path_count = pages.iter().map(|(_, page)| page.paths.len()).sum();
    let mut owners = HashMap::<&str, NodeId>::with_capacity(path_count);
    for (node_id, page) in pages {
        for path in &page.paths {
            if let Some(first) = owners.insert(path.as_str(), *node_id) {
                return Err(DistributedListError::ContradictoryOwners {
                    path: path.clone(),
                    first,
                    second: *node_id,
                });
            }
        }
    }
    Ok(())
}

fn merge_valid_pages(pages: &[(NodeId, ListObjectsPage)], limit: usize) -> ListObjectsPage {
    let mut consumed = vec![0_usize; pages.len()];
    let mut heap = BinaryHeap::with_capacity(pages.len());
    for (source, (_, page)) in pages.iter().enumerate() {
        if let Some(path) = page.paths.first() {
            heap.push(MergeCandidate {
                path: path.as_str(),
                source,
            });
        }
    }

    let mut paths = Vec::with_capacity(limit);
    while paths.len() < limit {
        let Some(candidate) = heap.pop() else {
            break;
        };
        paths.push(candidate.path.to_owned());
        consumed[candidate.source] += 1;
        let next = consumed[candidate.source];
        if let Some(path) = pages[candidate.source].1.paths.get(next) {
            heap.push(MergeCandidate {
                path: path.as_str(),
                source: candidate.source,
            });
        }
    }

    let has_more = pages
        .iter()
        .enumerate()
        .any(|(source, (_, page))| page.has_more || consumed[source] < page.paths.len());
    ListObjectsPage { paths, has_more }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MergeCandidate<'a> {
    path: &'a str,
    source: usize,
}

impl Ord for MergeCandidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .path
            .as_bytes()
            .cmp(self.path.as_bytes())
            .then_with(|| other.source.cmp(&self.source))
    }
}

impl PartialOrd for MergeCandidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u64) -> NodeId {
        NodeId(id)
    }

    fn page(id: u64, paths: &[&str], has_more: bool) -> ActiveListSource {
        ActiveListSource::page(
            node(id),
            ListObjectsPage {
                paths: paths.iter().map(|path| (*path).to_owned()).collect(),
                has_more,
            },
        )
    }

    #[test]
    fn merges_every_active_source_in_global_byte_lexical_order() {
        let merged = merge_active_list_pages(
            &[node(1), node(2), node(3)],
            Some("a"),
            10,
            vec![
                page(3, &["d", "z"], false),
                page(1, &["b", "f"], false),
                page(2, &["c", "g"], false),
            ],
        )
        .unwrap();

        assert_eq!(merged.paths, ["b", "c", "d", "f", "g", "z"]);
        assert!(!merged.has_more);
    }

    #[test]
    fn missing_active_source_makes_the_page_unavailable() {
        assert_eq!(
            merge_active_list_pages(&[node(1), node(2)], None, 10, vec![page(1, &["a"], false)],),
            Err(DistributedListError::MissingSource { node_id: node(2) })
        );
    }

    #[test]
    fn explicit_source_error_makes_the_page_unavailable() {
        assert_eq!(
            merge_active_list_pages(
                &[node(1), node(2)],
                None,
                10,
                vec![
                    page(1, &["a"], false),
                    ActiveListSource::unavailable(node(2)),
                ],
            ),
            Err(DistributedListError::SourceUnavailable { node_id: node(2) })
        );
    }

    #[test]
    fn identical_path_from_two_sources_is_never_deduplicated() {
        assert_eq!(
            merge_active_list_pages(
                &[node(4), node(9)],
                None,
                10,
                vec![
                    page(4, &["same/path"], false),
                    page(9, &["same/path"], false)
                ],
            ),
            Err(DistributedListError::ContradictoryOwners {
                path: "same/path".into(),
                first: node(4),
                second: node(9),
            })
        );
    }

    #[test]
    fn source_output_must_be_sorted_unique_and_after_the_cursor() {
        assert_eq!(
            merge_active_list_pages(&[node(1)], None, 10, vec![page(1, &["b", "a"], false)],),
            Err(DistributedListError::InvalidSource {
                node_id: node(1),
                violation: InvalidSourcePage::NotStrictlySorted,
            })
        );
        assert_eq!(
            merge_active_list_pages(&[node(1)], Some("a"), 10, vec![page(1, &["a", "b"], false)],),
            Err(DistributedListError::InvalidSource {
                node_id: node(1),
                violation: InvalidSourcePage::BeforeCursor,
            })
        );
    }

    #[test]
    fn unconsumed_items_or_a_source_tail_set_has_more() {
        let unconsumed = merge_active_list_pages(
            &[node(1), node(2)],
            None,
            1,
            vec![page(1, &["a"], false), page(2, &["b"], false)],
        )
        .unwrap();
        assert_eq!(unconsumed.paths, ["a"]);
        assert!(unconsumed.has_more);

        let source_tail = merge_active_list_pages(
            &[node(1), node(2)],
            None,
            10,
            vec![page(1, &["a"], false), page(2, &[], true)],
        )
        .unwrap();
        assert_eq!(source_tail.paths, ["a"]);
        assert!(source_tail.has_more);
    }

    #[test]
    fn continuations_pass_two_thousand_without_an_arbitrary_total_cap() {
        let active = [node(1), node(2)];
        let mut start_after = None::<String>;
        let mut all = Vec::new();
        let mut page_lengths = Vec::new();

        loop {
            let sources = active
                .iter()
                .map(|owner| generated_source_page(*owner, start_after.as_deref()))
                .collect();
            let merged =
                merge_active_list_pages(&active, start_after.as_deref(), MAX_LIST_OBJECTS, sources)
                    .unwrap();
            page_lengths.push(merged.paths.len());
            all.extend(merged.paths.iter().cloned());
            if !merged.has_more {
                break;
            }
            start_after = merged.paths.last().cloned();
        }

        assert_eq!(page_lengths, [1_000, 1_000, 5]);
        assert_eq!(all.len(), 2_005);
        assert_eq!(all.first().map(String::as_str), Some("item/0000"));
        assert_eq!(all.last().map(String::as_str), Some("item/2004"));
        assert!(all.windows(2).all(|pair| pair[0] < pair[1]));
    }

    fn generated_source_page(owner: NodeId, start_after: Option<&str>) -> ActiveListSource {
        let mut current = (0..2_005)
            .filter(|index| node(1 + (*index as u64 % 2)) == owner)
            .map(|index| format!("item/{index:04}"))
            .filter(|path| start_after.is_none_or(|cursor| path.as_str() > cursor));
        let paths = current.by_ref().take(MAX_LIST_OBJECTS).collect::<Vec<_>>();
        let has_more = current.next().is_some();
        ActiveListSource::page(owner, ListObjectsPage { paths, has_more })
    }

    #[test]
    fn every_page_is_a_fresh_read_committed_merge() {
        let active = [node(1), node(2)];
        let first = merge_active_list_pages(
            &active,
            None,
            1,
            vec![page(1, &["a"], false), page(2, &["c"], false)],
        )
        .unwrap();
        assert_eq!(first.paths, ["a"]);
        assert!(first.has_more);

        // Between requests, `b` commits and `c` is deleted. The continuation
        // uses only its exclusive path cursor and the new source observations.
        let second = merge_active_list_pages(
            &active,
            Some("a"),
            10,
            vec![page(1, &["b"], false), page(2, &[], false)],
        )
        .unwrap();
        assert_eq!(second.paths, ["b"]);
        assert!(!second.has_more);
    }
}
