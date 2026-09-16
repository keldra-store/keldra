use std::collections::BTreeSet;

use super::{
    Child, ComponentCompactionLimits, ComponentCompactionPlan, ComponentSegmentDescriptor,
    ComponentStreamRoot, Page, child_matches, decode_page, validate_root, validate_segments,
};
use crate::IndexError;

impl ComponentCompactionPlan {
    /// Exact immutable inputs selected for this bounded merge.
    pub fn input_runs(&self) -> &[ComponentSegmentDescriptor] {
        &self.inputs
    }
}

struct PageLoader<F> {
    component: super::ComponentIdentity,
    load: F,
}

/// Retain the callback's accounted allocation owner while decoded pages are
/// on the traversal stack. There is no history-sized decoded page cache.
struct LoadedPage<Owner> {
    page: Page,
    encoded_bytes: u64,
    _owner: Owner,
}

impl<F> PageLoader<F> {
    fn page<PageBytes>(
        &mut self,
        hash: [u8; 32],
        expected: Option<&Child>,
    ) -> Result<LoadedPage<PageBytes>, IndexError>
    where
        F: FnMut([u8; 32]) -> Result<PageBytes, IndexError>,
        PageBytes: AsRef<[u8]>,
    {
        let owner = (self.load)(hash)?;
        let bytes = owner.as_ref();
        if *crate::profiled_blake3_hash!(bytes).as_bytes() != hash {
            return Err(IndexError::Integrity);
        }
        let page = decode_page(self.component, bytes)?;
        if let Page::Leaf(segments) = &page {
            validate_segments(segments)?;
        }
        let encoded_bytes = bytes.len() as u64;
        if let Some(expected) = expected {
            if !child_matches(&summarize(&page, hash, encoded_bytes)?, expected) {
                return Err(IndexError::Integrity);
            }
        }
        Ok(LoadedPage {
            page,
            encoded_bytes,
            _owner: owner,
        })
    }
}

fn summarize(page: &Page, hash: [u8; 32], encoded_bytes: u64) -> Result<Child, IndexError> {
    match page {
        Page::Leaf(segments) => Child::from_segments(segments, hash, encoded_bytes),
        Page::Branch(children) => Child::from_children(children, hash, encoded_bytes),
    }
}

pub(super) fn select_component_compaction<PageBytes>(
    previous: ComponentStreamRoot,
    load_page: impl FnMut([u8; 32]) -> Result<PageBytes, IndexError>,
    limits: ComponentCompactionLimits,
) -> Result<Option<ComponentCompactionPlan>, IndexError>
where
    PageBytes: AsRef<[u8]>,
{
    let limits = limits.validate()?;
    validate_root(previous)?;
    let mut pages = PageLoader {
        component: previous.component,
        load: load_page,
    };
    let root_page = pages.page(previous.root_hash, None)?;
    let root = summarize(&root_page.page, previous.root_hash, root_page.encoded_bytes)?;
    if root.first_sequence != previous.first_sequence
        || root.last_sequence != previous.last_sequence
        || root.segment_count != previous.segment_count
        || root.encoded_bytes != previous.encoded_bytes
        || root.logical_bytes != previous.logical_bytes
        || root.directory_bytes != previous.directory_bytes
    {
        return Err(IndexError::Integrity);
    }
    let Some(source_level) =
        (0_u8..63).find(|level| level_count(&root, *level) >= limits.l0_trigger as u64)
    else {
        return Ok(None);
    };
    let target_level = source_level + 1;
    let mut source = Vec::with_capacity(limits.l0_trigger);
    collect_level(
        &mut pages,
        previous.root_hash,
        None,
        source_level,
        None,
        limits.l0_trigger,
        &mut source,
    )?;
    if source.len() != limits.l0_trigger {
        return Err(IndexError::Integrity);
    }
    let source_minimum_key = source
        .iter()
        .map(|run| run.minimum_key)
        .min()
        .ok_or(IndexError::Integrity)?;
    let source_maximum_key = source
        .iter()
        .map(|run| run.maximum_key)
        .max()
        .ok_or(IndexError::Integrity)?;
    let mut inputs = source;
    collect_level(
        &mut pages,
        previous.root_hash,
        None,
        target_level,
        Some((source_minimum_key, source_maximum_key)),
        limits
            .maximum_input_runs
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?,
        &mut inputs,
    )?;
    if inputs.len() > limits.maximum_input_runs {
        return Ok(None);
    }
    inputs.sort_unstable_by_key(|run| run.sequence);
    let minimum_key = inputs
        .iter()
        .map(|run| run.minimum_key)
        .min()
        .ok_or(IndexError::Integrity)?;
    let maximum_key = inputs
        .iter()
        .map(|run| run.maximum_key)
        .max()
        .ok_or(IndexError::Integrity)?;
    let selected = inputs
        .iter()
        .map(|run| run.sequence)
        .collect::<BTreeSet<_>>();
    let newest = inputs.last().ok_or(IndexError::Integrity)?.sequence;
    let covers_oldest_history = !has_older_overlap(
        &mut pages,
        previous.root_hash,
        None,
        newest,
        minimum_key,
        maximum_key,
        &selected,
    )?;
    Ok(Some(ComponentCompactionPlan {
        stream_root_hash: previous.root_hash,
        component: previous.component,
        target_level,
        minimum_key,
        maximum_key,
        source_start_offset: inputs
            .iter()
            .map(|run| run.source_start_offset)
            .min()
            .ok_or(IndexError::Integrity)?,
        next_offset: inputs
            .iter()
            .map(|run| run.next_offset)
            .max()
            .ok_or(IndexError::Integrity)?,
        through_atomic_position: inputs
            .iter()
            .map(|run| run.through_atomic_position)
            .max()
            .ok_or(IndexError::Integrity)?,
        inputs,
        covers_oldest_history,
    }))
}

#[allow(clippy::too_many_arguments)]
fn collect_level<F, PageBytes>(
    pages: &mut PageLoader<F>,
    hash: [u8; 32],
    expected: Option<&Child>,
    level: u8,
    overlap: Option<(super::StableDocumentKey, super::StableDocumentKey)>,
    maximum: usize,
    output: &mut Vec<ComponentSegmentDescriptor>,
) -> Result<(), IndexError>
where
    F: FnMut([u8; 32]) -> Result<PageBytes, IndexError>,
    PageBytes: AsRef<[u8]>,
{
    if output.len() >= maximum {
        return Ok(());
    }
    let loaded = pages.page(hash, expected)?;
    match loaded.page {
        Page::Leaf(segments) => {
            output.extend(
                segments
                    .into_iter()
                    .filter(|segment| {
                        segment.level == level
                            && overlap.is_none_or(|(minimum, maximum)| {
                                segment.minimum_key <= maximum && minimum <= segment.maximum_key
                            })
                    })
                    .take(maximum - output.len()),
            );
        }
        Page::Branch(children) => {
            for child in children {
                if output.len() >= maximum {
                    break;
                }
                if level_count(&child, level) == 0
                    || overlap.is_some_and(|(minimum, maximum)| {
                        child.minimum_key > maximum || minimum > child.maximum_key
                    })
                {
                    continue;
                }
                collect_level(
                    pages,
                    child.hash,
                    Some(&child),
                    level,
                    overlap,
                    maximum,
                    output,
                )?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn has_older_overlap<F, PageBytes>(
    pages: &mut PageLoader<F>,
    hash: [u8; 32],
    expected: Option<&Child>,
    before_sequence: u64,
    minimum: super::StableDocumentKey,
    maximum: super::StableDocumentKey,
    selected: &BTreeSet<u64>,
) -> Result<bool, IndexError>
where
    F: FnMut([u8; 32]) -> Result<PageBytes, IndexError>,
    PageBytes: AsRef<[u8]>,
{
    let loaded = pages.page(hash, expected)?;
    match loaded.page {
        Page::Leaf(segments) => Ok(segments.into_iter().any(|segment| {
            segment.sequence < before_sequence
                && !selected.contains(&segment.sequence)
                && segment.minimum_key <= maximum
                && minimum <= segment.maximum_key
        })),
        Page::Branch(children) => {
            for child in children {
                if child.first_sequence >= before_sequence
                    || child.minimum_key > maximum
                    || minimum > child.maximum_key
                {
                    continue;
                }
                if has_older_overlap(
                    pages,
                    child.hash,
                    Some(&child),
                    before_sequence,
                    minimum,
                    maximum,
                    selected,
                )? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn level_count(child: &Child, level: u8) -> u64 {
    child
        .level_counts
        .binary_search_by_key(&level, |(level, _)| *level)
        .ok()
        .map(|index| child.level_counts[index].1)
        .unwrap_or(0)
}
