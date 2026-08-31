//! Immutable, bounded page tree for v6 query-run descriptors.
//!
//! Appends copy only the right spine. Leaf records name mini-run descriptor
//! artifacts; no posting, point, or document-value data is copied here.

use std::collections::BTreeMap;

use crate::IndexError;

use super::{ProjectionPartitionIdentity, ProjectionQueryStreamRoot};

const MAGIC: &[u8; 8] = b"K6QPG002";
const FORMAT: u16 = 2;
pub const QUERY_RUN_PAGE_FANOUT: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryRunReference {
    pub hash: [u8; 32],
    pub sequence: u64,
    /// Whole-run LSM level. Fresh mini-runs are level zero; compaction replaces
    /// a contiguous same-level window with one run at the next level.
    pub level: u8,
    pub source_start_offset: u64,
    pub next_offset: u64,
    pub through_atomic_position: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryRunChild {
    pub hash: [u8; 32],
    pub run_count: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub source_start_offset: u64,
    pub next_offset: u64,
    pub through_atomic_position: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryRunPage {
    Leaf(Vec<QueryRunReference>),
    Branch(Vec<QueryRunChild>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedQueryRunPage {
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedQueryRunAppend {
    pub root: ProjectionQueryStreamRoot,
    pub pages: Vec<EncodedQueryRunPage>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryRunCompactionLimits {
    /// Number of adjacent runs at one level that makes that level eligible.
    pub level_trigger: usize,
    /// Hard fan-in bound for one compaction. This bounds merge lanes and the
    /// amount of page-tree state retained by a plan, not stream lifetime.
    pub maximum_input_runs: usize,
}

impl QueryRunCompactionLimits {
    pub fn validate(self) -> Result<Self, IndexError> {
        if self.level_trigger < 2
            || self.maximum_input_runs < self.level_trigger
            || self.maximum_input_runs > QUERY_RUN_PAGE_FANOUT
        {
            return invalid("query run compaction limits are invalid");
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRunCompactionPlan {
    stream_root_hash: [u8; 32],
    root_next_offset: u64,
    root_through_atomic_position: u64,
    inputs_newest_first: Vec<QueryRunReference>,
    output_level: u8,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
}

impl QueryRunCompactionPlan {
    pub fn inputs_newest_first(&self) -> &[QueryRunReference] {
        &self.inputs_newest_first
    }

    pub const fn output_level(&self) -> u8 {
        self.output_level
    }

    pub const fn source_start_offset(&self) -> u64 {
        self.source_start_offset
    }

    pub const fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub const fn through_atomic_position(&self) -> u64 {
        self.through_atomic_position
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedQueryRunSplice {
    pub root: ProjectionQueryStreamRoot,
    pub pages: Vec<EncodedQueryRunPage>,
}

pub fn append_query_run_path_copy(
    previous: Option<ProjectionQueryStreamRoot>,
    partition: ProjectionPartitionIdentity,
    catalog: [u8; 32],
    reference: QueryRunReference,
    mut load: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<PreparedQueryRunAppend, IndexError> {
    partition.validate()?;
    validate_reference(reference)?;
    if reference.level != 0 {
        return invalid("an appended query mini-run is not level zero");
    }
    if catalog == [0; 32] {
        return invalid("query run catalog is zero");
    }
    let mut pages = Vec::new();
    let child = match previous {
        None => {
            let page = encode_page(QueryRunPage::Leaf(vec![reference]))?;
            pages.push(page.clone());
            page
        }
        Some(root) if root.run_count == 0 => {
            root.validate_at(root.next_offset, root.through_atomic_position)?;
            if reference.sequence != 1
                || reference.source_start_offset != root.next_offset
                || reference.through_atomic_position < root.through_atomic_position
            {
                return invalid("first query run does not continue the empty root cut");
            }
            let page = encode_page(QueryRunPage::Leaf(vec![reference]))?;
            pages.push(page.clone());
            page
        }
        Some(root) => {
            root.validate_at(root.next_offset, root.through_atomic_position)?;
            if reference.sequence
                != root
                    .last_sequence
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?
                || reference.source_start_offset != root.next_offset
                || reference.through_atomic_position < root.through_atomic_position
            {
                return invalid("query run append is not contiguous");
            }
            let (next, split) = append_page(
                root.stream_root_hash,
                Some(root_child(root)?),
                reference,
                &mut load,
                &mut pages,
            )?;
            match split {
                None => next,
                Some(split) => {
                    let page = encode_page(QueryRunPage::Branch(vec![
                        child_summary(&next)?,
                        child_summary(&split)?,
                    ]))?;
                    pages.push(page.clone());
                    page
                }
            }
        }
    };
    let summary = child_summary(&child)?;
    let root = ProjectionQueryStreamRoot {
        stream_root_hash: child.hash,
        run_count: summary.run_count,
        first_sequence: summary.first_sequence,
        last_sequence: summary.last_sequence,
        source_start_offset: summary.source_start_offset,
        next_offset: summary.next_offset,
        through_atomic_position: summary.through_atomic_position,
    };
    root.validate_at(root.next_offset, root.through_atomic_position)?;
    Ok(PreparedQueryRunAppend { root, pages })
}

/// Lazily walk immutable mini-run references from newest to oldest. Pages are
/// loaded only along visited branches; callers may stop immediately by
/// returning an error from `visit`.
pub fn visit_query_runs_newest(
    root: ProjectionQueryStreamRoot,
    mut load: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    visit: &mut impl FnMut(QueryRunReference) -> Result<(), IndexError>,
) -> Result<(), IndexError> {
    root.validate_at(root.next_offset, root.through_atomic_position)?;
    if root.run_count == 0 {
        return Ok(());
    }
    visit_page_newest(
        root.stream_root_hash,
        Some(root_child(root)?),
        &mut load,
        visit,
    )
}

pub fn find_query_run_by_hash(
    root: ProjectionQueryStreamRoot,
    wanted: [u8; 32],
    mut load: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<Option<QueryRunReference>, IndexError> {
    let mut found = None;
    visit_query_runs_newest(root, |hash| load(hash), &mut |reference| {
        if reference.hash == wanted {
            found = Some(reference);
        }
        Ok(())
    })?;
    Ok(found)
}

/// Select one bounded, contiguous same-level window. The walk is newest-first
/// and stops once a complete window is found, retaining at most
/// `maximum_input_runs` references regardless of stream lifetime.
pub fn select_query_run_compaction(
    previous: ProjectionQueryStreamRoot,
    mut load: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    limits: QueryRunCompactionLimits,
) -> Result<Option<QueryRunCompactionPlan>, IndexError> {
    let limits = limits.validate()?;
    previous.validate_at(previous.next_offset, previous.through_atomic_position)?;
    if previous.run_count == 0 {
        return Ok(None);
    }
    let expected_root = root_child(previous)?;
    let mut level = None;
    let mut candidate = Vec::with_capacity(limits.maximum_input_runs);
    let mut selected = None;
    visit_page_newest_until(
        previous.stream_root_hash,
        Some(expected_root),
        &mut load,
        &mut |reference| {
            if level == Some(reference.level) {
                if candidate.len() < limits.maximum_input_runs {
                    candidate.push(reference);
                    return false;
                }
            } else {
                if candidate.len() >= limits.level_trigger {
                    selected = Some(std::mem::take(&mut candidate));
                    return true;
                }
                candidate.clear();
                level = Some(reference.level);
                candidate.push(reference);
                return false;
            }
            selected = Some(std::mem::take(&mut candidate));
            true
        },
    )?;
    let inputs_newest_first = match selected {
        Some(selected) => selected,
        None if candidate.len() >= limits.level_trigger => candidate,
        None => return Ok(None),
    };
    let newest = *inputs_newest_first.first().ok_or(IndexError::Integrity)?;
    let oldest = *inputs_newest_first.last().ok_or(IndexError::Integrity)?;
    let output_level = newest
        .level
        .checked_add(1)
        .ok_or(IndexError::OffsetOverflow)?;
    let plan = QueryRunCompactionPlan {
        stream_root_hash: previous.stream_root_hash,
        root_next_offset: previous.next_offset,
        root_through_atomic_position: previous.through_atomic_position,
        inputs_newest_first,
        output_level,
        source_start_offset: oldest.source_start_offset,
        next_offset: newest.next_offset,
        through_atomic_position: newest.through_atomic_position,
    };
    validate_compaction_plan(&plan, limits)?;
    Ok(Some(plan))
}

/// Replace a selected whole-run window with the already merged immutable run.
/// The output occupies the newest selected sequence, so future level-zero
/// appends retain their monotonic sequence without renumbering history.
pub fn splice_compacted_query_runs(
    previous: ProjectionQueryStreamRoot,
    plan: &QueryRunCompactionPlan,
    output: QueryRunReference,
    mut load: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<PreparedQueryRunSplice, IndexError> {
    previous.validate_at(previous.next_offset, previous.through_atomic_position)?;
    let limits = QueryRunCompactionLimits {
        level_trigger: 2,
        maximum_input_runs: QUERY_RUN_PAGE_FANOUT,
    };
    validate_compaction_plan(plan, limits)?;
    if previous.stream_root_hash != plan.stream_root_hash
        || previous.next_offset != plan.root_next_offset
        || previous.through_atomic_position != plan.root_through_atomic_position
    {
        return Err(IndexError::Integrity);
    }
    let newest = *plan
        .inputs_newest_first
        .first()
        .ok_or(IndexError::Integrity)?;
    validate_reference(output)?;
    if output.sequence != newest.sequence
        || output.level != plan.output_level
        || output.source_start_offset != plan.source_start_offset
        || output.next_offset != plan.next_offset
        || output.through_atomic_position != plan.through_atomic_position
    {
        return invalid("compacted query run does not exactly cover its inputs");
    }
    let selected = plan
        .inputs_newest_first
        .iter()
        .map(|reference| (reference.sequence, *reference))
        .collect::<BTreeMap<_, _>>();
    if selected.len() != plan.inputs_newest_first.len() {
        return invalid("query run compaction inputs are not unique");
    }
    let mut pages = Vec::new();
    let mut matched = 0usize;
    let rewritten = splice_subtree(
        previous.stream_root_hash,
        Some(root_child(previous)?),
        &selected,
        output,
        &mut load,
        &mut pages,
        &mut matched,
    )?
    .ok_or(IndexError::Integrity)?;
    if matched != selected.len() {
        return Err(IndexError::Integrity);
    }
    let summary = child_summary(&rewritten)?;
    let expected_count = previous
        .run_count
        .checked_sub(selected.len() as u64)
        .and_then(|count| count.checked_add(1))
        .ok_or(IndexError::OffsetOverflow)?;
    if summary.run_count != expected_count
        || summary.next_offset != previous.next_offset
        || summary.through_atomic_position != previous.through_atomic_position
        || summary.source_start_offset != previous.source_start_offset
    {
        return Err(IndexError::Integrity);
    }
    let root = ProjectionQueryStreamRoot {
        stream_root_hash: rewritten.hash,
        run_count: summary.run_count,
        first_sequence: summary.first_sequence,
        last_sequence: summary.last_sequence,
        source_start_offset: summary.source_start_offset,
        next_offset: summary.next_offset,
        through_atomic_position: summary.through_atomic_position,
    };
    root.validate_at(previous.next_offset, previous.through_atomic_position)?;
    Ok(PreparedQueryRunSplice { root, pages })
}

fn visit_page_newest_until(
    hash: [u8; 32],
    expected: Option<QueryRunChild>,
    load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    visit: &mut impl FnMut(QueryRunReference) -> bool,
) -> Result<bool, IndexError> {
    let (page, actual) = load_page_with_summary(hash, load)?;
    if expected.is_some_and(|expected| expected != actual) {
        return Err(IndexError::Integrity);
    }
    match page {
        QueryRunPage::Leaf(runs) => {
            for run in runs.into_iter().rev() {
                if visit(run) {
                    return Ok(true);
                }
            }
        }
        QueryRunPage::Branch(children) => {
            for child in children.into_iter().rev() {
                if visit_page_newest_until(child.hash, Some(child), load, visit)? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn visit_page_newest(
    hash: [u8; 32],
    expected: Option<QueryRunChild>,
    load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    visit: &mut impl FnMut(QueryRunReference) -> Result<(), IndexError>,
) -> Result<(), IndexError> {
    let (page, actual) = load_page_with_summary(hash, load)?;
    if expected.is_some_and(|expected| expected != actual) {
        return Err(IndexError::Integrity);
    }
    match page {
        QueryRunPage::Leaf(runs) => {
            for run in runs.into_iter().rev() {
                visit(run)?;
            }
        }
        QueryRunPage::Branch(children) => {
            for child in children.into_iter().rev() {
                visit_page_newest(child.hash, Some(child), load, visit)?;
            }
        }
    }
    Ok(())
}

fn validate_compaction_plan(
    plan: &QueryRunCompactionPlan,
    limits: QueryRunCompactionLimits,
) -> Result<(), IndexError> {
    let limits = limits.validate()?;
    if plan.stream_root_hash == [0; 32]
        || plan.inputs_newest_first.len() < limits.level_trigger
        || plan.inputs_newest_first.len() > limits.maximum_input_runs
    {
        return invalid("query run compaction plan has invalid fan-in");
    }
    let newest = *plan
        .inputs_newest_first
        .first()
        .ok_or(IndexError::Integrity)?;
    let oldest = *plan
        .inputs_newest_first
        .last()
        .ok_or(IndexError::Integrity)?;
    for reference in &plan.inputs_newest_first {
        validate_reference(*reference)?;
    }
    if plan.inputs_newest_first.windows(2).any(|pair| {
        pair[0].sequence <= pair[1].sequence
            || pair[1].next_offset != pair[0].source_start_offset
            || pair[0].through_atomic_position < pair[1].through_atomic_position
            || pair[0].level != pair[1].level
    }) || plan.output_level != newest.level.checked_add(1).unwrap_or(0)
        || plan.source_start_offset != oldest.source_start_offset
        || plan.next_offset != newest.next_offset
        || plan.through_atomic_position != newest.through_atomic_position
        || plan.root_next_offset < plan.next_offset
        || plan.root_through_atomic_position < plan.through_atomic_position
    {
        return invalid("query run compaction plan coverage is invalid");
    }
    Ok(())
}

fn splice_subtree(
    hash: [u8; 32],
    expected: Option<QueryRunChild>,
    selected: &BTreeMap<u64, QueryRunReference>,
    output: QueryRunReference,
    load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    pages: &mut Vec<EncodedQueryRunPage>,
    matched: &mut usize,
) -> Result<Option<EncodedQueryRunPage>, IndexError> {
    let (page, actual) = load_page_with_summary(hash, load)?;
    if expected.is_some_and(|expected| expected != actual) {
        return Err(IndexError::Integrity);
    }
    let next = match page {
        QueryRunPage::Leaf(runs) => {
            let mut next = Vec::with_capacity(runs.len());
            for reference in runs {
                match selected.get(&reference.sequence) {
                    None => next.push(reference),
                    Some(expected) if *expected == reference => {
                        *matched = matched.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
                        if reference.sequence == output.sequence {
                            next.push(output);
                        }
                    }
                    Some(_) => return Err(IndexError::Integrity),
                }
            }
            if next.is_empty() {
                return Ok(None);
            }
            QueryRunPage::Leaf(next)
        }
        QueryRunPage::Branch(children) => {
            let mut next = Vec::with_capacity(children.len());
            for child in children {
                let selected_here = selected
                    .range(child.first_sequence..=child.last_sequence)
                    .next()
                    .is_some();
                if !selected_here {
                    next.push(child);
                    continue;
                }
                if let Some(rewritten) = splice_subtree(
                    child.hash,
                    Some(child),
                    selected,
                    output,
                    load,
                    pages,
                    matched,
                )? {
                    next.push(child_summary(&rewritten)?);
                }
            }
            if next.is_empty() {
                return Ok(None);
            }
            QueryRunPage::Branch(next)
        }
    };
    let encoded = encode_page(next)?;
    pages.push(encoded.clone());
    Ok(Some(encoded))
}

fn load_page_with_summary(
    hash: [u8; 32],
    load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<(QueryRunPage, QueryRunChild), IndexError> {
    let bytes = load(hash)?;
    if *blake3::hash(&bytes).as_bytes() != hash {
        return Err(IndexError::Integrity);
    }
    let page = decode_query_run_page(&bytes)?;
    let summary = child_summary(&EncodedQueryRunPage { hash, bytes })?;
    Ok((page, summary))
}

fn root_child(root: ProjectionQueryStreamRoot) -> Result<QueryRunChild, IndexError> {
    root.validate_at(root.next_offset, root.through_atomic_position)?;
    if root.run_count == 0 {
        return invalid("an empty query stream has no page root");
    }
    Ok(QueryRunChild {
        hash: root.stream_root_hash,
        run_count: root.run_count,
        first_sequence: root.first_sequence,
        last_sequence: root.last_sequence,
        source_start_offset: root.source_start_offset,
        next_offset: root.next_offset,
        through_atomic_position: root.through_atomic_position,
    })
}

fn append_page(
    hash: [u8; 32],
    expected: Option<QueryRunChild>,
    reference: QueryRunReference,
    load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    pages: &mut Vec<EncodedQueryRunPage>,
) -> Result<(EncodedQueryRunPage, Option<EncodedQueryRunPage>), IndexError> {
    let (page, actual) = load_page_with_summary(hash, load)?;
    if expected.is_some_and(|expected| expected != actual) {
        return Err(IndexError::Integrity);
    }
    match page {
        QueryRunPage::Leaf(mut runs) => {
            runs.push(reference);
            if runs.len() <= QUERY_RUN_PAGE_FANOUT {
                let page = encode_page(QueryRunPage::Leaf(runs))?;
                pages.push(page.clone());
                Ok((page, None))
            } else {
                let right = runs.split_off(QUERY_RUN_PAGE_FANOUT / 2);
                let left = encode_page(QueryRunPage::Leaf(runs))?;
                let right = encode_page(QueryRunPage::Leaf(right))?;
                pages.push(left.clone());
                pages.push(right.clone());
                Ok((left, Some(right)))
            }
        }
        QueryRunPage::Branch(mut children) => {
            let last = children.pop().ok_or(IndexError::Integrity)?;
            let (next, split) = append_page(last.hash, Some(last), reference, load, pages)?;
            children.push(child_summary(&next)?);
            if let Some(split) = split {
                children.push(child_summary(&split)?);
            }
            if children.len() <= QUERY_RUN_PAGE_FANOUT {
                let page = encode_page(QueryRunPage::Branch(children))?;
                pages.push(page.clone());
                Ok((page, None))
            } else {
                let right = children.split_off(QUERY_RUN_PAGE_FANOUT / 2);
                let left = encode_page(QueryRunPage::Branch(children))?;
                let right = encode_page(QueryRunPage::Branch(right))?;
                pages.push(left.clone());
                pages.push(right.clone());
                Ok((left, Some(right)))
            }
        }
    }
}

pub fn encode_query_run_page(page: QueryRunPage) -> Result<EncodedQueryRunPage, IndexError> {
    encode_page(page)
}
pub fn decode_query_run_page(bytes: &[u8]) -> Result<QueryRunPage, IndexError> {
    let payload = verify(bytes)?;
    let mut d = D {
        bytes: payload,
        at: 0,
    };
    if d.take(8)? != MAGIC || d.u16()? != FORMAT {
        return Err(IndexError::InvalidFormat("query run page"));
    }
    let kind = d.byte()?;
    let count = d.u16()? as usize;
    if count == 0 || count > QUERY_RUN_PAGE_FANOUT {
        return Err(IndexError::InvalidFormat("query run page count"));
    }
    let page = match kind {
        1 => QueryRunPage::Leaf(
            (0..count)
                .map(|_| d.reference())
                .collect::<Result<_, _>>()?,
        ),
        2 => QueryRunPage::Branch((0..count).map(|_| d.child()).collect::<Result<_, _>>()?),
        _ => return Err(IndexError::InvalidFormat("query run page kind")),
    };
    if d.at != d.bytes.len() {
        return Err(IndexError::InvalidFormat("query run page trailing"));
    }
    validate_page(&page)?;
    Ok(page)
}

fn encode_page(page: QueryRunPage) -> Result<EncodedQueryRunPage, IndexError> {
    validate_page(&page)?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&FORMAT.to_be_bytes());
    match &page {
        QueryRunPage::Leaf(runs) => {
            bytes.push(1);
            bytes.extend_from_slice(&(runs.len() as u16).to_be_bytes());
            for run in runs {
                put_reference(&mut bytes, *run);
            }
        }
        QueryRunPage::Branch(children) => {
            bytes.push(2);
            bytes.extend_from_slice(&(children.len() as u16).to_be_bytes());
            for child in children {
                put_child(&mut bytes, *child);
            }
        }
    }
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    Ok(EncodedQueryRunPage {
        hash: *blake3::hash(&bytes).as_bytes(),
        bytes,
    })
}

fn child_summary(page: &EncodedQueryRunPage) -> Result<QueryRunChild, IndexError> {
    let hash = page.hash;
    match decode_query_run_page(&page.bytes)? {
        QueryRunPage::Leaf(runs) => {
            let first = runs.first().unwrap();
            let last = runs.last().unwrap();
            Ok(QueryRunChild {
                hash,
                run_count: runs.len() as u64,
                first_sequence: first.sequence,
                last_sequence: last.sequence,
                source_start_offset: first.source_start_offset,
                next_offset: last.next_offset,
                through_atomic_position: last.through_atomic_position,
            })
        }
        QueryRunPage::Branch(children) => {
            let first = children.first().unwrap();
            let last = children.last().unwrap();
            Ok(QueryRunChild {
                hash,
                run_count: children
                    .iter()
                    .try_fold(0u64, |n, c| n.checked_add(c.run_count))
                    .ok_or(IndexError::OffsetOverflow)?,
                first_sequence: first.first_sequence,
                last_sequence: last.last_sequence,
                source_start_offset: first.source_start_offset,
                next_offset: last.next_offset,
                through_atomic_position: last.through_atomic_position,
            })
        }
    }
}
fn validate_page(page: &QueryRunPage) -> Result<(), IndexError> {
    match page {
        QueryRunPage::Leaf(r) => {
            if r.is_empty()
                || r.len() > QUERY_RUN_PAGE_FANOUT
                || r.windows(2).any(|p| {
                    p[0].sequence >= p[1].sequence
                        || p[0].next_offset != p[1].source_start_offset
                        || p[0].through_atomic_position > p[1].through_atomic_position
                })
            {
                return invalid("query runs unsorted");
            }
            for r in r {
                validate_reference(*r)?;
            }
        }
        QueryRunPage::Branch(c) => {
            if c.is_empty()
                || c.len() > QUERY_RUN_PAGE_FANOUT
                || c.windows(2).any(|p| {
                    p[0].last_sequence >= p[1].first_sequence
                        || p[0].next_offset != p[1].source_start_offset
                        || p[0].through_atomic_position > p[1].through_atomic_position
                })
            {
                return invalid("query children unsorted");
            }
            if c.iter().any(|c| {
                c.hash == [0; 32]
                    || c.run_count == 0
                    || c.first_sequence == 0
                    || c.first_sequence > c.last_sequence
                    || c.source_start_offset >= c.next_offset
            }) {
                return invalid("query child invalid");
            }
        }
    };
    Ok(())
}
fn validate_reference(r: QueryRunReference) -> Result<(), IndexError> {
    if r.hash == [0; 32] || r.sequence == 0 || r.source_start_offset >= r.next_offset {
        invalid("query run invalid")
    } else {
        Ok(())
    }
}
fn put_reference(out: &mut Vec<u8>, r: QueryRunReference) {
    out.extend_from_slice(&r.hash);
    out.extend_from_slice(&r.sequence.to_be_bytes());
    out.push(r.level);
    for n in [
        r.source_start_offset,
        r.next_offset,
        r.through_atomic_position,
    ] {
        out.extend_from_slice(&n.to_be_bytes())
    }
}
fn put_child(out: &mut Vec<u8>, c: QueryRunChild) {
    out.extend_from_slice(&c.hash);
    for n in [
        c.run_count,
        c.first_sequence,
        c.last_sequence,
        c.source_start_offset,
        c.next_offset,
        c.through_atomic_position,
    ] {
        out.extend_from_slice(&n.to_be_bytes())
    }
}
fn verify(bytes: &[u8]) -> Result<&[u8], IndexError> {
    let n = bytes.len().checked_sub(32).ok_or(IndexError::Integrity)?;
    let (p, h) = bytes.split_at(n);
    if blake3::hash(p).as_bytes() != h {
        Err(IndexError::Integrity)
    } else {
        Ok(p)
    }
}
fn invalid<T>(s: &str) -> Result<T, IndexError> {
    Err(IndexError::InvalidDefinition(s.into()))
}
struct D<'a> {
    bytes: &'a [u8],
    at: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap()
    }

    #[test]
    fn right_spine_append_passes_fanout_without_rewriting_history() {
        let mut store = BTreeMap::new();
        let mut root = None;
        for sequence in 1..=(QUERY_RUN_PAGE_FANOUT as u64 + 3) {
            let reads = std::cell::Cell::new(0usize);
            let prepared = append_query_run_path_copy(
                root,
                partition(),
                [6; 32],
                QueryRunReference {
                    hash: [sequence as u8; 32],
                    sequence,
                    level: 0,
                    source_start_offset: sequence - 1,
                    next_offset: sequence,
                    through_atomic_position: sequence,
                },
                |hash| {
                    reads.set(reads.get() + 1);
                    store.get(&hash).cloned().ok_or(IndexError::Integrity)
                },
            )
            .unwrap();
            assert!(reads.get() <= 2);
            assert!(prepared.pages.len() <= 3);
            for page in prepared.pages {
                store.insert(page.hash, page.bytes);
            }
            root = Some(prepared.root);
        }
        let root = root.unwrap();
        assert_eq!(root.run_count, QUERY_RUN_PAGE_FANOUT as u64 + 3);
        let mut seen = Vec::new();
        visit_query_runs_newest(
            root,
            |hash| store.get(&hash).cloned().ok_or(IndexError::Integrity),
            &mut |run| {
                seen.push(run.sequence);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(seen.first(), Some(&(QUERY_RUN_PAGE_FANOUT as u64 + 3)));
        assert_eq!(seen.last(), Some(&1));
    }

    #[test]
    fn page_codec_preserves_level_and_rejects_unbounded_pages() {
        let reference = QueryRunReference {
            hash: [9; 32],
            sequence: 7,
            level: 3,
            source_start_offset: 4,
            next_offset: 8,
            through_atomic_position: 11,
        };
        let encoded = encode_query_run_page(QueryRunPage::Leaf(vec![reference])).unwrap();
        assert_eq!(
            decode_query_run_page(&encoded.bytes).unwrap(),
            QueryRunPage::Leaf(vec![reference])
        );
        assert!(encode_query_run_page(QueryRunPage::Leaf(Vec::new())).is_err());
        assert!(
            encode_query_run_page(QueryRunPage::Leaf(vec![
                reference;
                QUERY_RUN_PAGE_FANOUT + 1
            ]))
            .is_err()
        );
    }

    #[test]
    fn empty_generation_root_accepts_its_first_run_without_a_page() {
        let empty = ProjectionQueryStreamRoot::empty(partition(), [6; 32], 41, 12).unwrap();
        let reference = QueryRunReference {
            hash: [9; 32],
            sequence: 1,
            level: 0,
            source_start_offset: 41,
            next_offset: 42,
            through_atomic_position: 12,
        };
        let prepared =
            append_query_run_path_copy(Some(empty), partition(), [6; 32], reference, |_| {
                panic!("an empty root has no page to load")
            })
            .unwrap();
        assert_eq!(prepared.root.run_count, 1);
        assert_eq!(prepared.root.source_start_offset, 41);
        assert!(
            select_query_run_compaction(
                empty,
                |_| panic!("an empty root has no page to load"),
                QueryRunCompactionLimits {
                    level_trigger: 2,
                    maximum_input_runs: 4,
                },
            )
            .unwrap()
            .is_none()
        );
    }

    fn build_stream(count: u64) -> (ProjectionQueryStreamRoot, BTreeMap<[u8; 32], Vec<u8>>) {
        let mut store = BTreeMap::new();
        let mut root = None;
        for sequence in 1..=count {
            let prepared = append_query_run_path_copy(
                root,
                partition(),
                [6; 32],
                QueryRunReference {
                    hash: [sequence as u8; 32],
                    sequence,
                    level: 0,
                    source_start_offset: sequence - 1,
                    next_offset: sequence,
                    through_atomic_position: sequence,
                },
                |hash| store.get(&hash).cloned().ok_or(IndexError::Integrity),
            )
            .unwrap();
            for page in prepared.pages {
                store.insert(page.hash, page.bytes);
            }
            root = Some(prepared.root);
        }
        (root.unwrap(), store)
    }

    #[test]
    fn compaction_splices_a_bounded_window_across_fanout() {
        let count = QUERY_RUN_PAGE_FANOUT as u64 + 20;
        let (root, mut store) = build_stream(count);
        let limits = QueryRunCompactionLimits {
            level_trigger: 100,
            maximum_input_runs: 100,
        };
        let plan = select_query_run_compaction(
            root,
            |hash| store.get(&hash).cloned().ok_or(IndexError::Integrity),
            limits,
        )
        .unwrap()
        .unwrap();
        assert_eq!(plan.inputs_newest_first().len(), 100);
        assert_eq!(plan.inputs_newest_first()[0].sequence, count);
        assert_eq!(plan.inputs_newest_first()[99].sequence, count - 99);
        let output = QueryRunReference {
            hash: [249; 32],
            sequence: count,
            level: plan.output_level(),
            source_start_offset: plan.source_start_offset(),
            next_offset: plan.next_offset(),
            through_atomic_position: plan.through_atomic_position(),
        };
        let spliced = splice_compacted_query_runs(root, &plan, output, |hash| {
            store.get(&hash).cloned().ok_or(IndexError::Integrity)
        })
        .unwrap();
        assert_eq!(spliced.root.run_count, count - 99);
        assert!(
            spliced.pages.len() <= 3,
            "only two leaves and their root change"
        );
        for page in spliced.pages {
            store.insert(page.hash, page.bytes);
        }
        let mut seen = Vec::new();
        visit_query_runs_newest(
            spliced.root,
            |hash| store.get(&hash).cloned().ok_or(IndexError::Integrity),
            &mut |reference| {
                seen.push(reference);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(seen[0], output);
        assert_eq!(seen.last().unwrap().sequence, 1);
        assert_eq!(seen.len() as u64, count - 99);

        let appended = append_query_run_path_copy(
            Some(spliced.root),
            partition(),
            [6; 32],
            QueryRunReference {
                hash: [250; 32],
                sequence: count + 1,
                level: 0,
                source_start_offset: count,
                next_offset: count + 1,
                through_atomic_position: count + 1,
            },
            |hash| store.get(&hash).cloned().ok_or(IndexError::Integrity),
        )
        .unwrap();
        assert_eq!(appended.root.last_sequence, count + 1);
    }

    #[test]
    fn compaction_rejects_coverage_atomic_and_root_mismatches() {
        let (root, store) = build_stream(8);
        let plan = select_query_run_compaction(
            root,
            |hash| store.get(&hash).cloned().ok_or(IndexError::Integrity),
            QueryRunCompactionLimits {
                level_trigger: 4,
                maximum_input_runs: 4,
            },
        )
        .unwrap()
        .unwrap();
        let valid = QueryRunReference {
            hash: [240; 32],
            sequence: 8,
            level: 1,
            source_start_offset: 4,
            next_offset: 8,
            through_atomic_position: 8,
        };
        let mut wrong_coverage = valid;
        wrong_coverage.source_start_offset = 5;
        assert!(
            splice_compacted_query_runs(root, &plan, wrong_coverage, |hash| {
                store.get(&hash).cloned().ok_or(IndexError::Integrity)
            })
            .is_err()
        );
        let mut wrong_atomic = valid;
        wrong_atomic.through_atomic_position = 7;
        assert!(
            splice_compacted_query_runs(root, &plan, wrong_atomic, |hash| {
                store.get(&hash).cloned().ok_or(IndexError::Integrity)
            })
            .is_err()
        );

        let mut false_root = root;
        false_root.run_count += 1;
        assert!(matches!(
            select_query_run_compaction(
                false_root,
                |hash| store.get(&hash).cloned().ok_or(IndexError::Integrity),
                QueryRunCompactionLimits {
                    level_trigger: 4,
                    maximum_input_runs: 4,
                },
            ),
            Err(IndexError::Integrity)
        ));
        let mut false_cut = root;
        false_cut.through_atomic_position += 1;
        assert!(
            splice_compacted_query_runs(false_cut, &plan, valid, |hash| {
                store.get(&hash).cloned().ok_or(IndexError::Integrity)
            })
            .is_err()
        );
    }
}
impl<'a> D<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], IndexError> {
        let e = self.at.checked_add(n).ok_or(IndexError::OffsetOverflow)?;
        let v = self.bytes.get(self.at..e).ok_or(IndexError::Integrity)?;
        self.at = e;
        Ok(v)
    }
    fn byte(&mut self) -> Result<u8, IndexError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, IndexError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        ))
    }
    fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        ))
    }
    fn a32(&mut self) -> Result<[u8; 32], IndexError> {
        self.take(32)?.try_into().map_err(|_| IndexError::Integrity)
    }
    fn reference(&mut self) -> Result<QueryRunReference, IndexError> {
        Ok(QueryRunReference {
            hash: self.a32()?,
            sequence: self.u64()?,
            level: self.byte()?,
            source_start_offset: self.u64()?,
            next_offset: self.u64()?,
            through_atomic_position: self.u64()?,
        })
    }
    fn child(&mut self) -> Result<QueryRunChild, IndexError> {
        Ok(QueryRunChild {
            hash: self.a32()?,
            run_count: self.u64()?,
            first_sequence: self.u64()?,
            last_sequence: self.u64()?,
            source_start_offset: self.u64()?,
            next_offset: self.u64()?,
            through_atomic_position: self.u64()?,
        })
    }
}
