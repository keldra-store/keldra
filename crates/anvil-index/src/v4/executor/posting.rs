use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::future::Future;
use std::pin::Pin;

use crate::IndexError;

use super::super::postings::DecodedPostingBlock;
use super::super::{
    ArtifactDescriptor, ArtifactDirectoryRead, ComponentKind, ComponentStream, DocId, FieldId,
    NativeQueryStatisticsRecorder, PostingImpact, PostingReference, SegmentDescriptor, StreamLeaf,
    TermDictionary, component_ordinal_key, read_artifact_component,
};

type CursorFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<DocId>, IndexError>> + Send + 'a>>;

/// Maximum sorted document batch retained by one dictionary-range cursor.
/// Dictionary leaves and posting blocks are streamed one at a time; this is
/// the only state whose size does not already have a format-level component
/// bound.
const TERM_RANGE_DOCUMENT_BATCH: usize = 256;

#[derive(Clone)]
pub(super) struct TermBounds {
    minimum: Vec<u8>,
    minimum_inclusive: bool,
    maximum: Vec<u8>,
    maximum_inclusive: bool,
}

impl TermBounds {
    pub(super) fn new(
        minimum: Vec<u8>,
        minimum_inclusive: bool,
        maximum: Vec<u8>,
        maximum_inclusive: bool,
    ) -> Result<Self, IndexError> {
        if minimum.is_empty() || maximum.is_empty() || minimum > maximum {
            return Err(IndexError::InvalidQuery(
                "term range bounds are empty or reversed".into(),
            ));
        }
        Ok(Self {
            minimum,
            minimum_inclusive,
            maximum,
            maximum_inclusive,
        })
    }

    fn contains(&self, term: &[u8]) -> bool {
        (term > self.minimum.as_slice()
            || self.minimum_inclusive && term == self.minimum.as_slice())
            && (term < self.maximum.as_slice()
                || self.maximum_inclusive && term == self.maximum.as_slice())
    }
}

pub(super) struct PostingStream<'a, D> {
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    root: ArtifactDescriptor,
    stream: ComponentStream<'a, D>,
    next_ordinal: u32,
    end_ordinal: u32,
    block: Option<DecodedPostingBlock>,
    position: usize,
    current: Option<DocId>,
    resume_target: Option<DocId>,
    estimated_documents: u64,
    statistics: NativeQueryStatisticsRecorder,
}

impl<'a, D: ArtifactDirectoryRead> PostingStream<'a, D> {
    pub(super) fn new(
        directory: &'a D,
        segment: &'a SegmentDescriptor,
        field_id: FieldId,
        reference: PostingReference,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Result<Self, IndexError> {
        let root = component_root(segment, ComponentKind::POSTINGS, Some(field_id))?;
        let end_ordinal = reference
            .first_component_ordinal
            .checked_add(reference.component_count)
            .ok_or(IndexError::OffsetOverflow)?;
        let last = end_ordinal
            .checked_sub(1)
            .ok_or_else(|| IndexError::InvalidQuery("empty posting reference".into()))?;
        let stream = ComponentStream::new(
            directory,
            segment.identity,
            ComponentKind::POSTINGS,
            root.clone(),
            Some(component_ordinal_key(reference.first_component_ordinal).to_vec()),
            Some(component_ordinal_key(last).to_vec()),
        )?;
        Ok(Self {
            directory,
            segment,
            root,
            stream,
            next_ordinal: reference.first_component_ordinal,
            end_ordinal,
            block: None,
            position: 0,
            current: None,
            resume_target: None,
            estimated_documents: reference.document_frequency,
            statistics,
        })
    }

    pub(super) fn current_frequency(&self) -> Option<u32> {
        let block = self.block.as_ref()?;
        let index = self.position.checked_sub(1)?;
        block.frequency(index)
    }

    pub(super) fn current_component_ordinal(&self) -> Option<u32> {
        self.block.as_ref()?;
        self.next_ordinal.checked_sub(1)
    }

    /// Return the conservative scoring inputs which remain valid through the
    /// end of the posting block containing the first occurrence at or after
    /// `target`. `None` means that this term is exhausted for the segment.
    pub(super) async fn impact_window(
        &mut self,
        target: DocId,
    ) -> Result<Option<(PostingImpact, DocId)>, IndexError> {
        if self.advance(target).await?.is_none() {
            return Ok(None);
        }
        let block = self.block.as_ref().ok_or(IndexError::InvalidFormat(
            "full-text posting impact has no decoded block",
        ))?;
        let impact = required_impact(block)?;
        Ok(Some((impact, block.last_doc_id())))
    }

    async fn load_next_block(&mut self) -> Result<bool, IndexError> {
        if self.next_ordinal == self.end_ordinal {
            if self.stream.next_leaf().await?.is_some() {
                return Err(IndexError::InvalidFormat(
                    "posting reference resolved beyond its component count",
                ));
            }
            self.block = None;
            return Ok(false);
        }
        let leaf = self
            .stream
            .next_leaf()
            .await?
            .ok_or(IndexError::InvalidFormat(
                "posting reference ended before its component count",
            ))?;
        validate_ordinal_leaf(&leaf, self.next_ordinal)?;
        let loaded = read_artifact_component(
            self.directory,
            self.segment.identity,
            &leaf.descriptor,
            ComponentKind::POSTINGS,
        )
        .await?;
        self.statistics.posting_block_sought(
            u64::try_from(loaded.payload.len()).map_err(|_| IndexError::OffsetOverflow)?,
        );
        self.block = Some(
            self.directory
                .run_query_cpu(move || DecodedPostingBlock::decode_payload(&loaded.payload))
                .await?,
        );
        self.statistics.posting_block_decoded();
        self.position = 0;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(true)
    }

    pub(super) async fn next(&mut self) -> Result<Option<DocId>, IndexError> {
        if let Some(target) = self.resume_target.take() {
            return self.advance(target).await;
        }
        loop {
            if let Some(block) = &self.block
                && let Some(value) = block.doc_id(self.position)
            {
                self.position += 1;
                self.current = Some(value);
                return Ok(Some(value));
            }
            if !self.load_next_block().await? {
                self.current = None;
                return Ok(None);
            }
        }
    }

    pub(super) async fn advance(&mut self, target: DocId) -> Result<Option<DocId>, IndexError> {
        self.statistics.posting_advance();
        let target = self
            .resume_target
            .take()
            .map_or(target, |resume| resume.max(target));
        if self.current.is_some_and(|current| current >= target) {
            return Ok(self.current);
        }
        loop {
            if let Some(block) = &self.block {
                if block.last_doc_id() >= target {
                    if let Some((ordinal, value)) = block.lower_bound_from(self.position, target) {
                        self.position = ordinal + 1;
                        self.current = Some(value);
                        return Ok(Some(value));
                    }
                } else {
                    self.statistics.posting_block_skipped();
                }
            }
            if !self.load_next_block().await? {
                self.current = None;
                return Ok(None);
            }
        }
    }

    /// Drop the decoded posting block while retaining only the immutable
    /// component locator and the next logical DocId. Physical k-way merges use
    /// this after extracting one segment head so thousands of segments do not
    /// multiply the decode ceiling.
    pub(super) fn release_decoded(&mut self) -> Result<(), IndexError> {
        let Some(current) = self.current.take() else {
            self.block = None;
            self.position = 0;
            return Ok(());
        };
        let ordinal = self
            .current_component_ordinal()
            .ok_or(IndexError::InvalidFormat(
                "posting cursor has no component ordinal",
            ))?;
        let last = self
            .end_ordinal
            .checked_sub(1)
            .ok_or(IndexError::InvalidFormat("empty posting reference"))?;
        self.stream = ComponentStream::new(
            self.directory,
            self.segment.identity,
            ComponentKind::POSTINGS,
            self.root.clone(),
            Some(component_ordinal_key(ordinal).to_vec()),
            Some(component_ordinal_key(last).to_vec()),
        )?;
        self.next_ordinal = ordinal;
        self.block = None;
        self.position = 0;
        self.resume_target = Some(current.checked_next()?);
        Ok(())
    }
}

fn required_impact(block: &DecodedPostingBlock) -> Result<PostingImpact, IndexError> {
    block.impact().ok_or(IndexError::InvalidFormat(
        "full-text posting block has no impact bound",
    ))
}

/// Exact union of every posting whose term falls within one ordered dictionary
/// range. Only one dictionary leaf, one posting block, and a fixed DocId batch
/// are resident at once. Reaching the batch boundary restarts the bounded
/// dictionary traversal strictly after the last emitted DocId.
pub(super) struct TermRangeStream<'a, D> {
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    field_id: FieldId,
    dictionary_root: ArtifactDescriptor,
    bounds: TermBounds,
    next_minimum: u32,
    batch: Vec<DocId>,
    position: usize,
    current: Option<DocId>,
    exhausted: bool,
    estimated_documents: u64,
    statistics: NativeQueryStatisticsRecorder,
}

impl<'a, D: ArtifactDirectoryRead> TermRangeStream<'a, D> {
    pub(super) fn new(
        directory: &'a D,
        segment: &'a SegmentDescriptor,
        field_id: FieldId,
        dictionary_root: ArtifactDescriptor,
        bounds: TermBounds,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Result<Self, IndexError> {
        // Validate the postings stream once. Individual posting references are
        // still checked when their bounded cursors are opened.
        component_root(segment, ComponentKind::POSTINGS, Some(field_id))?;
        Ok(Self {
            directory,
            segment,
            field_id,
            dictionary_root,
            bounds,
            next_minimum: 0,
            batch: Vec::new(),
            position: 0,
            current: None,
            exhausted: false,
            estimated_documents: u64::from(segment.document_count),
            statistics,
        })
    }

    async fn prepare_batch(&mut self) -> Result<(), IndexError> {
        self.batch.clear();
        self.position = 0;
        self.current = None;
        if self.exhausted || self.next_minimum >= self.segment.document_count {
            self.exhausted = true;
            return Ok(());
        }

        self.statistics.term_seek();
        let mut candidates = BTreeSet::new();
        let mut dictionaries = ComponentStream::new(
            self.directory,
            self.segment.identity,
            ComponentKind::TERM_DICTIONARY,
            self.dictionary_root.clone(),
            Some(self.bounds.minimum.clone()),
            Some(self.bounds.maximum.clone()),
        )?;
        while let Some(leaf) = dictionaries.next_leaf().await? {
            let loaded = read_artifact_component(
                self.directory,
                self.segment.identity,
                &leaf.descriptor,
                ComponentKind::TERM_DICTIONARY,
            )
            .await?;
            let dictionary = self
                .directory
                .run_query_cpu(move || TermDictionary::decode_payload(&loaded.payload))
                .await?;
            for entry in dictionary
                .entries()
                .iter()
                .filter(|entry| self.bounds.contains(&entry.term))
            {
                self.statistics.enumerated_terms(1);
                let mut postings = PostingStream::new(
                    self.directory,
                    self.segment,
                    self.field_id,
                    entry.postings,
                    self.statistics.clone(),
                )?;
                let mut candidate = postings.advance(DocId::new(self.next_minimum)).await?;
                while let Some(doc_id) = candidate {
                    if candidates.len() == TERM_RANGE_DOCUMENT_BATCH
                        && candidates.last().is_some_and(|largest| doc_id >= *largest)
                    {
                        // This posting and every value after it are ordered and
                        // cannot enter the globally smallest retained batch.
                        break;
                    }
                    candidates.insert(doc_id);
                    if candidates.len() > TERM_RANGE_DOCUMENT_BATCH {
                        candidates.pop_last();
                    }
                    candidate = postings.next().await?;
                }
            }
        }

        if candidates.is_empty() {
            self.exhausted = true;
            return Ok(());
        }
        self.batch.extend(candidates);
        self.next_minimum = self
            .batch
            .last()
            .copied()
            .ok_or(IndexError::InvalidFormat("empty term-range document batch"))?
            .checked_next()?
            .get();
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DocId>, IndexError> {
        if self.position == self.batch.len() {
            self.prepare_batch().await?;
        }
        let Some(value) = self.batch.get(self.position).copied() else {
            self.current = None;
            return Ok(None);
        };
        self.position += 1;
        self.current = Some(value);
        Ok(Some(value))
    }

    async fn advance(&mut self, target: DocId) -> Result<Option<DocId>, IndexError> {
        if self.current.is_some_and(|current| current >= target) {
            return Ok(self.current);
        }
        let remaining = &self.batch[self.position..];
        let offset = remaining.partition_point(|value| *value < target);
        if let Some(value) = remaining.get(offset).copied() {
            self.position += offset + 1;
            self.current = Some(value);
            return Ok(Some(value));
        }
        self.next_minimum = self.next_minimum.max(target.get());
        self.batch.clear();
        self.position = 0;
        self.current = None;
        self.next().await
    }
}

// Keeping cursor state inline avoids an allocation per Boolean iterator node.
#[allow(clippy::large_enum_variant)]
pub(super) enum DocCursor<'a, D> {
    Empty,
    All { next: u32, end: u32 },
    Posting(PostingStream<'a, D>),
    TermRange(TermRangeStream<'a, D>),
    And(AndCursor<'a, D>),
    Or(OrCursor<'a, D>),
    Not(NotCursor<'a, D>),
}

pub(super) struct AndCursor<'a, D> {
    children: Vec<DocCursor<'a, D>>,
    heads: Vec<Option<DocId>>,
    initialized: bool,
    emitted: Option<DocId>,
    estimated_documents: u64,
    statistics: NativeQueryStatisticsRecorder,
}

pub(super) struct OrCursor<'a, D> {
    children: Vec<DocCursor<'a, D>>,
    heads: BinaryHeap<Reverse<(DocId, usize)>>,
    initialized: bool,
    estimated_documents: u64,
    statistics: NativeQueryStatisticsRecorder,
}

pub(super) struct NotCursor<'a, D> {
    include: Box<DocCursor<'a, D>>,
    exclude: Box<DocCursor<'a, D>>,
    exclude_head: Option<DocId>,
    initialized: bool,
    estimated_documents: u64,
}

impl<'a, D: ArtifactDirectoryRead> DocCursor<'a, D> {
    pub(super) fn all(document_count: u32) -> Self {
        Self::All {
            next: 0,
            end: document_count,
        }
    }

    pub(super) fn and(mut children: Vec<Self>, statistics: NativeQueryStatisticsRecorder) -> Self {
        if children.is_empty() {
            return Self::Empty;
        }
        if children.len() == 1 {
            return children.pop().unwrap();
        }
        let original_costs = children
            .iter()
            .map(Self::estimated_cost)
            .collect::<Vec<_>>();
        children.sort_by_key(Self::estimated_cost);
        let chosen_costs = children
            .iter()
            .map(Self::estimated_cost)
            .collect::<Vec<_>>();
        statistics.planned_conjunction(&original_costs, &chosen_costs);
        let estimated_documents = chosen_costs[0];
        let heads = vec![None; children.len()];
        Self::And(AndCursor {
            children,
            heads,
            initialized: false,
            emitted: None,
            estimated_documents,
            statistics,
        })
    }

    pub(super) fn or(mut children: Vec<Self>, statistics: NativeQueryStatisticsRecorder) -> Self {
        children.retain(|child| !matches!(child, Self::Empty));
        if children.is_empty() {
            return Self::Empty;
        }
        if children.len() == 1 {
            return children.pop().unwrap();
        }
        let estimated_documents = children.iter().fold(0_u64, |total, child| {
            total.saturating_add(child.estimated_cost())
        });
        Self::Or(OrCursor {
            children,
            heads: BinaryHeap::new(),
            initialized: false,
            estimated_documents,
            statistics,
        })
    }

    pub(super) fn not(include: Self, exclude: Self) -> Self {
        let estimated_documents = include.estimated_cost();
        Self::Not(NotCursor {
            include: Box::new(include),
            exclude: Box::new(exclude),
            exclude_head: None,
            initialized: false,
            estimated_documents,
        })
    }

    fn estimated_cost(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::All { next, end } => u64::from(end.saturating_sub(*next)),
            Self::Posting(cursor) => cursor.estimated_documents,
            Self::TermRange(cursor) => cursor.estimated_documents,
            Self::And(cursor) => cursor.estimated_documents,
            Self::Or(cursor) => cursor.estimated_documents,
            Self::Not(cursor) => cursor.estimated_documents,
        }
    }

    pub(super) fn next(&mut self) -> CursorFuture<'_> {
        Box::pin(async move {
            match self {
                Self::Empty => Ok(None),
                Self::All { next, end } => {
                    if *next >= *end {
                        return Ok(None);
                    }
                    let value = DocId::new(*next);
                    *next = next.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
                    Ok(Some(value))
                }
                Self::Posting(cursor) => cursor.next().await,
                Self::TermRange(cursor) => cursor.next().await,
                Self::And(cursor) => cursor.next().await,
                Self::Or(cursor) => cursor.next().await,
                Self::Not(cursor) => cursor.next().await,
            }
        })
    }

    pub(super) fn advance(&mut self, target: DocId) -> CursorFuture<'_> {
        Box::pin(async move {
            match self {
                Self::Empty => Ok(None),
                Self::All { next, end } => {
                    *next = (*next).max(target.get());
                    if *next >= *end {
                        return Ok(None);
                    }
                    let value = DocId::new(*next);
                    *next = next.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
                    Ok(Some(value))
                }
                Self::Posting(cursor) => cursor.advance(target).await,
                Self::TermRange(cursor) => cursor.advance(target).await,
                Self::And(cursor) => cursor.advance(target).await,
                Self::Or(cursor) => cursor.advance(target).await,
                Self::Not(cursor) => cursor.advance(target).await,
            }
        })
    }

    pub(super) fn release_decoded(&mut self) -> Result<(), IndexError> {
        match self {
            Self::Empty | Self::All { .. } | Self::TermRange(_) => Ok(()),
            Self::Posting(cursor) => cursor.release_decoded(),
            Self::And(cursor) => cursor
                .children
                .iter_mut()
                .try_for_each(Self::release_decoded),
            Self::Or(cursor) => cursor
                .children
                .iter_mut()
                .try_for_each(Self::release_decoded),
            Self::Not(cursor) => {
                cursor.include.release_decoded()?;
                cursor.exclude.release_decoded()
            }
        }
    }
}

impl<'a, D: ArtifactDirectoryRead> AndCursor<'a, D> {
    async fn initialize(&mut self) -> Result<bool, IndexError> {
        if !self.initialized {
            for (head, child) in self.heads.iter_mut().zip(&mut self.children) {
                *head = child.next().await?;
            }
            self.initialized = true;
        }
        Ok(self.heads.iter().all(Option::is_some))
    }

    async fn align(&mut self) -> Result<Option<DocId>, IndexError> {
        loop {
            let Some(target) = self.heads.iter().flatten().copied().max() else {
                return Ok(None);
            };
            let mut moved = false;
            for (head, child) in self.heads.iter_mut().zip(&mut self.children) {
                if head.is_some_and(|value| value < target) {
                    self.statistics.conjunction_advance();
                    *head = child.advance(target).await?;
                    if head.is_none() {
                        return Ok(None);
                    }
                    moved = true;
                }
            }
            if !moved && self.heads.iter().all(|head| *head == Some(target)) {
                return Ok(Some(target));
            }
        }
    }

    async fn next(&mut self) -> Result<Option<DocId>, IndexError> {
        if !self.initialize().await? {
            return Ok(None);
        }
        if self.emitted.take().is_some() {
            self.heads[0] = self.children[0].next().await?;
            if self.heads[0].is_none() {
                return Ok(None);
            }
        }
        let value = self.align().await?;
        self.emitted = value;
        Ok(value)
    }

    async fn advance(&mut self, target: DocId) -> Result<Option<DocId>, IndexError> {
        if !self.initialize().await? {
            return Ok(None);
        }
        self.emitted = None;
        for (head, child) in self.heads.iter_mut().zip(&mut self.children) {
            if head.is_some_and(|value| value < target) {
                self.statistics.conjunction_advance();
                *head = child.advance(target).await?;
                if head.is_none() {
                    return Ok(None);
                }
            }
        }
        let value = self.align().await?;
        self.emitted = value;
        Ok(value)
    }
}

impl<'a, D: ArtifactDirectoryRead> OrCursor<'a, D> {
    async fn initialize(&mut self) -> Result<(), IndexError> {
        if !self.initialized {
            for (child_index, child) in self.children.iter_mut().enumerate() {
                if let Some(doc_id) = child.next().await? {
                    self.heads.push(Reverse((doc_id, child_index)));
                    self.statistics.union_heap_push();
                }
            }
            self.initialized = true;
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DocId>, IndexError> {
        self.initialize().await?;
        let Some(Reverse((value, child_index))) = self.heads.pop() else {
            return Ok(None);
        };
        self.statistics.union_heap_pop();
        if let Some(next) = self.children[child_index].next().await? {
            self.heads.push(Reverse((next, child_index)));
            self.statistics.union_heap_push();
        }
        while self
            .heads
            .peek()
            .is_some_and(|Reverse((head, _))| *head == value)
        {
            let Reverse((_, child_index)) = self
                .heads
                .pop()
                .ok_or(IndexError::InvalidFormat("union heap became empty"))?;
            self.statistics.union_heap_pop();
            if let Some(next) = self.children[child_index].next().await? {
                self.heads.push(Reverse((next, child_index)));
                self.statistics.union_heap_push();
            }
        }
        Ok(Some(value))
    }

    async fn advance(&mut self, target: DocId) -> Result<Option<DocId>, IndexError> {
        self.initialize().await?;
        while self
            .heads
            .peek()
            .is_some_and(|Reverse((head, _))| *head < target)
        {
            let Reverse((_, child_index)) = self
                .heads
                .pop()
                .ok_or(IndexError::InvalidFormat("union heap became empty"))?;
            self.statistics.union_heap_pop();
            if let Some(next) = self.children[child_index].advance(target).await? {
                self.heads.push(Reverse((next, child_index)));
                self.statistics.union_heap_push();
            }
        }
        self.next().await
    }
}

impl<'a, D: ArtifactDirectoryRead> NotCursor<'a, D> {
    async fn next(&mut self) -> Result<Option<DocId>, IndexError> {
        if !self.initialized {
            self.exclude_head = self.exclude.next().await?;
            self.initialized = true;
        }
        while let Some(candidate) = self.include.next().await? {
            if self.exclude_head.is_some_and(|value| value < candidate) {
                self.exclude_head = self.exclude.advance(candidate).await?;
            }
            if self.exclude_head != Some(candidate) {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    async fn advance(&mut self, target: DocId) -> Result<Option<DocId>, IndexError> {
        if !self.initialized {
            self.exclude_head = self.exclude.next().await?;
            self.initialized = true;
        }
        let Some(candidate) = self.include.advance(target).await? else {
            return Ok(None);
        };
        if self.exclude_head.is_some_and(|value| value < candidate) {
            self.exclude_head = self.exclude.advance(candidate).await?;
        }
        if self.exclude_head != Some(candidate) {
            return Ok(Some(candidate));
        }
        self.next().await
    }
}

pub(super) fn component_root(
    segment: &SegmentDescriptor,
    kind: ComponentKind,
    field_id: Option<FieldId>,
) -> Result<super::super::ArtifactDescriptor, IndexError> {
    segment
        .components
        .binary_search_by_key(&(kind, field_id, 0), |component| {
            (component.role, component.field_id, component.ordinal)
        })
        .ok()
        .map(|index| segment.components[index].artifact.clone())
        .ok_or(IndexError::InvalidFormat(
            "format-v4 segment lacks a required component stream",
        ))
}

fn validate_ordinal_leaf(leaf: &StreamLeaf, expected: u32) -> Result<(), IndexError> {
    let key = component_ordinal_key(expected);
    if leaf.minimum_key != key || leaf.maximum_key != key || leaf.element_count == 0 {
        return Err(IndexError::InvalidFormat(
            "posting stream leaf ordinal or count",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v4::PostingBlock;

    #[test]
    fn full_text_scoring_fails_closed_without_impact_data() {
        let payload = PostingBlock::new(vec![DocId::new(1)], None)
            .unwrap()
            .encode_payload()
            .unwrap();
        let decoded = DecodedPostingBlock::decode_payload(&payload).unwrap();
        assert!(matches!(
            required_impact(&decoded),
            Err(IndexError::InvalidFormat(
                "full-text posting block has no impact bound"
            ))
        ));
    }
}
