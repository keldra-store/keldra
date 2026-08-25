use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;

use crate::IndexError;

use super::super::postings::DecodedPostingBlock;
use super::super::{
    ArtifactDescriptor, ArtifactDirectoryRead, ComponentKind, ComponentStream, DocId,
    DocValueBlock, FieldId, NativeQueryStatisticsRecorder, PointBlock, PointValue, PositionsBlock,
    PostingImpact, PostingReference, RangeBound, SegmentDescriptor, StreamLeaf, TermDictionary,
    component_ordinal_key, read_artifact_component,
};

type CursorFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<DocId>, IndexError>> + Send + 'a>>;

/// One exact segment-local candidate set. Lucene's point and multi-term
/// queries likewise materialize matches once before exposing an ordered
/// DocId iterator. The caller charges these words from the query budget before
/// execution; total memory is independent of the number of matching values.
struct SegmentDocSet {
    words: Vec<u64>,
    document_count: u32,
}

impl SegmentDocSet {
    fn new(document_count: u32) -> Result<Self, IndexError> {
        let words = usize::try_from(document_count)
            .map_err(|_| IndexError::OffsetOverflow)?
            .div_ceil(u64::BITS as usize);
        Ok(Self {
            words: vec![0; words],
            document_count,
        })
    }

    fn insert(&mut self, doc_id: DocId) -> Result<(), IndexError> {
        if doc_id.get() >= self.document_count {
            return Err(IndexError::InvalidFormat(
                "candidate DocId exceeds its segment",
            ));
        }
        let ordinal = doc_id.get() as usize;
        self.words[ordinal / u64::BITS as usize] |= 1 << (ordinal % u64::BITS as usize);
        Ok(())
    }

    fn first_at_or_after(&self, target: u32) -> Option<DocId> {
        if target >= self.document_count {
            return None;
        }
        let ordinal = target as usize;
        let mut word_index = ordinal / u64::BITS as usize;
        let bit = ordinal % u64::BITS as usize;
        let mut word = self.words[word_index] & (u64::MAX << bit);
        loop {
            if word != 0 {
                let candidate = word_index
                    .checked_mul(u64::BITS as usize)?
                    .checked_add(word.trailing_zeros() as usize)?;
                let candidate = u32::try_from(candidate).ok()?;
                return (candidate < self.document_count).then(|| DocId::new(candidate));
            }
            word_index = word_index.checked_add(1)?;
            word = *self.words.get(word_index)?;
        }
    }
}

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
    first_ordinal: u32,
    next_ordinal: u32,
    end_ordinal: u32,
    component_max_doc_ids: Vec<DocId>,
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
        if reference.component_max_doc_ids.len() != reference.component_count as usize
            || reference
                .component_max_doc_ids
                .last()
                .is_some_and(|maximum| maximum.get() >= segment.document_count)
        {
            return Err(IndexError::InvalidFormat("posting component bound"));
        }
        let stream = ComponentStream::new(
            directory,
            segment.identity,
            &segment.packs,
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
            first_ordinal: reference.first_component_ordinal,
            next_ordinal: reference.first_component_ordinal,
            end_ordinal,
            component_max_doc_ids: reference.component_max_doc_ids,
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
            &self.segment.packs,
            &leaf.descriptor,
            ComponentKind::POSTINGS,
        )
        .await?;
        self.statistics.posting_block_sought(
            u64::try_from(loaded.payload.len()).map_err(|_| IndexError::OffsetOverflow)?,
        );
        let block = self
            .directory
            .run_query_cpu(move || DecodedPostingBlock::decode_payload(&loaded.payload))
            .await?;
        let relative = self
            .next_ordinal
            .checked_sub(self.first_ordinal)
            .ok_or(IndexError::InvalidFormat("posting component ordinal"))?;
        let expected = self
            .component_max_doc_ids
            .get(relative as usize)
            .ok_or(IndexError::InvalidFormat("posting component bound"))?;
        if block.last_doc_id() != *expected {
            return Err(IndexError::InvalidFormat("posting component bound"));
        }
        self.block = Some(block);
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
        self.seek_component_containing(target)?;
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

    /// Position the routed stream at the first posting component which can
    /// contain `target`.
    fn seek_component_containing(&mut self, target: DocId) -> Result<(), IndexError> {
        let mut relative = self
            .component_max_doc_ids
            .partition_point(|maximum| *maximum < target);
        // Reading the final component proves exhaustion when the target lies
        // beyond the declared final bound instead of trusting unvisited
        // cross-component metadata to produce an empty result.
        relative = relative.min(self.component_max_doc_ids.len() - 1);
        let ordinal = self
            .first_ordinal
            .checked_add(u32::try_from(relative).map_err(|_| IndexError::OffsetOverflow)?)
            .ok_or(IndexError::OffsetOverflow)?;
        if ordinal <= self.next_ordinal {
            return Ok(());
        }
        let last = self
            .end_ordinal
            .checked_sub(1)
            .ok_or(IndexError::InvalidFormat("empty posting reference"))?;
        self.stream = ComponentStream::new(
            self.directory,
            self.segment.identity,
            &self.segment.packs,
            ComponentKind::POSTINGS,
            self.root.clone(),
            Some(component_ordinal_key(ordinal).to_vec()),
            Some(component_ordinal_key(last).to_vec()),
        )?;
        self.next_ordinal = ordinal;
        self.block = None;
        self.position = 0;
        self.current = None;
        Ok(())
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
            &self.segment.packs,
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
/// range. Immutable dictionary and posting leaves are traversed once into a
/// charged segment-local DocId set, then exposed as an ordered cursor.
pub(super) struct TermRangeStream<'a, D> {
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    field_id: FieldId,
    dictionary_root: ArtifactDescriptor,
    bounds: TermBounds,
    documents: Option<SegmentDocSet>,
    current: Option<DocId>,
    exhausted: bool,
    estimated_documents: u64,
    statistics: NativeQueryStatisticsRecorder,
}

#[derive(Clone)]
pub(super) struct PointBounds {
    lower: Option<RangeBound>,
    upper: Option<RangeBound>,
    presence: bool,
    null: bool,
}

impl PointBounds {
    pub(super) fn new(
        lower: Option<RangeBound>,
        upper: Option<RangeBound>,
    ) -> Result<Self, IndexError> {
        if lower.is_none() && upper.is_none() {
            return Err(IndexError::InvalidQuery(
                "point range requires a bound".into(),
            ));
        }
        if let (Some(lower), Some(upper)) = (&lower, &upper)
            && (std::mem::discriminant(&lower.value) != std::mem::discriminant(&upper.value)
                || lower.value > upper.value)
        {
            return Err(IndexError::InvalidQuery(
                "point range bounds are reversed".into(),
            ));
        }
        Ok(Self {
            lower,
            upper,
            presence: false,
            null: false,
        })
    }

    pub(super) fn presence() -> Self {
        Self {
            lower: None,
            upper: None,
            presence: true,
            null: false,
        }
    }

    pub(super) fn null() -> Self {
        Self {
            lower: None,
            upper: None,
            presence: false,
            null: true,
        }
    }

    fn contains(&self, value: &PointValue) -> bool {
        if self.presence {
            return value == &PointValue::Presence;
        }
        if self.null {
            return value == &PointValue::Null;
        }
        let PointValue::Value(value) = value else {
            return false;
        };
        self.lower.as_ref().is_none_or(|bound| {
            std::mem::discriminant(value) == std::mem::discriminant(&bound.value)
                && (value > &bound.value || bound.inclusive && value == &bound.value)
        }) && self.upper.as_ref().is_none_or(|bound| {
            std::mem::discriminant(value) == std::mem::discriminant(&bound.value)
                && (value < &bound.value || bound.inclusive && value == &bound.value)
        })
    }
}

pub(super) struct PointRangeStream<'a, D> {
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    field_id: FieldId,
    root: ArtifactDescriptor,
    bounds: PointBounds,
    documents: Option<SegmentDocSet>,
    current: Option<DocId>,
    exhausted: bool,
    estimated_documents: u64,
    statistics: NativeQueryStatisticsRecorder,
}

impl<'a, D: ArtifactDirectoryRead> PointRangeStream<'a, D> {
    pub(super) fn new(
        directory: &'a D,
        segment: &'a SegmentDescriptor,
        field_id: FieldId,
        bounds: PointBounds,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Result<Self, IndexError> {
        Ok(Self {
            directory,
            segment,
            field_id,
            root: component_root(segment, ComponentKind::POINTS, Some(field_id))?,
            bounds,
            documents: None,
            current: None,
            exhausted: false,
            estimated_documents: u64::from(segment.document_count),
            statistics,
        })
    }

    async fn prepare(&mut self) -> Result<(), IndexError> {
        if self.documents.is_some() {
            return Ok(());
        }
        let (minimum, maximum) = if self.bounds.presence {
            let (minimum, maximum) =
                super::super::point_value_range(self.field_id, &PointValue::Presence)?;
            (Some(minimum), Some(maximum))
        } else if self.bounds.null {
            let (minimum, maximum) =
                super::super::point_value_range(self.field_id, &PointValue::Null)?;
            (Some(minimum), Some(maximum))
        } else {
            (
                self.bounds
                    .lower
                    .as_ref()
                    .map(|bound| {
                        super::super::point_value_range(
                            self.field_id,
                            &PointValue::Value(bound.value.clone()),
                        )
                        .map(|range| range.0)
                    })
                    .transpose()?,
                self.bounds
                    .upper
                    .as_ref()
                    .map(|bound| {
                        super::super::point_value_range(
                            self.field_id,
                            &PointValue::Value(bound.value.clone()),
                        )
                        .map(|range| range.1)
                    })
                    .transpose()?,
            )
        };
        let mut stream = ComponentStream::new(
            self.directory,
            self.segment.identity,
            &self.segment.packs,
            ComponentKind::POINTS,
            self.root.clone(),
            minimum,
            maximum,
        )?;
        let mut candidates = SegmentDocSet::new(self.segment.document_count)?;
        while let Some(leaf) = stream.next_leaf().await? {
            let loaded = read_artifact_component(
                self.directory,
                self.segment.identity,
                &self.segment.packs,
                &leaf.descriptor,
                ComponentKind::POINTS,
            )
            .await?;
            self.statistics.point_blocks_decoded(1);
            let block = self
                .directory
                .run_query_cpu(move || PointBlock::decode_payload(&loaded.payload))
                .await?;
            if block.field_id != self.field_id {
                return Err(IndexError::InvalidFormat("point field identity"));
            }
            for entry in block.entries() {
                if !self.bounds.contains(&entry.value) {
                    continue;
                }
                candidates.insert(entry.doc_id)?;
            }
        }
        self.documents = Some(candidates);
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DocId>, IndexError> {
        let target = self
            .current
            .map(DocId::checked_next)
            .transpose()?
            .map_or(0, DocId::get);
        self.advance(DocId::new(target)).await
    }

    async fn advance(&mut self, target: DocId) -> Result<Option<DocId>, IndexError> {
        if self.exhausted {
            return Ok(None);
        }
        if self.current.is_some_and(|current| current >= target) {
            return Ok(self.current);
        }
        self.prepare().await?;
        self.current = self
            .documents
            .as_ref()
            .expect("point candidate set prepared")
            .first_at_or_after(target.get());
        self.exhausted = self.current.is_none();
        Ok(self.current)
    }
}

pub(super) struct DocValuePresenceStream<'a, D> {
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    field_id: FieldId,
    stream: ComponentStream<'a, D>,
    block: Option<DocValueBlock>,
    offset: usize,
    current: Option<DocId>,
    statistics: NativeQueryStatisticsRecorder,
}

impl<'a, D: ArtifactDirectoryRead> DocValuePresenceStream<'a, D> {
    pub(super) fn new(
        directory: &'a D,
        segment: &'a SegmentDescriptor,
        field_id: FieldId,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Result<Self, IndexError> {
        let root = component_root(segment, ComponentKind::DOC_VALUES, Some(field_id))?;
        Ok(Self {
            directory,
            segment,
            field_id,
            stream: ComponentStream::new(
                directory,
                segment.identity,
                &segment.packs,
                ComponentKind::DOC_VALUES,
                root,
                None,
                None,
            )?,
            block: None,
            offset: 0,
            current: None,
            statistics,
        })
    }

    async fn load(&mut self) -> Result<bool, IndexError> {
        let Some(leaf) = self.stream.next_leaf().await? else {
            self.block = None;
            return Ok(false);
        };
        let loaded = read_artifact_component(
            self.directory,
            self.segment.identity,
            &self.segment.packs,
            &leaf.descriptor,
            ComponentKind::DOC_VALUES,
        )
        .await?;
        self.statistics.doc_value_blocks_decoded(1);
        let block = self
            .directory
            .run_query_cpu(move || DocValueBlock::decode_payload(&loaded.payload))
            .await?;
        if block.field_id != self.field_id {
            return Err(IndexError::InvalidFormat("doc-value field identity"));
        }
        self.block = Some(block);
        self.offset = 0;
        Ok(true)
    }

    async fn next(&mut self) -> Result<Option<DocId>, IndexError> {
        loop {
            if self.block.is_none() && !self.load().await? {
                self.current = None;
                return Ok(None);
            }
            let block = self.block.as_ref().expect("loaded doc-value block");
            while self.offset < block.cells().len() {
                let offset = self.offset;
                self.offset += 1;
                if block.cells()[offset].present {
                    let doc = block
                        .first_doc_id
                        .get()
                        .checked_add(u32::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?)
                        .ok_or(IndexError::OffsetOverflow)?;
                    self.current = Some(DocId::new(doc));
                    return Ok(self.current);
                }
            }
            self.block = None;
        }
    }

    async fn advance(&mut self, target: DocId) -> Result<Option<DocId>, IndexError> {
        if self.current.is_some_and(|current| current >= target) {
            return Ok(self.current);
        }
        loop {
            let Some(block) = self.block.as_ref() else {
                return self.next().await;
            };
            let target_offset = target.get().saturating_sub(block.first_doc_id.get()) as usize;
            self.offset = self.offset.max(target_offset.min(block.cells().len()));
            match self.next().await? {
                Some(doc) if doc < target => continue,
                value => return Ok(value),
            }
        }
    }
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
            documents: None,
            current: None,
            exhausted: false,
            estimated_documents: u64::from(segment.document_count),
            statistics,
        })
    }

    async fn prepare(&mut self) -> Result<(), IndexError> {
        if self.documents.is_some() {
            return Ok(());
        }

        self.statistics.term_seek();
        let mut candidates = SegmentDocSet::new(self.segment.document_count)?;
        let mut dictionaries = ComponentStream::new(
            self.directory,
            self.segment.identity,
            &self.segment.packs,
            ComponentKind::TERM_DICTIONARY,
            self.dictionary_root.clone(),
            Some(self.bounds.minimum.clone()),
            Some(self.bounds.maximum.clone()),
        )?;
        while let Some(leaf) = dictionaries.next_leaf().await? {
            let loaded = read_artifact_component(
                self.directory,
                self.segment.identity,
                &self.segment.packs,
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
                    entry.postings.clone(),
                    self.statistics.clone(),
                )?;
                let mut candidate = postings.next().await?;
                while let Some(doc_id) = candidate {
                    candidates.insert(doc_id)?;
                    candidate = postings.next().await?;
                }
            }
        }
        self.documents = Some(candidates);
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DocId>, IndexError> {
        let target = self
            .current
            .map(DocId::checked_next)
            .transpose()?
            .map_or(0, DocId::get);
        self.advance(DocId::new(target)).await
    }

    async fn advance(&mut self, target: DocId) -> Result<Option<DocId>, IndexError> {
        if self.exhausted {
            return Ok(None);
        }
        if self.current.is_some_and(|current| current >= target) {
            return Ok(self.current);
        }
        self.prepare().await?;
        self.current = self
            .documents
            .as_ref()
            .expect("term-range candidate set prepared")
            .first_at_or_after(target.get());
        self.exhausted = self.current.is_none();
        Ok(self.current)
    }
}

// Keeping cursor state inline avoids an allocation per Boolean iterator node.
#[allow(clippy::large_enum_variant)]
pub(super) enum DocCursor<'a, D> {
    Empty,
    All { next: u32, end: u32 },
    Posting(PostingStream<'a, D>),
    Phrase(PhraseCursor<'a, D>),
    TermRange(TermRangeStream<'a, D>),
    PointRange(PointRangeStream<'a, D>),
    DocValuePresence(DocValuePresenceStream<'a, D>),
    And(AndCursor<'a, D>),
    Or(OrCursor<'a, D>),
    Not(NotCursor<'a, D>),
}

pub(super) struct PhraseCursor<'a, D> {
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    field_id: FieldId,
    terms: Vec<PostingStream<'a, D>>,
    heads: Vec<Option<DocId>>,
    initialized: bool,
    emitted: Option<DocId>,
    estimated_documents: u64,
    statistics: NativeQueryStatisticsRecorder,
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

    pub(super) fn phrase(
        directory: &'a D,
        segment: &'a SegmentDescriptor,
        field_id: FieldId,
        terms: Vec<PostingReference>,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Result<Self, IndexError> {
        if terms.is_empty() {
            return Err(IndexError::InvalidQuery(
                "phrase query requires at least one term".into(),
            ));
        }
        let estimated_documents = terms
            .iter()
            .map(|term| term.document_frequency)
            .min()
            .unwrap_or(0);
        let terms = terms
            .into_iter()
            .map(|term| PostingStream::new(directory, segment, field_id, term, statistics.clone()))
            .collect::<Result<Vec<_>, IndexError>>()?;
        Ok(Self::Phrase(PhraseCursor {
            directory,
            segment,
            field_id,
            heads: vec![None; terms.len()],
            terms,
            initialized: false,
            emitted: None,
            estimated_documents,
            statistics,
        }))
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
            Self::Phrase(cursor) => cursor.estimated_documents,
            Self::TermRange(cursor) => cursor.estimated_documents,
            Self::PointRange(cursor) => cursor.estimated_documents,
            Self::DocValuePresence(cursor) => u64::from(cursor.segment.document_count),
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
                Self::Phrase(cursor) => cursor.next().await,
                Self::TermRange(cursor) => cursor.next().await,
                Self::PointRange(cursor) => cursor.next().await,
                Self::DocValuePresence(cursor) => cursor.next().await,
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
                Self::Phrase(cursor) => cursor.advance(target).await,
                Self::TermRange(cursor) => cursor.advance(target).await,
                Self::PointRange(cursor) => cursor.advance(target).await,
                Self::DocValuePresence(cursor) => cursor.advance(target).await,
                Self::And(cursor) => cursor.advance(target).await,
                Self::Or(cursor) => cursor.advance(target).await,
                Self::Not(cursor) => cursor.advance(target).await,
            }
        })
    }

    pub(super) fn release_decoded(&mut self) -> Result<(), IndexError> {
        match self {
            Self::Empty
            | Self::All { .. }
            | Self::TermRange(_)
            | Self::PointRange(_)
            | Self::DocValuePresence(_) => Ok(()),
            Self::Posting(cursor) => cursor.release_decoded(),
            Self::Phrase(cursor) => cursor.release_decoded(),
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

impl<'a, D: ArtifactDirectoryRead> PhraseCursor<'a, D> {
    async fn initialize(&mut self) -> Result<bool, IndexError> {
        if !self.initialized {
            for (head, term) in self.heads.iter_mut().zip(&mut self.terms) {
                *head = term.next().await?;
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
            for (head, term) in self.heads.iter_mut().zip(&mut self.terms) {
                if head.is_some_and(|value| value < target) {
                    self.statistics.conjunction_advance();
                    *head = term.advance(target).await?;
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

    async fn positionally_matches(&mut self, doc_id: DocId) -> Result<bool, IndexError> {
        self.statistics.two_phase_verification();
        let mut positions = Vec::with_capacity(self.terms.len());
        for term in &self.terms {
            let ordinal = term
                .current_component_ordinal()
                .ok_or(IndexError::InvalidFormat(
                    "phrase position lookup has no posting ordinal",
                ))?;
            let Some(values) =
                positions_for(self.directory, self.segment, self.field_id, ordinal, doc_id).await?
            else {
                return Err(IndexError::InvalidFormat(
                    "phrase posting has no position entry",
                ));
            };
            positions.push(values);
        }
        self.directory
            .run_query_cpu(move || Ok(positional_sequence(&positions)))
            .await
    }

    async fn next(&mut self) -> Result<Option<DocId>, IndexError> {
        if !self.initialize().await? {
            return Ok(None);
        }
        loop {
            if self.emitted.take().is_some() {
                self.heads[0] = self.terms[0].next().await?;
                if self.heads[0].is_none() {
                    return Ok(None);
                }
            }
            let Some(candidate) = self.align().await? else {
                return Ok(None);
            };
            self.emitted = Some(candidate);
            if self.positionally_matches(candidate).await? {
                return Ok(Some(candidate));
            }
        }
    }

    async fn advance(&mut self, target: DocId) -> Result<Option<DocId>, IndexError> {
        if self.emitted.is_some_and(|current| current >= target) {
            return Ok(self.emitted);
        }
        if !self.initialize().await? {
            return Ok(None);
        }
        self.emitted = None;
        for (head, term) in self.heads.iter_mut().zip(&mut self.terms) {
            if head.is_some_and(|value| value < target) {
                self.statistics.conjunction_advance();
                *head = term.advance(target).await?;
                if head.is_none() {
                    return Ok(None);
                }
            }
        }
        self.next().await
    }

    fn release_decoded(&mut self) -> Result<(), IndexError> {
        self.terms
            .iter_mut()
            .try_for_each(PostingStream::release_decoded)
    }
}

async fn positions_for<D: ArtifactDirectoryRead>(
    directory: &D,
    segment: &SegmentDescriptor,
    field_id: FieldId,
    ordinal: u32,
    doc_id: DocId,
) -> Result<Option<Vec<u32>>, IndexError> {
    let root = component_root(segment, ComponentKind::POSITIONS, Some(field_id))?;
    let key = component_ordinal_key(ordinal).to_vec();
    let mut stream = ComponentStream::new(
        directory,
        segment.identity,
        &segment.packs,
        ComponentKind::POSITIONS,
        root,
        Some(key.clone()),
        Some(key.clone()),
    )?;
    let Some(leaf) = stream.next_leaf().await? else {
        return Err(IndexError::InvalidFormat(
            "phrase posting has no position component",
        ));
    };
    if leaf.minimum_key != key || leaf.maximum_key != key || stream.next_leaf().await?.is_some() {
        return Err(IndexError::InvalidFormat("position stream ordinal"));
    }
    let loaded = read_artifact_component(
        directory,
        segment.identity,
        &segment.packs,
        &leaf.descriptor,
        ComponentKind::POSITIONS,
    )
    .await?;
    let block = directory
        .run_query_cpu(move || PositionsBlock::decode_payload(&loaded.payload))
        .await?;
    Ok(block
        .entries()
        .binary_search_by_key(&doc_id, |entry| entry.doc_id)
        .ok()
        .map(|index| block.entries()[index].positions.clone()))
}

fn positional_sequence(positions: &[Vec<u32>]) -> bool {
    let Some(first) = positions.first() else {
        return false;
    };
    first.iter().any(|start| {
        positions
            .iter()
            .enumerate()
            .skip(1)
            .all(|(offset, values)| {
                start
                    .checked_add(offset as u32)
                    .is_some_and(|expected| values.binary_search(&expected).is_ok())
            })
    })
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
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use crate::IndexFileRead;
    use crate::v4::build::{
        BuildLimits, ExactMemorySink, NativeSegmentWriter, ProjectedRecord, ProjectedSource,
        ProjectedTerm, PublishedObject, SourcePush,
    };
    use crate::v4::{
        ArtifactPackReference, Cardinality, Collation, ComponentVersion, FIELD_PRESENCE_TERM,
        FieldCapabilities, FieldComponents, FieldSchema, FieldType, IndexKind, IndexSemantics,
        ObjectIdentity, PostingBlock, Schema, SegmentIdentity, TERM_TYPE_FIELD_PRESENCE,
        canonical_term_key, scalar_term,
    };

    #[derive(Clone)]
    struct MemoryFile(Arc<[u8]>);

    impl IndexFileRead for MemoryFile {
        type Slice = Arc<[u8]>;

        async fn read_at(&self, offset: u64, maximum: usize) -> Result<Self::Slice, IndexError> {
            let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
            if start >= self.0.len() {
                return Ok(Arc::from([]));
            }
            Ok(Arc::from(
                &self.0[start..start.saturating_add(maximum).min(self.0.len())],
            ))
        }
    }

    struct MemoryArtifacts(BTreeMap<String, PublishedObject>);

    impl ArtifactDirectoryRead for MemoryArtifacts {
        type File = MemoryFile;

        async fn open_artifact(
            &self,
            pack: &ArtifactPackReference,
        ) -> Result<Self::File, IndexError> {
            let object = self
                .0
                .get(&pack.path)
                .ok_or_else(|| IndexError::FileNotFound(pack.path.clone()))?;
            if object.object_version != pack.object_version
                || *blake3::hash(&object.bytes).as_bytes() != pack.object_content_hash
                || object.bytes.len() as u64 != pack.object_length
            {
                return Err(IndexError::Integrity);
            }
            Ok(MemoryFile(Arc::from(object.bytes.as_slice())))
        }
    }

    fn component_version(component_kind: ComponentKind) -> ComponentVersion {
        ComponentVersion {
            component_kind,
            codec_version: u16::from(component_kind == ComponentKind::IDENTITY_TABLE) + 1,
        }
    }

    fn keyword_schema() -> Schema {
        let mut field = FieldSchema {
            id: FieldId::new(0),
            name: "value".into(),
            source_selector: "/value".into(),
            field_type: FieldType::Keyword,
            cardinality: Cardinality::Single,
            allow_missing: false,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::EXACT,
            analyzer: None,
            components: FieldComponents::TERMS,
        };
        field.components = field.compiled_components().unwrap();
        Schema {
            kind: IndexKind::TypedJson,
            path_prefix: String::new(),
            content_type_scope: Some("application/json".into()),
            fields: vec![field],
            semantics: IndexSemantics::TypedJson,
            physical_order: Vec::new(),
            component_versions: BTreeSet::from([
                ComponentKind::ROUTING_NODE,
                ComponentKind::IDENTITY_TABLE,
                ComponentKind::LIVE_MASK,
                ComponentKind::PATH_LOCATOR,
                ComponentKind::TERM_DICTIONARY,
                ComponentKind::POSTINGS,
                ComponentKind::SCORING_STATISTICS,
            ])
            .into_iter()
            .map(component_version)
            .collect(),
        }
    }

    fn keyword_source(ordinal: u32, term_type: u8, term: &[u8]) -> ProjectedSource {
        ProjectedSource {
            source_identity: ObjectIdentity {
                path: format!("source-{ordinal:05}"),
                version: 1,
            },
            records: vec![ProjectedRecord {
                result_identity: None,
                order_key: Vec::new(),
                terms: vec![
                    ProjectedTerm {
                        field_id: FieldId::new(0),
                        term_type,
                        term: term.to_vec(),
                        frequency: 1,
                        positions: Vec::new(),
                    },
                    ProjectedTerm {
                        field_id: FieldId::new(0),
                        term_type: TERM_TYPE_FIELD_PRESENCE,
                        term: FIELD_PRESENCE_TERM.to_vec(),
                        frequency: 1,
                        positions: Vec::new(),
                    },
                ],
                points: Vec::new(),
                doc_values: Vec::new(),
                vectors: Vec::new(),
                field_lengths: Vec::new(),
            }],
        }
    }

    async fn multi_component_postings() -> (SegmentDescriptor, MemoryArtifacts, PostingReference) {
        let schema = keyword_schema();
        let identity = SegmentIdentity::new(41, 1, schema.fingerprint().unwrap(), 1).unwrap();
        let mut writer = NativeSegmentWriter::new(
            identity,
            schema,
            BuildLimits::new(128 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let (term_type, term) =
            scalar_term(&super::super::super::ScalarValue::String("common".into())).unwrap();
        for ordinal in 0..=(16 * 1024) {
            assert_eq!(
                writer
                    .push_source(keyword_source(ordinal, term_type, &term))
                    .unwrap(),
                SourcePush::Accepted
            );
        }
        let mut sink = ExactMemorySink::new();
        let segment = writer.seal(&mut sink).await.unwrap().descriptor;
        let directory = MemoryArtifacts(sink.objects().clone());
        let canonical = canonical_term_key(FieldId::new(0), term_type, &term).unwrap();
        let root = component_root(
            &segment,
            ComponentKind::TERM_DICTIONARY,
            Some(FieldId::new(0)),
        )
        .unwrap();
        let mut dictionaries = ComponentStream::new(
            &directory,
            segment.identity,
            &segment.packs,
            ComponentKind::TERM_DICTIONARY,
            root,
            Some(canonical.clone()),
            Some(canonical.clone()),
        )
        .unwrap();
        let leaf = dictionaries.next_leaf().await.unwrap().unwrap();
        let loaded = read_artifact_component(
            &directory,
            segment.identity,
            &segment.packs,
            &leaf.descriptor,
            ComponentKind::TERM_DICTIONARY,
        )
        .await
        .unwrap();
        let dictionary = TermDictionary::decode_payload(&loaded.payload).unwrap();
        let reference = dictionary.exact(&canonical).unwrap().postings.clone();
        assert_eq!(reference.component_count, 2);
        assert_eq!(reference.component_max_doc_ids.len(), 2);
        (segment, directory, reference)
    }

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

    #[tokio::test]
    async fn early_and_late_advances_each_open_only_the_selected_posting_component() {
        let (segment, directory, reference) = multi_component_postings().await;
        let early_statistics = NativeQueryStatisticsRecorder::new();
        let mut early = PostingStream::new(
            &directory,
            &segment,
            FieldId::new(0),
            reference.clone(),
            early_statistics.clone(),
        )
        .unwrap();
        assert_eq!(
            early.advance(DocId::new(0)).await.unwrap(),
            Some(DocId::new(0))
        );
        let early_snapshot = early_statistics.snapshot();
        assert_eq!(
            early_snapshot.posting_blocks_sought, 1,
            "{early_snapshot:?}"
        );
        assert_eq!(
            early_snapshot.posting_blocks_decoded, 1,
            "{early_snapshot:?}"
        );

        let statistics = NativeQueryStatisticsRecorder::new();
        let mut postings = PostingStream::new(
            &directory,
            &segment,
            FieldId::new(0),
            reference,
            statistics.clone(),
        )
        .unwrap();

        assert_eq!(
            postings.advance(DocId::new(16 * 1024)).await.unwrap(),
            Some(DocId::new(16 * 1024))
        );
        let snapshot = statistics.snapshot();
        assert_eq!(snapshot.posting_blocks_sought, 1, "{snapshot:?}");
        assert_eq!(snapshot.posting_blocks_decoded, 1, "{snapshot:?}");
        assert_eq!(snapshot.posting_blocks_skipped, 0, "{snapshot:?}");
    }
}
