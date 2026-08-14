use std::collections::{BTreeMap, BTreeSet};

use crate::IndexError;

use super::build::{
    ComponentBatchSink, DescriptorLeaf, PublishedStream, StreamingComponentPublisher,
};
use super::locator::PathLocatorBlockBuilder;
use super::{
    ArtifactDescriptor, ArtifactDirectoryRead, ComponentKind, ComponentStream, DocId, DocIdRange,
    LiveMaskBlock, LocatorEntry, LocatorValue, PathLocatorBlock, SegmentDescriptor,
    SegmentIdentity, read_artifact_component,
};

pub const LOCATOR_COMPACTION_FAN_IN: usize = 4;

/// One immutable path-locator delta in generation order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocatorStreamRoot {
    pub sequence: u64,
    pub identity: SegmentIdentity,
    pub artifact: ArtifactDescriptor,
}

impl LocatorStreamRoot {
    fn validate(&self) -> Result<(), IndexError> {
        if self.sequence == 0 {
            return Err(IndexError::InvalidFormat(
                "path-locator sequence must be non-zero",
            ));
        }
        self.identity.validate()?;
        self.artifact.validate(self.identity.index_id)?;
        if self.artifact.component_kind != ComponentKind::ROUTING_NODE {
            return Err(IndexError::InvalidFormat(
                "path-locator root has the wrong component kind",
            ));
        }
        Ok(())
    }
}

/// Resolve one path from immutable locator deltas. Highest object version wins
/// regardless of delta enumeration order. For equal live versions, the newer
/// sequence carries the current post-merge DocId ranges; live/delete conflicts
/// at one version are corruption.
pub async fn locate_path<D: ArtifactDirectoryRead>(
    directory: &D,
    roots: &[LocatorStreamRoot],
    path: &str,
) -> Result<Option<LocatorValue>, IndexError> {
    let paths = [path];
    let mut values = locate_path_values(directory, roots, &paths, usize::MAX).await?;
    Ok(values.pop().flatten())
}

/// Resolve borrowed sorted unique paths into ordinal-aligned values without
/// cloning path keys. `maximum_result_bytes` bounds the exact retained backing
/// allocations used by the result and its selection state, including retained
/// live DocId ranges.
#[doc(hidden)]
pub async fn locate_path_values<D: ArtifactDirectoryRead>(
    directory: &D,
    roots: &[LocatorStreamRoot],
    paths: &[&str],
    maximum_result_bytes: usize,
) -> Result<Vec<Option<LocatorValue>>, IndexError> {
    for path in paths {
        validate_locator_query_path(path)?;
    }
    if paths
        .windows(2)
        .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
    {
        return Err(IndexError::InvalidQuery(
            "path locator wave must be sorted and unique".into(),
        ));
    }

    let requested_fixed_bytes =
        locator_result_fixed_resident_bytes(paths.len(), paths.len(), paths.len())?;
    if requested_fixed_bytes > maximum_result_bytes {
        return Err(IndexError::ResourceLimit {
            needed: requested_fixed_bytes,
            limit: maximum_result_bytes,
        });
    }
    let mut selected = Vec::with_capacity(paths.len());
    selected.resize_with(paths.len(), || None);
    let selected_sequences = vec![0_u64; paths.len()];
    let seen_epochs = vec![0_u64; paths.len()];
    let fixed_bytes = locator_result_fixed_resident_bytes(
        selected.capacity(),
        selected_sequences.capacity(),
        seen_epochs.capacity(),
    )?;
    if fixed_bytes > maximum_result_bytes {
        return Err(IndexError::ResourceLimit {
            needed: fixed_bytes,
            limit: maximum_result_bytes,
        });
    }
    if paths.is_empty() {
        return Ok(selected);
    }

    locate_path_values_inner(
        directory,
        roots,
        paths,
        selected,
        selected_sequences,
        seen_epochs,
        fixed_bytes,
        maximum_result_bytes,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn locate_path_values_inner<D: ArtifactDirectoryRead>(
    directory: &D,
    roots: &[LocatorStreamRoot],
    paths: &[&str],
    mut selected: Vec<Option<LocatorValue>>,
    mut selected_sequences: Vec<u64>,
    mut seen_epochs: Vec<u64>,
    fixed_bytes: usize,
    maximum_result_bytes: usize,
) -> Result<Vec<Option<LocatorValue>>, IndexError> {
    let mut selected_dynamic_bytes = 0usize;
    for (root_ordinal, root) in roots.iter().enumerate() {
        root.validate()?;
        if roots[..root_ordinal]
            .iter()
            .any(|previous| previous.sequence == root.sequence)
        {
            return Err(IndexError::InvalidFormat("duplicate path-locator sequence"));
        }
        let epoch = u64::try_from(root_ordinal)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(IndexError::OffsetOverflow)?;
        let mut stream = ComponentStream::new(
            directory,
            root.identity,
            ComponentKind::PATH_LOCATOR,
            root.artifact.clone(),
            Some(paths[0].as_bytes().to_vec()),
            Some(paths[paths.len() - 1].as_bytes().to_vec()),
        )?;
        let mut cursor = 0usize;
        while let Some(leaf) = stream.next_leaf().await? {
            while cursor < paths.len() && paths[cursor].as_bytes() < leaf.minimum_key.as_slice() {
                cursor += 1;
            }
            let first = cursor;
            while cursor < paths.len() && paths[cursor].as_bytes() <= leaf.maximum_key.as_slice() {
                cursor += 1;
            }
            if first == cursor {
                continue;
            }
            let loaded = read_artifact_component(
                directory,
                root.identity,
                &leaf.descriptor,
                ComponentKind::PATH_LOCATOR,
            )
            .await?;
            let block = PathLocatorBlock::decode_payload(&loaded.payload)?;
            for position in first..cursor {
                let Some(value) = block.get(paths[position]) else {
                    continue;
                };
                if seen_epochs[position] == epoch {
                    return Err(IndexError::InvalidFormat(
                        "one path appears twice in a locator delta",
                    ));
                }
                seen_epochs[position] = epoch;
                resolve_locator_slot(
                    &mut selected[position],
                    &mut selected_sequences[position],
                    root.sequence,
                    value,
                    fixed_bytes,
                    &mut selected_dynamic_bytes,
                    maximum_result_bytes,
                )?;
            }
        }
    }
    Ok(selected)
}

/// Resolve a bounded sorted unique wave of paths while merge-seeking each
/// immutable locator stream once. The returned map contains only matched
/// paths, so misses do not allocate durable-looking placeholder state.
pub async fn locate_paths<D: ArtifactDirectoryRead>(
    directory: &D,
    roots: &[LocatorStreamRoot],
    paths: &[String],
) -> Result<BTreeMap<String, LocatorValue>, IndexError> {
    let borrowed = paths.iter().map(String::as_str).collect::<Vec<_>>();
    let values = locate_path_values(directory, roots, &borrowed, usize::MAX).await?;
    Ok(paths
        .iter()
        .zip(values)
        .filter_map(|(path, value)| value.map(|value| (path.clone(), value)))
        .collect())
}

fn validate_locator_query_path(path: &str) -> Result<(), IndexError> {
    if path.is_empty() || path.len() > super::INDEX_ROUTING_KEY_BYTES || path.contains('\0') {
        return Err(IndexError::InvalidQuery(
            "path locator query requires a valid exact path".into(),
        ));
    }
    Ok(())
}

fn locator_result_fixed_resident_bytes(
    result_capacity: usize,
    sequence_capacity: usize,
    seen_capacity: usize,
) -> Result<usize, IndexError> {
    std::mem::size_of::<Vec<Option<LocatorValue>>>()
        .checked_add(
            result_capacity
                .checked_mul(std::mem::size_of::<Option<LocatorValue>>())
                .ok_or(IndexError::OffsetOverflow)?,
        )
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Vec<u64>>()))
        .and_then(|bytes| {
            bytes.checked_add(sequence_capacity.checked_mul(std::mem::size_of::<u64>())?)
        })
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Vec<u64>>()))
        .and_then(|bytes| bytes.checked_add(seen_capacity.checked_mul(std::mem::size_of::<u64>())?))
        .ok_or(IndexError::OffsetOverflow)
}

fn locator_value_dynamic_bytes(value: &LocatorValue) -> Result<usize, IndexError> {
    match value {
        LocatorValue::Live { ranges, .. } => ranges
            .capacity()
            .checked_mul(std::mem::size_of::<DocIdRange>())
            .ok_or(IndexError::OffsetOverflow),
        LocatorValue::Deleted { .. } => Ok(0),
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_locator_slot(
    selected: &mut Option<LocatorValue>,
    selected_sequence: &mut u64,
    sequence: u64,
    value: &LocatorValue,
    fixed_bytes: usize,
    selected_dynamic_bytes: &mut usize,
    maximum_result_bytes: usize,
) -> Result<(), IndexError> {
    let replace = match selected.as_ref() {
        None => true,
        Some(previous) => {
            if previous.version() == value.version()
                && previous != value
                && !matches!(
                    (previous, value),
                    (LocatorValue::Live { .. }, LocatorValue::Live { .. })
                )
            {
                return Err(IndexError::InvalidFormat(
                    "conflicting path-locator values at one version",
                ));
            }
            previous.version() < value.version()
                || previous.version() == value.version()
                    && matches!(
                        (previous, value),
                        (LocatorValue::Live { .. }, LocatorValue::Live { .. })
                    )
                    && sequence > *selected_sequence
                || previous == value && sequence > *selected_sequence
        }
    };
    if !replace {
        return Ok(());
    }

    let incoming_bytes = match value {
        LocatorValue::Live { ranges, .. } => ranges
            .len()
            .checked_mul(std::mem::size_of::<DocIdRange>())
            .ok_or(IndexError::OffsetOverflow)?,
        LocatorValue::Deleted { .. } => 0,
    };
    let needed = fixed_bytes
        .checked_add(*selected_dynamic_bytes)
        .and_then(|bytes| bytes.checked_add(incoming_bytes))
        .ok_or(IndexError::OffsetOverflow)?;
    if needed > maximum_result_bytes {
        return Err(IndexError::ResourceLimit {
            needed,
            limit: maximum_result_bytes,
        });
    }
    let replacement = value.clone();
    let replacement_bytes = locator_value_dynamic_bytes(&replacement)?;
    let actual_needed = fixed_bytes
        .checked_add(*selected_dynamic_bytes)
        .and_then(|bytes| bytes.checked_add(replacement_bytes))
        .ok_or(IndexError::OffsetOverflow)?;
    if actual_needed > maximum_result_bytes {
        return Err(IndexError::ResourceLimit {
            needed: actual_needed,
            limit: maximum_result_bytes,
        });
    }
    let previous_bytes = selected
        .as_ref()
        .map(locator_value_dynamic_bytes)
        .transpose()?
        .unwrap_or(0);
    *selected_dynamic_bytes = (*selected_dynamic_bytes)
        .checked_sub(previous_bytes)
        .and_then(|bytes| bytes.checked_add(replacement_bytes))
        .ok_or(IndexError::OffsetOverflow)?;
    *selected = Some(replacement);
    *selected_sequence = sequence;
    Ok(())
}

fn resolve_locator_pair(
    selected: &mut Option<(u64, LocatorValue)>,
    sequence: u64,
    value: LocatorValue,
) -> Result<(), IndexError> {
    let Some((previous_sequence, previous)) = selected.as_ref() else {
        *selected = Some((sequence, value));
        return Ok(());
    };
    if previous.version() < value.version()
        || previous.version() == value.version()
            && matches!(
                (previous, &value),
                (LocatorValue::Live { .. }, LocatorValue::Live { .. })
            )
            && sequence > *previous_sequence
        || previous == &value && sequence > *previous_sequence
    {
        *selected = Some((sequence, value));
    } else if previous.version() == value.version()
        && previous != &value
        && !matches!(
            (previous, &value),
            (LocatorValue::Live { .. }, LocatorValue::Live { .. })
        )
    {
        return Err(IndexError::InvalidFormat(
            "conflicting path-locator values at one version",
        ));
    }
    Ok(())
}

/// Merge a small deterministic fan-in of complete locator deltas into one
/// immutable stream. At most one decoded locator block per input and one
/// output block are resident; deletion tombstones are retained.
pub async fn compact_locator_roots<D, S>(
    directory: &D,
    sink: &mut S,
    roots: &[LocatorStreamRoot],
    output_identity: SegmentIdentity,
    locator_codec_version: u16,
    routing_codec_version: u16,
) -> Result<PublishedStream, IndexError>
where
    D: ArtifactDirectoryRead,
    S: ComponentBatchSink,
{
    if !(2..=LOCATOR_COMPACTION_FAN_IN).contains(&roots.len()) {
        return Err(IndexError::InvalidDefinition(
            "locator compaction requires two to four roots".into(),
        ));
    }
    output_identity.validate()?;
    let mut sequences = BTreeSet::new();
    let mut ordered = roots.to_vec();
    ordered.sort_by_key(|root| root.sequence);
    let mut cursors = Vec::with_capacity(ordered.len());
    for root in ordered {
        root.validate()?;
        if root.identity.index_id != output_identity.index_id
            || root.identity.definition_version != output_identity.definition_version
            || root.identity.schema_fingerprint != output_identity.schema_fingerprint
            || !sequences.insert(root.sequence)
        {
            return Err(IndexError::InvalidFormat(
                "locator compaction roots have incompatible identities or sequences",
            ));
        }
        let mut cursor = LocatorMergeCursor::new(directory, root)?;
        cursor.advance().await?;
        cursors.push(cursor);
    }

    let mut block = PathLocatorBlockBuilder::default();
    let mut output = StreamingComponentPublisher::new(
        sink,
        output_identity,
        ComponentKind::PATH_LOCATOR,
        locator_codec_version,
        routing_codec_version,
    )?;
    while let Some(path) = cursors
        .iter()
        .filter_map(|cursor| cursor.current.as_ref().map(|entry| entry.path.as_str()))
        .min()
        .map(str::to_owned)
    {
        let mut selected = None;
        for cursor in &mut cursors {
            if cursor
                .current
                .as_ref()
                .is_some_and(|entry| entry.path == path)
            {
                let entry = cursor.current.take().expect("checked locator entry");
                resolve_locator_pair(&mut selected, cursor.sequence, entry.value)?;
                cursor.advance().await?;
            }
        }
        let entry = LocatorEntry {
            path,
            value: selected
                .expect("at least one cursor selected the minimum path")
                .1,
        };
        if let Some(pending) = block.push(entry)? {
            let full = block.finish()?.expect("full locator block is non-empty");
            push_locator_block(&mut output, full).await?;
            debug_assert!(block.push(pending)?.is_none());
        }
    }
    if let Some(final_block) = block.finish()? {
        push_locator_block(&mut output, final_block).await?;
    }
    output.finish().await
}

struct LocatorMergeCursor<'a, D> {
    sequence: u64,
    identity: SegmentIdentity,
    directory: &'a D,
    stream: ComponentStream<'a, D>,
    entries: std::vec::IntoIter<LocatorEntry>,
    previous_path: Option<String>,
    current: Option<LocatorEntry>,
}

impl<'a, D: ArtifactDirectoryRead> LocatorMergeCursor<'a, D> {
    fn new(directory: &'a D, root: LocatorStreamRoot) -> Result<Self, IndexError> {
        Ok(Self {
            sequence: root.sequence,
            identity: root.identity,
            directory,
            stream: ComponentStream::new(
                directory,
                root.identity,
                ComponentKind::PATH_LOCATOR,
                root.artifact,
                None,
                None,
            )?,
            entries: Vec::new().into_iter(),
            previous_path: None,
            current: None,
        })
    }

    async fn advance(&mut self) -> Result<(), IndexError> {
        loop {
            if let Some(entry) = self.entries.next() {
                if self
                    .previous_path
                    .as_ref()
                    .is_some_and(|previous| previous >= &entry.path)
                {
                    return Err(IndexError::InvalidFormat(
                        "locator stream leaves are not globally ordered",
                    ));
                }
                self.previous_path = Some(entry.path.clone());
                self.current = Some(entry);
                return Ok(());
            }
            let Some(leaf) = self.stream.next_leaf().await? else {
                self.current = None;
                return Ok(());
            };
            let loaded = read_artifact_component(
                self.directory,
                self.identity,
                &leaf.descriptor,
                ComponentKind::PATH_LOCATOR,
            )
            .await?;
            self.entries = PathLocatorBlock::decode_payload(&loaded.payload)?
                .into_entries()
                .into_iter();
        }
    }
}

async fn push_locator_block<S: ComponentBatchSink>(
    output: &mut StreamingComponentPublisher<'_, S>,
    block: PathLocatorBlock,
) -> Result<(), IndexError> {
    let first = block.entries().first().expect("locator block is non-empty");
    let last = block.entries().last().expect("locator block is non-empty");
    let minimum_key = first.path.as_bytes().to_vec();
    let maximum_key = last.path.as_bytes().to_vec();
    let element_count =
        u64::try_from(block.entries().len()).map_err(|_| IndexError::OffsetOverflow)?;
    let payload = block.encode_payload()?;
    output
        .push_payload(minimum_key, maximum_key, element_count, payload)
        .await
}

/// Publish one sorted locator delta through the ordinary component sink.
pub async fn publish_locator_delta<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    locator_codec_version: u16,
    routing_codec_version: u16,
    entries: Vec<LocatorEntry>,
) -> Result<PublishedStream, IndexError> {
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::PATH_LOCATOR,
        locator_codec_version,
        routing_codec_version,
    )?;
    for block in PathLocatorBlock::split(entries)? {
        let first = block.entries().first().expect("locator block is non-empty");
        let last = block.entries().last().expect("locator block is non-empty");
        let count = u64::try_from(block.entries().len()).map_err(|_| IndexError::OffsetOverflow)?;
        publisher
            .push_payload(
                first.path.as_bytes().to_vec(),
                last.path.as_bytes().to_vec(),
                count,
                block.encode_payload()?,
            )
            .await?;
    }
    publisher.finish().await
}

/// Materialize a replacement immutable live-mask stream while reusing every
/// unaffected data leaf. The segment core and DocIds remain unchanged.
pub async fn rewrite_segment_live_mask<D, S>(
    directory: &D,
    sink: &mut S,
    segment: &SegmentDescriptor,
    routing_codec_version: u16,
    ranges: &[DocIdRange],
) -> Result<SegmentDescriptor, IndexError>
where
    D: ArtifactDirectoryRead,
    S: ComponentBatchSink,
{
    segment.validate()?;
    if ranges.is_empty() {
        return Ok(segment.clone());
    }
    let mut previous_end = 0u32;
    for range in ranges {
        let end = range
            .first_doc_id
            .get()
            .checked_add(range.count)
            .ok_or(IndexError::OffsetOverflow)?;
        if range.segment_id != segment.identity.segment_id
            || range.count == 0
            || range.first_doc_id.get() < previous_end
            || end > segment.document_count
        {
            return Err(IndexError::InvalidDefinition(
                "live-mask rewrite ranges must name this segment and be ordered, disjoint, and in bounds"
                    .into(),
            ));
        }
        previous_end = end;
    }
    let component_index = segment
        .components
        .binary_search_by_key(&(ComponentKind::LIVE_MASK, None, 0), |component| {
            (component.role, component.field_id, component.ordinal)
        })
        .map_err(|_| IndexError::InvalidFormat("segment has no canonical live-mask stream"))?;
    let component = &segment.components[component_index];
    let mut stream = ComponentStream::new(
        directory,
        segment.identity,
        ComponentKind::LIVE_MASK,
        component.artifact.clone(),
        None,
        None,
    )?;
    let mut output = None::<StreamingComponentPublisher<'_, S>>;
    let mut cleared = 0u32;
    let mut range_cursor = 0usize;
    while let Some(leaf) = stream.next_leaf().await? {
        if output.is_none() {
            output = Some(StreamingComponentPublisher::new(
                sink,
                segment.identity,
                ComponentKind::LIVE_MASK,
                leaf.descriptor.codec_version,
                routing_codec_version,
            )?);
        }
        let output = output.as_mut().expect("live-mask output was opened");
        let first = decode_doc_key(&leaf.minimum_key)?;
        let last = decode_doc_key(&leaf.maximum_key)?;
        let leaf_end = last.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        while range_cursor < ranges.len()
            && ranges[range_cursor]
                .first_doc_id
                .get()
                .checked_add(ranges[range_cursor].count)
                .ok_or(IndexError::OffsetOverflow)?
                <= first
        {
            range_cursor += 1;
        }
        let mut range_end = range_cursor;
        while range_end < ranges.len() && ranges[range_end].first_doc_id.get() < leaf_end {
            range_end += 1;
        }
        if range_cursor == range_end {
            output
                .push_descriptor_leaf(DescriptorLeaf {
                    minimum_key: leaf.minimum_key,
                    maximum_key: leaf.maximum_key,
                    element_count: leaf.element_count,
                    descriptor: leaf.descriptor,
                })
                .await?;
            continue;
        }
        let loaded = read_artifact_component(
            directory,
            segment.identity,
            &leaf.descriptor,
            ComponentKind::LIVE_MASK,
        )
        .await?;
        let block = LiveMaskBlock::decode_payload(&loaded.payload)?;
        let affected = ranges[range_cursor..range_end].iter().map(|range| {
            let range_first = range.first_doc_id.get().max(first);
            let range_end = range
                .first_doc_id
                .get()
                .checked_add(range.count)
                .expect("validated live-mask range");
            (
                DocId::new(range_first),
                range_end.min(leaf_end) - range_first,
            )
        });
        let (block, newly_cleared) = block.clear_ranges(affected)?;
        cleared = cleared
            .checked_add(newly_cleared)
            .ok_or(IndexError::OffsetOverflow)?;
        let payload = block.encode_payload()?;
        output
            .push_payload(
                leaf.minimum_key,
                leaf.maximum_key,
                leaf.element_count,
                payload,
            )
            .await?;
    }
    let old_totals = stream.traversed_totals();
    let replacement = output
        .ok_or(IndexError::InvalidFormat(
            "segment live-mask stream is empty",
        ))?
        .finish()
        .await?;
    let mut components = segment.components.clone();
    components[component_index].artifact = replacement.root;
    let encoded_bytes = replace_total(
        segment.encoded_bytes,
        old_totals.encoded_bytes,
        replacement.encoded_bytes,
    )?;
    let logical_bytes = replace_total(
        segment.logical_bytes,
        old_totals.logical_bytes,
        replacement.logical_bytes,
    )?;
    SegmentDescriptor::new(
        segment.identity,
        segment.document_count,
        segment
            .live_document_count
            .checked_sub(cleared)
            .ok_or(IndexError::InvalidFormat("live-mask count underflow"))?,
        components,
        encoded_bytes,
        logical_bytes,
    )
}

fn decode_doc_key(key: &[u8]) -> Result<u32, IndexError> {
    let bytes: [u8; 4] = key
        .try_into()
        .map_err(|_| IndexError::InvalidFormat("live-mask routing key"))?;
    Ok(u32::from_be_bytes(bytes))
}

fn replace_total(total: u64, old: u64, new: u64) -> Result<u64, IndexError> {
    total
        .checked_sub(old)
        .and_then(|value| value.checked_add(new))
        .ok_or(IndexError::OffsetOverflow)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::IndexFileRead;
    use crate::v4::build::{ComponentLeaf, ExactMemorySink, PublishedObject, publish_stream};
    use crate::v4::{
        DocumentIdentity, IdentityBlock, ObjectIdentity, SegmentComponent, SegmentStatistics,
        encode_component,
    };

    struct MemoryDirectory {
        objects: BTreeMap<String, PublishedObject>,
    }

    struct MemoryFile(Arc<[u8]>);

    impl IndexFileRead for MemoryFile {
        type Slice = Arc<[u8]>;

        async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
            let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
            let end = start.saturating_add(max_length).min(self.0.len());
            Ok(self.0.get(start..end).unwrap_or_default().to_vec().into())
        }
    }

    impl ArtifactDirectoryRead for MemoryDirectory {
        type File = MemoryFile;

        async fn open_artifact(
            &self,
            descriptor: &ArtifactDescriptor,
        ) -> Result<Self::File, IndexError> {
            let object = self
                .objects
                .get(&descriptor.path)
                .ok_or_else(|| IndexError::FileNotFound(descriptor.path.clone()))?;
            if object.object_version != descriptor.object_version {
                return Err(IndexError::Integrity);
            }
            let start =
                usize::try_from(descriptor.offset).map_err(|_| IndexError::OffsetOverflow)?;
            let length = usize::try_from(descriptor.encoded_length)
                .map_err(|_| IndexError::OffsetOverflow)?;
            let end = start
                .checked_add(length)
                .ok_or(IndexError::OffsetOverflow)?;
            Ok(MemoryFile(
                object
                    .bytes
                    .get(start..end)
                    .ok_or(IndexError::Integrity)?
                    .to_vec()
                    .into(),
            ))
        }
    }

    fn directory(sink: &ExactMemorySink) -> MemoryDirectory {
        MemoryDirectory {
            objects: sink.objects().clone(),
        }
    }

    #[tokio::test]
    async fn locator_resolution_uses_highest_version_across_deltas() {
        let first = SegmentIdentity::new(9, 1, [7; 32], 11).unwrap();
        let second = SegmentIdentity::new(9, 1, [7; 32], 12).unwrap();
        let mut sink = ExactMemorySink::new();
        let live = publish_locator_delta(
            &mut sink,
            first,
            1,
            1,
            vec![LocatorEntry {
                path: "objects/a".into(),
                value: LocatorValue::Live {
                    object_version: 4,
                    ranges: vec![super::super::DocIdRange {
                        segment_id: 11,
                        first_doc_id: DocId::new(0),
                        count: 1,
                    }],
                },
            }],
        )
        .await
        .unwrap();
        let deleted = publish_locator_delta(
            &mut sink,
            second,
            1,
            1,
            vec![LocatorEntry {
                path: "objects/a".into(),
                value: LocatorValue::Deleted {
                    tombstone_version: 5,
                },
            }],
        )
        .await
        .unwrap();
        let roots = vec![
            LocatorStreamRoot {
                sequence: 2,
                identity: second,
                artifact: deleted.root,
            },
            LocatorStreamRoot {
                sequence: 1,
                identity: first,
                artifact: live.root,
            },
        ];
        assert_eq!(
            locate_path(&directory(&sink), &roots, "objects/a")
                .await
                .unwrap(),
            Some(LocatorValue::Deleted {
                tombstone_version: 5
            })
        );
    }

    #[tokio::test]
    async fn newer_sequence_wins_an_equal_version_live_relocation() {
        let first = SegmentIdentity::new(9, 1, [7; 32], 11).unwrap();
        let second = SegmentIdentity::new(9, 1, [7; 32], 12).unwrap();
        let mut sink = ExactMemorySink::new();
        let mut roots = Vec::new();
        for (sequence, identity, segment_id) in [(1, first, 11), (2, second, 12)] {
            let stream = publish_locator_delta(
                &mut sink,
                identity,
                1,
                1,
                vec![LocatorEntry {
                    path: "objects/a".into(),
                    value: LocatorValue::Live {
                        object_version: 4,
                        ranges: vec![super::super::DocIdRange {
                            segment_id,
                            first_doc_id: DocId::new(0),
                            count: 1,
                        }],
                    },
                }],
            )
            .await
            .unwrap();
            roots.push(LocatorStreamRoot {
                sequence,
                identity,
                artifact: stream.root,
            });
        }
        let Some(LocatorValue::Live { ranges, .. }) =
            locate_path(&directory(&sink), &roots, "objects/a")
                .await
                .unwrap()
        else {
            panic!("relocated source must remain live")
        };
        assert_eq!(ranges[0].segment_id, 12);
    }

    #[tokio::test]
    async fn locator_wave_merge_seeks_multiple_roots_and_omits_misses() {
        let first = SegmentIdentity::new(9, 1, [7; 32], 11).unwrap();
        let second = SegmentIdentity::new(9, 1, [7; 32], 12).unwrap();
        let mut sink = ExactMemorySink::new();
        let first_stream = publish_locator_delta(
            &mut sink,
            first,
            1,
            1,
            vec![
                LocatorEntry {
                    path: "objects/a".into(),
                    value: LocatorValue::Live {
                        object_version: 4,
                        ranges: vec![super::super::DocIdRange {
                            segment_id: 11,
                            first_doc_id: DocId::new(0),
                            count: 1,
                        }],
                    },
                },
                LocatorEntry {
                    path: "objects/m".into(),
                    value: LocatorValue::Deleted {
                        tombstone_version: 8,
                    },
                },
            ],
        )
        .await
        .unwrap();
        let second_stream = publish_locator_delta(
            &mut sink,
            second,
            1,
            1,
            vec![
                LocatorEntry {
                    path: "objects/a".into(),
                    value: LocatorValue::Live {
                        object_version: 4,
                        ranges: vec![super::super::DocIdRange {
                            segment_id: 12,
                            first_doc_id: DocId::new(7),
                            count: 1,
                        }],
                    },
                },
                LocatorEntry {
                    path: "objects/z".into(),
                    value: LocatorValue::Live {
                        object_version: 2,
                        ranges: vec![super::super::DocIdRange {
                            segment_id: 12,
                            first_doc_id: DocId::new(8),
                            count: 1,
                        }],
                    },
                },
            ],
        )
        .await
        .unwrap();
        let roots = vec![
            LocatorStreamRoot {
                sequence: 1,
                identity: first,
                artifact: first_stream.root,
            },
            LocatorStreamRoot {
                sequence: 2,
                identity: second,
                artifact: second_stream.root,
            },
        ];
        let paths = ('a'..='z')
            .map(|suffix| format!("objects/{suffix}"))
            .collect::<Vec<_>>();
        let borrowed = paths.iter().map(String::as_str).collect::<Vec<_>>();
        let fixed = locator_result_fixed_resident_bytes(paths.len(), paths.len(), paths.len())
            .expect("locator result fixed reserve");
        let two_live_ranges = 2 * std::mem::size_of::<DocIdRange>();
        assert!(matches!(
            locate_path_values(
                &directory(&sink),
                &roots,
                &borrowed,
                fixed + two_live_ranges - 1,
            )
            .await,
            Err(IndexError::ResourceLimit { needed, limit })
                if needed == fixed + two_live_ranges && limit == needed - 1
        ));
        let ordinal = locate_path_values(&directory(&sink), &roots, &borrowed, usize::MAX)
            .await
            .unwrap();
        assert_eq!(ordinal.len(), paths.len());
        assert!(matches!(
            ordinal[0],
            Some(LocatorValue::Live { ref ranges, .. }) if ranges[0].segment_id == 12
        ));
        assert!(ordinal[1].is_none());
        assert_eq!(
            ordinal[12],
            Some(LocatorValue::Deleted {
                tombstone_version: 8
            })
        );
        assert!(matches!(ordinal[25], Some(LocatorValue::Live { .. })));

        let located = locate_paths(&directory(&sink), &roots, &paths)
            .await
            .unwrap();
        assert_eq!(located.len(), 3);
        assert!(matches!(
            located["objects/a"],
            LocatorValue::Live { ref ranges, .. } if ranges[0].segment_id == 12
        ));
        assert_eq!(
            located["objects/m"],
            LocatorValue::Deleted {
                tombstone_version: 8
            }
        );
        assert!(matches!(located["objects/z"], LocatorValue::Live { .. }));
    }

    #[tokio::test]
    async fn locator_wave_rejects_unsorted_or_duplicate_paths_before_io() {
        let directory = MemoryDirectory {
            objects: BTreeMap::new(),
        };
        assert!(
            locate_paths(&directory, &[], &["b".into(), "a".into()])
                .await
                .is_err()
        );
        assert!(
            locate_paths(&directory, &[], &["a".into(), "a".into()])
                .await
                .is_err()
        );
        assert!(
            locate_path_values(&directory, &[], &["b", "a"], usize::MAX)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn locator_wave_enforces_its_exact_fixed_result_reserve() {
        let directory = MemoryDirectory {
            objects: BTreeMap::new(),
        };
        let paths = ["objects/a"];
        let exact = locator_result_fixed_resident_bytes(1, 1, 1).unwrap();
        assert!(matches!(
            locate_path_values(&directory, &[], &paths, exact - 1).await,
            Err(IndexError::ResourceLimit { needed, limit })
                if needed == exact && limit == exact - 1
        ));
        assert_eq!(
            locate_path_values(&directory, &[], &paths, exact)
                .await
                .unwrap(),
            vec![None]
        );
    }

    #[tokio::test]
    async fn locator_compaction_retains_tombstones_and_latest_relocations() {
        let mut sink = ExactMemorySink::new();
        let mut roots = Vec::new();
        for (sequence, segment_id, entries) in [
            (
                1,
                11,
                vec![
                    LocatorEntry {
                        path: "objects/a".into(),
                        value: LocatorValue::Live {
                            object_version: 4,
                            ranges: vec![super::super::DocIdRange {
                                segment_id: 11,
                                first_doc_id: DocId::new(0),
                                count: 1,
                            }],
                        },
                    },
                    LocatorEntry {
                        path: "objects/deleted".into(),
                        value: LocatorValue::Live {
                            object_version: 7,
                            ranges: vec![super::super::DocIdRange {
                                segment_id: 11,
                                first_doc_id: DocId::new(1),
                                count: 1,
                            }],
                        },
                    },
                ],
            ),
            (
                2,
                12,
                vec![
                    LocatorEntry {
                        path: "objects/a".into(),
                        value: LocatorValue::Live {
                            object_version: 4,
                            ranges: vec![super::super::DocIdRange {
                                segment_id: 12,
                                first_doc_id: DocId::new(3),
                                count: 1,
                            }],
                        },
                    },
                    LocatorEntry {
                        path: "objects/deleted".into(),
                        value: LocatorValue::Deleted {
                            tombstone_version: 8,
                        },
                    },
                ],
            ),
            (
                3,
                13,
                vec![LocatorEntry {
                    path: "objects/z".into(),
                    value: LocatorValue::Live {
                        object_version: 2,
                        ranges: vec![super::super::DocIdRange {
                            segment_id: 13,
                            first_doc_id: DocId::new(0),
                            count: 1,
                        }],
                    },
                }],
            ),
            (
                4,
                14,
                vec![LocatorEntry {
                    path: "objects/a".into(),
                    value: LocatorValue::Live {
                        object_version: 4,
                        ranges: vec![super::super::DocIdRange {
                            segment_id: 14,
                            first_doc_id: DocId::new(2),
                            count: 1,
                        }],
                    },
                }],
            ),
        ] {
            let identity = SegmentIdentity::new(9, 1, [7; 32], segment_id).unwrap();
            let stream = publish_locator_delta(&mut sink, identity, 1, 1, entries)
                .await
                .unwrap();
            roots.push(LocatorStreamRoot {
                sequence,
                identity,
                artifact: stream.root,
            });
        }

        let input = directory(&sink);
        let output_identity = SegmentIdentity::new(9, 1, [7; 32], 20).unwrap();
        let compacted =
            compact_locator_roots(&input, &mut sink, &roots[..3], output_identity, 1, 1)
                .await
                .unwrap();
        let compacted_root = LocatorStreamRoot {
            sequence: 3,
            identity: output_identity,
            artifact: compacted.root,
        };
        let output = directory(&sink);

        let located = locate_paths(
            &output,
            std::slice::from_ref(&compacted_root),
            &[
                "objects/a".into(),
                "objects/deleted".into(),
                "objects/z".into(),
            ],
        )
        .await
        .unwrap();
        assert!(matches!(
            located["objects/a"],
            LocatorValue::Live { ref ranges, .. } if ranges[0].segment_id == 12
        ));
        assert_eq!(
            located["objects/deleted"],
            LocatorValue::Deleted {
                tombstone_version: 8
            }
        );
        assert!(matches!(located["objects/z"], LocatorValue::Live { .. }));

        let with_newer = locate_path(&output, &[compacted_root, roots[3].clone()], "objects/a")
            .await
            .unwrap();
        assert!(matches!(
            with_newer,
            Some(LocatorValue::Live { ref ranges, .. }) if ranges[0].segment_id == 14
        ));
    }

    #[tokio::test]
    async fn live_mask_rewrite_reuses_unaffected_leaves() {
        let identity = SegmentIdentity::new(9, 1, [7; 32], 11).unwrap();
        let mut sink = ExactMemorySink::new();
        let identity_block = IdentityBlock::new(
            DocId::MIN,
            (0u32..4)
                .map(|version| DocumentIdentity {
                    source: ObjectIdentity {
                        path: format!("objects/{version}"),
                        version: u64::from(version) + 1,
                    },
                    source_record: 0,
                    result: None,
                })
                .collect(),
        )
        .unwrap();
        let identities = publish_one(
            &mut sink,
            identity,
            ComponentKind::IDENTITY_TABLE,
            0,
            3,
            4,
            identity_block.encode_payload().unwrap(),
        )
        .await;
        let mask = super::super::LiveMask::all_live(4).unwrap();
        let live = publish_one(
            &mut sink,
            identity,
            ComponentKind::LIVE_MASK,
            0,
            3,
            4,
            mask.blocks()[0].encode_payload().unwrap(),
        )
        .await;
        let statistics = SegmentStatistics::new(4, 4, 0, None, Vec::new(), Vec::new()).unwrap();
        let stats = publish_one(
            &mut sink,
            identity,
            ComponentKind::SCORING_STATISTICS,
            0,
            0,
            1,
            statistics.encode_payload().unwrap(),
        )
        .await;
        let encoded_bytes = identities.encoded_bytes + live.encoded_bytes + stats.encoded_bytes;
        let logical_bytes = identities.logical_bytes + live.logical_bytes + stats.logical_bytes;
        let segment = SegmentDescriptor::new(
            identity,
            4,
            4,
            vec![
                SegmentComponent {
                    role: ComponentKind::IDENTITY_TABLE,
                    field_id: None,
                    ordinal: 0,
                    artifact: identities.root,
                },
                SegmentComponent {
                    role: ComponentKind::LIVE_MASK,
                    field_id: None,
                    ordinal: 0,
                    artifact: live.root,
                },
                SegmentComponent {
                    role: ComponentKind::SCORING_STATISTICS,
                    field_id: None,
                    ordinal: 0,
                    artifact: stats.root,
                },
            ],
            encoded_bytes,
            logical_bytes,
        )
        .unwrap();
        let original = directory(&sink);
        let rewritten = rewrite_segment_live_mask(
            &original,
            &mut sink,
            &segment,
            1,
            &[DocIdRange {
                segment_id: identity.segment_id,
                first_doc_id: DocId::new(2),
                count: 1,
            }],
        )
        .await
        .unwrap();
        assert_eq!(rewritten.live_document_count, 3);
        let rewritten_directory = directory(&sink);
        let blocks = super::super::SegmentComponentReader::new(&rewritten_directory, &rewritten)
            .unwrap()
            .live_mask_blocks(None, None)
            .await
            .unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].is_live(DocId::new(2)), Some(false));
        assert_eq!(blocks[0].is_live(DocId::new(3)), Some(true));
    }

    async fn publish_one(
        sink: &mut ExactMemorySink,
        identity: SegmentIdentity,
        kind: ComponentKind,
        first: u32,
        last: u32,
        count: u64,
        payload: Vec<u8>,
    ) -> PublishedStream {
        let logical = payload.len() as u64;
        publish_stream(
            sink,
            identity,
            1,
            vec![ComponentLeaf {
                minimum_key: first.to_be_bytes().to_vec(),
                maximum_key: last.to_be_bytes().to_vec(),
                element_count: count,
                component: encode_component(identity, kind, 1, 0, logical, payload).unwrap(),
            }],
        )
        .await
        .unwrap()
    }
}
