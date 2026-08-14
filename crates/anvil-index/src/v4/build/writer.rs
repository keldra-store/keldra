mod layout;
mod statistics;
mod streams;
mod terms;

#[cfg(test)]
mod tests;

use crate::IndexError;

use self::layout::{
    WriterCharge, build_document_layout, build_source_doc_refs, build_term_refs,
    charged_vec_capacity,
};
use self::statistics::StatisticsAccumulator;
use self::streams::{
    publish_fast_columns, publish_identities, publish_live_mask, publish_locator, publish_norms,
    publish_stored_fields, publish_vectors,
};
use self::terms::publish_terms;
use super::super::{
    ComponentKind, ComponentStatistics, FieldComponents, FieldId, IndexSemantics, Schema,
    SegmentComponent, SegmentDescriptor, SegmentIdentity,
};
use super::{
    BuildLimits, ComponentBatchSink, ProjectedRecord, ProjectedSource, PublishedStream, SourcePush,
    StreamingComponentPublisher,
};

#[derive(Debug)]
pub struct BuiltSegment {
    pub descriptor: SegmentDescriptor,
    pub locator: PublishedStream,
    pub source_count: u64,
}

/// Bounded format-v4 segment writer. Source admission includes the worst
/// retained flat-reference phase required to seal the accepted projection.
/// No seal phase allocates a corpus-sized structure which was not charged at
/// admission.
pub struct NativeSegmentWriter {
    identity: SegmentIdentity,
    schema: Schema,
    limits: BuildLimits,
    sources: Vec<ProjectedSource>,
    source_path_ordinals: Vec<u32>,
    charge: WriterCharge,
}

impl NativeSegmentWriter {
    pub fn new(
        identity: SegmentIdentity,
        schema: Schema,
        limits: BuildLimits,
    ) -> Result<Self, IndexError> {
        identity.validate()?;
        limits.validate()?;
        if schema.fingerprint()? != identity.schema_fingerprint {
            return Err(IndexError::InvalidDefinition(
                "segment identity does not match the schema fingerprint".into(),
            ));
        }
        for required in [
            ComponentKind::ROUTING_NODE,
            ComponentKind::IDENTITY_TABLE,
            ComponentKind::LIVE_MASK,
            ComponentKind::PATH_LOCATOR,
            ComponentKind::SCORING_STATISTICS,
        ] {
            schema.codec_version(required)?;
        }
        let charge = WriterCharge::for_schema(&schema)?;
        Ok(Self {
            identity,
            schema,
            limits,
            sources: Vec::new(),
            source_path_ordinals: Vec::new(),
            charge,
        })
    }

    pub fn buffered_source_bytes(&self) -> usize {
        self.charge.peak_bytes().unwrap_or(usize::MAX)
    }

    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    pub fn source_version(&self, path: &str) -> Option<u64> {
        self.source_path_ordinals
            .binary_search_by(|ordinal| {
                self.sources[*ordinal as usize]
                    .source_identity
                    .path
                    .as_str()
                    .cmp(path)
            })
            .ok()
            .map(|position| {
                self.sources[self.source_path_ordinals[position] as usize]
                    .source_identity
                    .version
            })
    }

    pub fn push_source(&mut self, source: ProjectedSource) -> Result<SourcePush, IndexError> {
        source.validate()?;
        for record in &source.records {
            validate_record_schema(&self.schema, record)?;
        }
        let path_position = self
            .source_path_ordinals
            .binary_search_by(|ordinal| {
                self.sources[*ordinal as usize]
                    .source_identity
                    .path
                    .cmp(&source.source_identity.path)
            })
            .map_or_else(Ok, |_| {
                Err(IndexError::InvalidDefinition(
                    "one native segment cannot contain the same source path twice".into(),
                ))
            })?;
        let required_sources = self
            .sources
            .len()
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        let charged_source_capacity =
            charged_vec_capacity(self.sources.capacity(), required_sources)?;
        let charged_path_ordinal_capacity =
            charged_vec_capacity(self.source_path_ordinals.capacity(), required_sources)?;
        let next = self.charge.with_source(
            &source,
            charged_source_capacity,
            charged_path_ordinal_capacity,
        )?;
        let needed = next.peak_bytes()?;
        if needed > self.limits.maximum_buffered_source_bytes() {
            if self.sources.is_empty() {
                return Err(IndexError::ResourceLimit {
                    needed,
                    limit: self.limits.maximum_buffered_source_bytes(),
                });
            }
            return Ok(SourcePush::Full(source));
        }
        if self.sources.capacity() < required_sources {
            self.sources
                .try_reserve_exact(1)
                .map_err(|_| IndexError::ResourceLimit {
                    needed,
                    limit: self.limits.maximum_buffered_source_bytes(),
                })?;
        }
        if self.source_path_ordinals.capacity() < required_sources {
            self.source_path_ordinals
                .try_reserve_exact(1)
                .map_err(|_| IndexError::ResourceLimit {
                    needed,
                    limit: self.limits.maximum_buffered_source_bytes(),
                })?;
        }
        let next = self.charge.with_source(
            &source,
            self.sources.capacity(),
            self.source_path_ordinals.capacity(),
        )?;
        let needed = next.peak_bytes()?;
        if needed > self.limits.maximum_buffered_source_bytes() {
            return Err(IndexError::ResourceLimit {
                needed,
                limit: self.limits.maximum_buffered_source_bytes(),
            });
        }
        let source_ordinal =
            u32::try_from(self.sources.len()).map_err(|_| IndexError::OffsetOverflow)?;
        self.sources.push(source);
        self.source_path_ordinals
            .insert(path_position, source_ordinal);
        self.charge = next;
        Ok(SourcePush::Accepted)
    }

    pub async fn seal<S: ComponentBatchSink>(
        self,
        sink: &mut S,
    ) -> Result<BuiltSegment, IndexError> {
        if self.sources.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "cannot seal an empty native segment".into(),
            ));
        }
        let source_count =
            u64::try_from(self.sources.len()).map_err(|_| IndexError::OffsetOverflow)?;
        let documents = build_document_layout(&self.sources, self.charge.document_count())?;
        let document_count =
            u32::try_from(documents.len()).map_err(|_| IndexError::ResourceLimit {
                needed: documents.len(),
                limit: u32::MAX as usize,
            })?;
        let locator_references = build_source_doc_refs(&self.sources, &documents)?;
        let routing_codec = self.schema.codec_version(ComponentKind::ROUTING_NODE)?;
        let mut statistics =
            StatisticsAccumulator::from_documents(&self.schema, &self.sources, &documents)?;
        let mut assembly = SegmentAssembly::new(&self.schema)?;

        publish_identities(
            sink,
            self.identity,
            &self.schema,
            routing_codec,
            &self.sources,
            &documents,
            &mut assembly,
        )
        .await?;
        publish_live_mask(
            sink,
            self.identity,
            &self.schema,
            routing_codec,
            document_count,
            &mut assembly,
        )
        .await?;
        let locator = publish_locator(
            sink,
            self.identity,
            &self.schema,
            routing_codec,
            &self.sources,
            &locator_references,
        )
        .await?;
        drop(locator_references);

        let term_references = build_term_refs(&self.sources, &documents, self.charge.term_count())?;
        publish_terms(
            sink,
            self.identity,
            &self.schema,
            routing_codec,
            &self.sources,
            &documents,
            &term_references,
            &mut statistics,
            &mut assembly,
        )
        .await?;
        drop(term_references);

        publish_fast_columns(
            sink,
            self.identity,
            &self.schema,
            routing_codec,
            &self.sources,
            &documents,
            &mut assembly,
        )
        .await?;
        publish_stored_fields(
            sink,
            self.identity,
            &self.schema,
            routing_codec,
            &self.sources,
            &documents,
            &mut assembly,
        )
        .await?;
        publish_vectors(
            sink,
            self.identity,
            &self.schema,
            routing_codec,
            &self.sources,
            &documents,
            &mut assembly,
        )
        .await?;
        publish_norms(
            sink,
            self.identity,
            &self.schema,
            routing_codec,
            &self.sources,
            &documents,
            &mut assembly,
        )
        .await?;

        assembly
            .component_statistics
            .sort_by_key(|value| (value.role, value.field_id));
        let component_statistics = std::mem::take(&mut assembly.component_statistics);
        let statistics = statistics.finish(
            source_count,
            &self.schema,
            &self.sources,
            &documents,
            component_statistics,
        )?;
        let statistics_payload = statistics.encode_payload()?;
        drop(statistics);
        let mut publisher = StreamingComponentPublisher::new(
            sink,
            self.identity,
            ComponentKind::SCORING_STATISTICS,
            self.schema
                .codec_version(ComponentKind::SCORING_STATISTICS)?,
            routing_codec,
        )?;
        publisher
            .push_payload(
                b"statistics".to_vec(),
                b"statistics".to_vec(),
                1,
                statistics_payload,
            )
            .await?;
        assembly.add(
            ComponentKind::SCORING_STATISTICS,
            None,
            publisher.finish().await?,
        )?;
        assembly
            .components
            .sort_by_key(|component| (component.role, component.field_id, component.ordinal));

        Ok(BuiltSegment {
            descriptor: SegmentDescriptor::new(
                self.identity,
                document_count,
                document_count,
                assembly.components,
                assembly.encoded_bytes,
                assembly.logical_bytes,
            )?,
            locator,
            source_count,
        })
    }
}

struct SegmentAssembly {
    components: Vec<SegmentComponent>,
    component_statistics: Vec<ComponentStatistics>,
    encoded_bytes: u64,
    logical_bytes: u64,
}

impl SegmentAssembly {
    fn new(schema: &Schema) -> Result<Self, IndexError> {
        let shape = schema.segment_shape()?;
        Ok(Self {
            components: layout::charged_vec(shape.component_count)?,
            component_statistics: layout::charged_vec(shape.component_statistics_count)?,
            encoded_bytes: 0,
            logical_bytes: 0,
        })
    }

    fn add(
        &mut self,
        role: ComponentKind,
        field_id: Option<FieldId>,
        stream: PublishedStream,
    ) -> Result<(), IndexError> {
        if let Some(summary) = stream.statistics(role, field_id)? {
            self.component_statistics.push(summary);
        }
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(stream.encoded_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        self.logical_bytes = self
            .logical_bytes
            .checked_add(stream.logical_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        self.components.push(SegmentComponent {
            role,
            field_id,
            ordinal: 0,
            artifact: stream.root,
        });
        Ok(())
    }
}

async fn push_payload<S: ComponentBatchSink>(
    publisher: &mut StreamingComponentPublisher<'_, S>,
    first: u32,
    count: u32,
    payload: Vec<u8>,
) -> Result<(), IndexError> {
    let last = first
        .checked_add(count - 1)
        .ok_or(IndexError::OffsetOverflow)?;
    publisher
        .push_payload(
            first.to_be_bytes().to_vec(),
            last.to_be_bytes().to_vec(),
            u64::from(count),
            payload,
        )
        .await
}

fn validate_record_schema(schema: &Schema, record: &ProjectedRecord) -> Result<(), IndexError> {
    for term in &record.terms {
        let field = require_field_component(schema, term.field_id, FieldComponents::TERMS)?;
        if !term.positions.is_empty() && !field.components.contains(FieldComponents::POSITIONS) {
            return Err(IndexError::InvalidDefinition(
                "projected term positions require a positions component".into(),
            ));
        }
    }
    for column in &record.columns {
        let field = require_field_component(schema, column.field_id, FieldComponents::FAST_COLUMN)?;
        if column.multi_valued != (field.cardinality == super::super::Cardinality::Multi) {
            return Err(IndexError::InvalidDefinition(
                "projected column cardinality differs from its schema".into(),
            ));
        }
    }
    for vector in &record.vectors {
        require_field_component(schema, vector.field_id, FieldComponents::VECTOR)?;
        let dimensions = match &schema.semantics {
            IndexSemantics::Vector { dimensions, .. }
            | IndexSemantics::Hybrid { dimensions, .. } => dimensions,
            _ => {
                return Err(IndexError::InvalidDefinition(
                    "vector projection requires vector index semantics".into(),
                ));
            }
        };
        if vector.values.len() != *dimensions as usize {
            return Err(IndexError::InvalidDefinition(
                "projected vector dimensions differ from its schema".into(),
            ));
        }
    }
    for (field_id, _) in &record.field_lengths {
        require_field_component(schema, *field_id, FieldComponents::NORMS)?;
    }
    if record.stored_fields.is_some()
        && !schema
            .fields
            .iter()
            .any(|field| field.components.contains(FieldComponents::STORED))
    {
        return Err(IndexError::InvalidDefinition(
            "stored projection has no stored schema field".into(),
        ));
    }
    Ok(())
}

fn require_field_component(
    schema: &Schema,
    field_id: FieldId,
    component: FieldComponents,
) -> Result<&super::super::FieldSchema, IndexError> {
    let field = schema
        .fields
        .get(field_id.get() as usize)
        .ok_or_else(|| IndexError::InvalidDefinition("projected field is not in schema".into()))?;
    if !field.components.contains(component) {
        return Err(IndexError::InvalidDefinition(
            "projected field component is not in schema".into(),
        ));
    }
    Ok(field)
}
