use std::collections::BTreeSet;

use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Encoder};
use super::model::{ComponentKind, INDEX_COMPONENT_BYTES, INDEX_ROUTING_KEY_BYTES};

const SCHEMA_DOMAIN: &[u8] = b"anvil.index.schema.v4";
const STATISTICS_FIXED_PAYLOAD_BYTES: usize = 35;
const STATISTICS_PHYSICAL_BOUNDS_BYTES: usize = 2 * (4 + INDEX_ROUTING_KEY_BYTES);
const STATISTICS_FIELD_BYTES: usize = 103;
const STATISTICS_LENGTH_OPTIONS_BYTES: usize = 8;
const STATISTICS_VECTOR_DIMENSIONS_BYTES: usize = 4;
const STATISTICS_FIELD_COMPONENT_BYTES: usize = 47;
const STATISTICS_GLOBAL_COMPONENT_BYTES: usize = 43;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct FieldId(u32);

impl FieldId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum IndexKind {
    Path = 1,
    MetadataFilter = 2,
    TypedJson = 3,
    FullText = 4,
    Vector = 5,
    Hybrid = 6,
    GitSource = 7,
    Tensor = 8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct ScalarDomain(u8);

impl ScalarDomain {
    pub const BOOLEAN: Self = Self(1 << 0);
    pub const NUMBER: Self = Self(1 << 1);
    pub const UNSIGNED: Self = Self(1 << 2);
    pub const STRING: Self = Self(1 << 3);
    pub const NULL: Self = Self(1 << 4);
    pub const ALL_JSON: Self = Self((1 << 5) - 1);

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }

    fn validate(self) -> Result<(), IndexError> {
        if self.0 == 0 || self.0 & !Self::ALL_JSON.0 != 0 {
            return Err(IndexError::InvalidDefinition(
                "field scalar domain is empty or unknown".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct FieldComponents(u16);

impl FieldComponents {
    pub const TERMS: Self = Self(1 << 0);
    pub const FAST_COLUMN: Self = Self(1 << 1);
    pub const STORED: Self = Self(1 << 2);
    pub const POSITIONS: Self = Self(1 << 3);
    pub const NORMS: Self = Self(1 << 4);
    pub const VECTOR: Self = Self(1 << 5);
    const ALL: u16 = (1 << 6) - 1;

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    fn validate(self) -> Result<(), IndexError> {
        if self.0 == 0 || self.0 & !Self::ALL != 0 {
            return Err(IndexError::InvalidDefinition(
                "field components are empty or unknown".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Cardinality {
    Single = 1,
    Multi = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Collation {
    BinaryUtf8 = 1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldSchema {
    pub id: FieldId,
    pub name: String,
    pub source_selector: String,
    pub domain: ScalarDomain,
    pub cardinality: Cardinality,
    pub allow_missing: bool,
    pub allow_null: bool,
    pub collation: Collation,
    pub components: FieldComponents,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Analyzer {
    Keyword = 1,
    UnicodeAlphanumericLowercase = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum VectorMetric {
    DotProduct = 1,
    Cosine = 2,
    Euclidean = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum VectorNormalization {
    None = 1,
    L2 = 2,
}

#[derive(Clone, Debug, PartialEq)]
pub enum IndexSemantics {
    Path,
    MetadataFilter,
    TypedJson,
    FullText {
        analyzer: Analyzer,
        bm25_k1: f64,
        bm25_b: f64,
    },
    Vector {
        dimensions: u32,
        metric: VectorMetric,
        normalization: VectorNormalization,
    },
    Hybrid {
        analyzer: Analyzer,
        bm25_k1: f64,
        bm25_b: f64,
        dimensions: u32,
        metric: VectorMetric,
        normalization: VectorNormalization,
        lexical_weight: f64,
        vector_weight: f64,
    },
    GitSource {
        repository_scope: String,
    },
    Tensor {
        model_scope: String,
    },
}

impl IndexSemantics {
    fn kind(&self) -> IndexKind {
        match self {
            Self::Path => IndexKind::Path,
            Self::MetadataFilter => IndexKind::MetadataFilter,
            Self::TypedJson => IndexKind::TypedJson,
            Self::FullText { .. } => IndexKind::FullText,
            Self::Vector { .. } => IndexKind::Vector,
            Self::Hybrid { .. } => IndexKind::Hybrid,
            Self::GitSource { .. } => IndexKind::GitSource,
            Self::Tensor { .. } => IndexKind::Tensor,
        }
    }

    fn validate(&self) -> Result<(), IndexError> {
        match self {
            Self::FullText {
                bm25_k1, bm25_b, ..
            } => validate_bm25(*bm25_k1, *bm25_b),
            Self::Vector { dimensions, .. } => validate_dimensions(*dimensions),
            Self::Hybrid {
                bm25_k1,
                bm25_b,
                dimensions,
                lexical_weight,
                vector_weight,
                ..
            } => {
                validate_bm25(*bm25_k1, *bm25_b)?;
                validate_dimensions(*dimensions)?;
                if !lexical_weight.is_finite()
                    || !vector_weight.is_finite()
                    || *lexical_weight < 0.0
                    || *vector_weight < 0.0
                    || *lexical_weight + *vector_weight <= 0.0
                {
                    return Err(IndexError::InvalidDefinition(
                        "hybrid weights must be finite, non-negative, and not both zero".into(),
                    ));
                }
                Ok(())
            }
            Self::GitSource { repository_scope }
                if repository_scope.contains('\0')
                    || repository_scope.len() > INDEX_ROUTING_KEY_BYTES =>
            {
                Err(IndexError::InvalidDefinition(
                    "Git scope contains NUL".into(),
                ))
            }
            Self::Tensor { model_scope }
                if model_scope.contains('\0') || model_scope.len() > INDEX_ROUTING_KEY_BYTES =>
            {
                Err(IndexError::InvalidDefinition(
                    "Tensor scope contains NUL".into(),
                ))
            }
            _ => Ok(()),
        }
    }
}

fn validate_bm25(k1: f64, b: f64) -> Result<(), IndexError> {
    if !k1.is_finite() || !b.is_finite() || k1 < 0.0 || !(0.0..=1.0).contains(&b) {
        return Err(IndexError::InvalidDefinition(
            "BM25 parameters must be finite, k1 non-negative, and b in 0..=1".into(),
        ));
    }
    Ok(())
}

fn validate_dimensions(dimensions: u32) -> Result<(), IndexError> {
    if dimensions == 0 {
        return Err(IndexError::InvalidDefinition(
            "vector dimensions must be non-zero".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum OrderDirection {
    Ascending = 1,
    Descending = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderField {
    pub field_id: FieldId,
    pub direction: OrderDirection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComponentVersion {
    pub component_kind: ComponentKind,
    pub codec_version: u16,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub kind: IndexKind,
    pub path_prefix: String,
    pub content_type_scope: Option<String>,
    pub fields: Vec<FieldSchema>,
    pub semantics: IndexSemantics,
    pub physical_order: Vec<OrderField>,
    pub component_versions: Vec<ComponentVersion>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentSchemaShape {
    pub field_count: usize,
    pub component_count: usize,
    pub component_statistics_count: usize,
    pub statistics_payload_bytes: usize,
}

impl Schema {
    pub fn codec_version(&self, kind: ComponentKind) -> Result<u16, IndexError> {
        self.component_versions
            .binary_search_by_key(&kind, |version| version.component_kind)
            .ok()
            .map(|index| self.component_versions[index].codec_version)
            .ok_or_else(|| {
                IndexError::InvalidDefinition(format!(
                    "schema has no codec version for component kind {}",
                    kind.get()
                ))
            })
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        if self.kind != self.semantics.kind()
            || self.path_prefix.len() > INDEX_ROUTING_KEY_BYTES
            || self.path_prefix.contains('\0')
            || self.content_type_scope.as_ref().is_some_and(|value| {
                value.is_empty() || value.contains('\0') || value.len() > INDEX_ROUTING_KEY_BYTES
            })
        {
            return Err(IndexError::InvalidDefinition(
                "schema scope or kind is invalid".into(),
            ));
        }
        self.semantics.validate()?;
        let shape = self.segment_shape()?;
        let statistics_component_bytes = shape
            .statistics_payload_bytes
            .checked_add(COMPONENT_HEADER_BYTES)
            .ok_or(IndexError::OffsetOverflow)?;
        if statistics_component_bytes > INDEX_COMPONENT_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: statistics_component_bytes,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        let mut names = BTreeSet::new();
        for (ordinal, field) in self.fields.iter().enumerate() {
            let expected = u32::try_from(ordinal).map_err(|_| IndexError::OffsetOverflow)?;
            if field.id.get() != expected
                || field.name.is_empty()
                || field.name.contains('\0')
                || field.name.len() > INDEX_ROUTING_KEY_BYTES
                || field.source_selector.contains('\0')
                || field.source_selector.len() > INDEX_ROUTING_KEY_BYTES
                || !names.insert(field.name.as_str())
            {
                return Err(IndexError::InvalidDefinition(
                    "schema fields require dense IDs and unique valid names".into(),
                ));
            }
            field.domain.validate()?;
            field.components.validate()?;
            if !field.allow_null && field.domain.bits() & ScalarDomain::NULL.bits() != 0 {
                return Err(IndexError::InvalidDefinition(
                    "field null domain conflicts with null policy".into(),
                ));
            }
        }
        let mut ordered = BTreeSet::new();
        for order in &self.physical_order {
            let field = self
                .fields
                .get(order.field_id.get() as usize)
                .ok_or_else(|| IndexError::InvalidDefinition("unknown order field".into()))?;
            if field.cardinality != Cardinality::Single
                || !field.components.contains(FieldComponents::FAST_COLUMN)
                || !ordered.insert(order.field_id)
            {
                return Err(IndexError::InvalidDefinition(
                    "physical order requires unique single-valued fast-column fields".into(),
                ));
            }
        }
        if self.component_versions.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "schema requires a component codec catalogue".into(),
            ));
        }
        let mut previous = None;
        for version in &self.component_versions {
            if version.codec_version == 0
                || previous.is_some_and(|kind| kind >= version.component_kind)
            {
                return Err(IndexError::InvalidDefinition(
                    "component versions must be unique and component-kind ordered".into(),
                ));
            }
            previous = Some(version.component_kind);
        }
        Ok(())
    }

    pub(crate) fn segment_shape(&self) -> Result<SegmentSchemaShape, IndexError> {
        let mut component_count = 3usize;
        let mut component_statistics_count = 0usize;
        let mut statistics_payload_bytes = STATISTICS_FIXED_PAYLOAD_BYTES;
        if !self.physical_order.is_empty() {
            statistics_payload_bytes = statistics_payload_bytes
                .checked_add(STATISTICS_PHYSICAL_BOUNDS_BYTES)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        let mut stored = false;
        for field in &self.fields {
            statistics_payload_bytes = statistics_payload_bytes
                .checked_add(STATISTICS_FIELD_BYTES)
                .ok_or(IndexError::OffsetOverflow)?;
            if field.components.contains(FieldComponents::NORMS) {
                statistics_payload_bytes = statistics_payload_bytes
                    .checked_add(STATISTICS_LENGTH_OPTIONS_BYTES)
                    .ok_or(IndexError::OffsetOverflow)?;
                component_count = component_count
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            if field.components.contains(FieldComponents::VECTOR) {
                statistics_payload_bytes = statistics_payload_bytes
                    .checked_add(
                        STATISTICS_VECTOR_DIMENSIONS_BYTES
                            .checked_add(STATISTICS_FIELD_COMPONENT_BYTES)
                            .ok_or(IndexError::OffsetOverflow)?,
                    )
                    .ok_or(IndexError::OffsetOverflow)?;
                component_count = component_count
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                component_statistics_count = component_statistics_count
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            if field.components.contains(FieldComponents::FAST_COLUMN) {
                statistics_payload_bytes = statistics_payload_bytes
                    .checked_add(STATISTICS_FIELD_COMPONENT_BYTES)
                    .ok_or(IndexError::OffsetOverflow)?;
                component_count = component_count
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                component_statistics_count = component_statistics_count
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            if field.components.contains(FieldComponents::TERMS) {
                statistics_payload_bytes = statistics_payload_bytes
                    .checked_add(STATISTICS_FIELD_COMPONENT_BYTES)
                    .ok_or(IndexError::OffsetOverflow)?;
                component_count = component_count
                    .checked_add(2)
                    .ok_or(IndexError::OffsetOverflow)?;
                component_statistics_count = component_statistics_count
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                if field.components.contains(FieldComponents::POSITIONS) {
                    statistics_payload_bytes = statistics_payload_bytes
                        .checked_add(STATISTICS_FIELD_COMPONENT_BYTES)
                        .ok_or(IndexError::OffsetOverflow)?;
                    component_count = component_count
                        .checked_add(1)
                        .ok_or(IndexError::OffsetOverflow)?;
                    component_statistics_count = component_statistics_count
                        .checked_add(1)
                        .ok_or(IndexError::OffsetOverflow)?;
                }
            }
            stored |= field.components.contains(FieldComponents::STORED);
        }
        if stored {
            statistics_payload_bytes = statistics_payload_bytes
                .checked_add(STATISTICS_GLOBAL_COMPONENT_BYTES)
                .ok_or(IndexError::OffsetOverflow)?;
            component_count = component_count
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
            component_statistics_count = component_statistics_count
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        Ok(SegmentSchemaShape {
            field_count: self.fields.len(),
            component_count,
            component_statistics_count,
            statistics_payload_bytes,
        })
    }

    pub fn fingerprint(&self) -> Result<[u8; 32], IndexError> {
        self.validate()?;
        let mut out = Encoder::default();
        out.raw(SCHEMA_DOMAIN);
        out.u8(self.kind as u8);
        out.string(&self.path_prefix)?;
        match &self.content_type_scope {
            Some(value) => {
                out.bool(true);
                out.string(value)?;
            }
            None => out.bool(false),
        }
        out.usize_u32(self.fields.len())?;
        for field in &self.fields {
            out.u32(field.id.get());
            out.string(&field.name)?;
            out.string(&field.source_selector)?;
            out.u8(field.domain.bits());
            out.u8(field.cardinality as u8);
            out.bool(field.allow_missing);
            out.bool(field.allow_null);
            out.u8(field.collation as u8);
            out.u16(field.components.bits());
        }
        encode_semantics(&mut out, &self.semantics)?;
        out.usize_u32(self.physical_order.len())?;
        for order in &self.physical_order {
            out.u32(order.field_id.get());
            out.u8(order.direction as u8);
        }
        // Stable result then source `(path, version)` and source-record
        // ordinal tie-break is mandatory. This marker versions that contract.
        out.u8(2);
        out.usize_u32(self.component_versions.len())?;
        for version in &self.component_versions {
            out.u16(version.component_kind.get());
            out.u16(version.codec_version);
        }
        Ok(*blake3::hash(&out.finish()).as_bytes())
    }
}

fn encode_semantics(out: &mut Encoder, value: &IndexSemantics) -> Result<(), IndexError> {
    out.u8(value.kind() as u8);
    match value {
        IndexSemantics::Path | IndexSemantics::MetadataFilter | IndexSemantics::TypedJson => {}
        IndexSemantics::FullText {
            analyzer,
            bm25_k1,
            bm25_b,
        } => {
            out.u8(*analyzer as u8);
            out.u64(canonical_f64_bits(*bm25_k1));
            out.u64(canonical_f64_bits(*bm25_b));
        }
        IndexSemantics::Vector {
            dimensions,
            metric,
            normalization,
        } => encode_vector(out, *dimensions, *metric, *normalization),
        IndexSemantics::Hybrid {
            analyzer,
            bm25_k1,
            bm25_b,
            dimensions,
            metric,
            normalization,
            lexical_weight,
            vector_weight,
        } => {
            out.u8(*analyzer as u8);
            out.u64(canonical_f64_bits(*bm25_k1));
            out.u64(canonical_f64_bits(*bm25_b));
            encode_vector(out, *dimensions, *metric, *normalization);
            out.u64(canonical_f64_bits(*lexical_weight));
            out.u64(canonical_f64_bits(*vector_weight));
        }
        IndexSemantics::GitSource { repository_scope } => out.string(repository_scope)?,
        IndexSemantics::Tensor { model_scope } => out.string(model_scope)?,
    }
    Ok(())
}

fn encode_vector(
    out: &mut Encoder,
    dimensions: u32,
    metric: VectorMetric,
    normalization: VectorNormalization,
) {
    out.u32(dimensions);
    out.u8(metric as u8);
    out.u8(normalization as u8);
}

fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 { 0 } else { value.to_bits() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v4::{
        ComponentStatistics, FieldStatistics, INDEX_DECODE_BYTES, PhysicalOrderBounds,
        SegmentStatistics,
    };

    fn component(role: ComponentKind, field_id: Option<FieldId>) -> ComponentStatistics {
        ComponentStatistics {
            role,
            field_id,
            leaf_count: 1,
            component_count: 1,
            encoded_bytes: 1,
            logical_bytes: 1,
            decoded_bytes_upper_bound: INDEX_DECODE_BYTES as u64,
        }
    }

    fn maximum_statistics(schema: &Schema) -> SegmentStatistics {
        let fields = schema
            .fields
            .iter()
            .map(|field| FieldStatistics {
                field_id: field.id,
                present_documents: 0,
                null_documents: 0,
                value_count: 0,
                unique_terms: 0,
                total_term_frequency: 0,
                total_field_length: 0,
                minimum_field_length: field
                    .components
                    .contains(FieldComponents::NORMS)
                    .then_some(0),
                maximum_field_length: field
                    .components
                    .contains(FieldComponents::NORMS)
                    .then_some(0),
                vector_count: 0,
                vector_dimensions: field
                    .components
                    .contains(FieldComponents::VECTOR)
                    .then_some(4),
                multi_valued_documents: 0,
                boolean_values: 0,
                number_values: 0,
                unsigned_values: 0,
                string_values: 0,
            })
            .collect();
        let mut components = Vec::new();
        let mut stored = false;
        for field in &schema.fields {
            if field.components.contains(FieldComponents::TERMS) {
                components.push(component(ComponentKind::POSTINGS, Some(field.id)));
                if field.components.contains(FieldComponents::POSITIONS) {
                    components.push(component(ComponentKind::POSITIONS, Some(field.id)));
                }
            }
            if field.components.contains(FieldComponents::FAST_COLUMN) {
                components.push(component(ComponentKind::FAST_COLUMN, Some(field.id)));
            }
            if field.components.contains(FieldComponents::VECTOR) {
                components.push(component(ComponentKind::VECTORS, Some(field.id)));
            }
            stored |= field.components.contains(FieldComponents::STORED);
        }
        if stored {
            components.push(component(ComponentKind::STORED_FIELDS, None));
        }
        components.sort_by_key(|value| (value.role, value.field_id));
        SegmentStatistics::new(
            1,
            1,
            0,
            (!schema.physical_order.is_empty()).then(|| PhysicalOrderBounds {
                minimum_key: vec![0; INDEX_ROUTING_KEY_BYTES],
                maximum_key: vec![0; INDEX_ROUTING_KEY_BYTES],
            }),
            fields,
            components,
        )
        .unwrap()
    }

    fn fields(count: usize, components: FieldComponents) -> Vec<FieldSchema> {
        (0..count)
            .map(|ordinal| FieldSchema {
                id: FieldId::new(u32::try_from(ordinal).unwrap()),
                name: format!("field-{ordinal}"),
                source_selector: format!("/field-{ordinal}"),
                domain: ScalarDomain::STRING,
                cardinality: Cardinality::Single,
                allow_missing: true,
                allow_null: false,
                collation: Collation::BinaryUtf8,
                components,
            })
            .collect()
    }

    fn admission_schema(count: usize, components: FieldComponents, ordered: bool) -> Schema {
        let vector = components.contains(FieldComponents::VECTOR);
        Schema {
            kind: if vector {
                IndexKind::Hybrid
            } else {
                IndexKind::TypedJson
            },
            path_prefix: "/objects".into(),
            content_type_scope: Some("application/json".into()),
            fields: fields(count, components),
            semantics: if vector {
                IndexSemantics::Hybrid {
                    analyzer: Analyzer::UnicodeAlphanumericLowercase,
                    bm25_k1: 1.2,
                    bm25_b: 0.75,
                    dimensions: 4,
                    metric: VectorMetric::Cosine,
                    normalization: VectorNormalization::L2,
                    lexical_weight: 0.5,
                    vector_weight: 0.5,
                }
            } else {
                IndexSemantics::TypedJson
            },
            physical_order: ordered
                .then_some(OrderField {
                    field_id: FieldId::new(0),
                    direction: OrderDirection::Ascending,
                })
                .into_iter()
                .collect(),
            component_versions: vec![ComponentVersion {
                component_kind: ComponentKind::SCORING_STATISTICS,
                codec_version: 1,
            }],
        }
    }

    fn schema() -> Schema {
        Schema {
            kind: IndexKind::TypedJson,
            path_prefix: "/objects".into(),
            content_type_scope: Some("application/json".into()),
            fields: vec![FieldSchema {
                id: FieldId::new(0),
                name: "modified".into(),
                source_selector: "/modified".into(),
                domain: ScalarDomain::NUMBER,
                cardinality: Cardinality::Single,
                allow_missing: true,
                allow_null: false,
                collation: Collation::BinaryUtf8,
                components: FieldComponents::TERMS.union(FieldComponents::FAST_COLUMN),
            }],
            semantics: IndexSemantics::TypedJson,
            physical_order: vec![OrderField {
                field_id: FieldId::new(0),
                direction: OrderDirection::Descending,
            }],
            component_versions: vec![
                ComponentVersion {
                    component_kind: ComponentKind::TERM_DICTIONARY,
                    codec_version: 1,
                },
                ComponentVersion {
                    component_kind: ComponentKind::POSTINGS,
                    codec_version: 1,
                },
            ],
        }
    }

    #[test]
    fn fingerprint_is_deterministic_and_semantic() {
        let first = schema();
        assert_eq!(
            first.fingerprint().unwrap(),
            first.clone().fingerprint().unwrap()
        );
        let mut changed = first.clone();
        changed.physical_order[0].direction = OrderDirection::Ascending;
        assert_ne!(first.fingerprint().unwrap(), changed.fingerprint().unwrap());
        let mut changed = first.clone();
        changed.fields[0].allow_missing = false;
        assert_ne!(first.fingerprint().unwrap(), changed.fingerprint().unwrap());
    }

    #[test]
    fn statistics_admission_accepts_1702_maximal_fields_and_rejects_1703() {
        let maximal = FieldComponents::TERMS
            .union(FieldComponents::FAST_COLUMN)
            .union(FieldComponents::STORED)
            .union(FieldComponents::POSITIONS)
            .union(FieldComponents::NORMS)
            .union(FieldComponents::VECTOR);
        let accepted = admission_schema(1_702, maximal, true);
        let accepted_shape = accepted.segment_shape().unwrap();
        assert_eq!(accepted_shape.statistics_payload_bytes, 523_984);
        assert_eq!(
            maximum_statistics(&accepted)
                .encode_payload()
                .unwrap()
                .len(),
            accepted_shape.statistics_payload_bytes
        );
        assert_eq!(accepted_shape.component_count, 10_216);
        assert_eq!(accepted_shape.component_statistics_count, 6_809);
        accepted.validate().unwrap();

        let rejected = admission_schema(1_703, maximal, true);
        let rejected_shape = rejected.segment_shape().unwrap();
        assert_eq!(rejected_shape.statistics_payload_bytes, 524_287);
        assert_eq!(
            maximum_statistics(&rejected)
                .encode_payload()
                .unwrap()
                .len(),
            rejected_shape.statistics_payload_bytes
        );
        assert_eq!(
            rejected.validate(),
            Err(IndexError::ResourceLimit {
                needed: 524_407,
                limit: INDEX_COMPONENT_BYTES,
            })
        );
    }

    #[test]
    fn statistics_admission_accepts_5088_bare_fields_and_rejects_5089() {
        let accepted = admission_schema(5_088, FieldComponents::STORED, false);
        let accepted_shape = accepted.segment_shape().unwrap();
        assert_eq!(accepted_shape.statistics_payload_bytes, 524_142);
        assert_eq!(
            maximum_statistics(&accepted)
                .encode_payload()
                .unwrap()
                .len(),
            accepted_shape.statistics_payload_bytes
        );
        accepted.validate().unwrap();

        let rejected = admission_schema(5_089, FieldComponents::STORED, false);
        let rejected_shape = rejected.segment_shape().unwrap();
        assert_eq!(rejected_shape.statistics_payload_bytes, 524_245);
        assert_eq!(
            maximum_statistics(&rejected)
                .encode_payload()
                .unwrap()
                .len(),
            rejected_shape.statistics_payload_bytes
        );
        assert_eq!(
            rejected.validate(),
            Err(IndexError::ResourceLimit {
                needed: 524_365,
                limit: INDEX_COMPONENT_BYTES,
            })
        );
    }

    #[test]
    fn statistics_admission_weights_mixed_field_features_exactly() {
        let maximal = FieldComponents::TERMS
            .union(FieldComponents::FAST_COLUMN)
            .union(FieldComponents::STORED)
            .union(FieldComponents::POSITIONS)
            .union(FieldComponents::NORMS)
            .union(FieldComponents::VECTOR);
        let mut mixed = admission_schema(1, maximal, true);
        mixed.fields.extend(fields(1, FieldComponents::STORED));
        mixed.fields[1].id = FieldId::new(1);
        mixed.fields[1].name = "stored".into();
        mixed.fields[1].source_selector = "/stored".into();
        let mut terms = fields(1, FieldComponents::TERMS.union(FieldComponents::POSITIONS));
        terms[0].id = FieldId::new(2);
        terms[0].name = "terms".into();
        terms[0].source_selector = "/terms".into();
        mixed.fields.extend(terms);
        let shape = mixed.segment_shape().unwrap();
        assert_eq!(shape.statistics_payload_bytes, 8_881);
        assert_eq!(
            maximum_statistics(&mixed).encode_payload().unwrap().len(),
            shape.statistics_payload_bytes
        );
        assert_eq!(shape.component_count, 13);
        assert_eq!(shape.component_statistics_count, 7);
        mixed.validate().unwrap();
    }

    #[test]
    fn dense_ids_and_single_value_order_are_enforced() {
        let mut invalid = schema();
        invalid.fields[0].id = FieldId::new(1);
        assert!(invalid.fingerprint().is_err());
        let mut invalid = schema();
        invalid.fields[0].cardinality = Cardinality::Multi;
        assert!(invalid.fingerprint().is_err());
    }
}
