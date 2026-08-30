use std::collections::BTreeSet;

use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Encoder};
use super::model::{
    ComponentKind, INDEX_COMPONENT_BYTES, INDEX_ROUTING_KEY_BYTES, INDEX_TERM_BYTES,
};

const SCHEMA_DOMAIN: &[u8] = b"keldra.index.schema.v4";
const MEMBERSHIP_RECIPE_DOMAIN: &[u8] = b"keldra.index.membership-recipe/v1";
const FIELD_RECIPE_DOMAIN: &[u8] = b"keldra.index.field-recipe/v1";
const STATISTICS_FIXED_PAYLOAD_BYTES: usize = 35;
const STATISTICS_FIELD_BYTES: usize = 103;
const STATISTICS_LENGTH_OPTIONS_BYTES: usize = 8;
const STATISTICS_VECTOR_DIMENSIONS_BYTES: usize = 4;
const STATISTICS_FIELD_COMPONENT_BYTES: usize = 47;

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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum FieldType {
    Boolean = 1,
    SignedInteger = 2,
    UnsignedInteger = 3,
    Float = 4,
    Keyword = 5,
    Text = 6,
    /// Internal fixed-width vector field used by Vector and Hybrid definitions.
    /// It is not one of the public Typed JSON field types.
    Vector = 7,
    /// A public timestamp whose physical scalar is signed Unix epoch
    /// milliseconds. Parsing and presentation use the field's DateFormat.
    Date = 8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DateFormat {
    Iso8601,
    Strftime(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct FieldCapabilities(u16);

impl FieldCapabilities {
    pub const EXACT: Self = Self(1 << 0);
    pub const PREFIX: Self = Self(1 << 1);
    pub const RANGE: Self = Self(1 << 2);
    pub const ORDER: Self = Self(1 << 3);
    pub const FACET: Self = Self(1 << 4);
    pub const AGGREGATE: Self = Self(1 << 5);
    pub const FULL_TEXT: Self = Self(1 << 6);
    const ALL: u16 = (1 << 7) - 1;

    pub const fn empty() -> Self {
        Self(0)
    }

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
                "field capabilities are empty or unknown".into(),
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
    pub const POINTS: Self = Self(1 << 1);
    pub const DOC_VALUES: Self = Self(1 << 2);
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
    pub field_type: FieldType,
    pub cardinality: Cardinality,
    pub allow_missing: bool,
    pub allow_null: bool,
    pub collation: Collation,
    pub capabilities: FieldCapabilities,
    pub analyzer: Option<Analyzer>,
    pub date_format: Option<DateFormat>,
    pub components: FieldComponents,
}

impl FieldSchema {
    pub fn compiled_components(&self) -> Result<FieldComponents, IndexError> {
        if self.field_type == FieldType::Vector {
            if self.capabilities != FieldCapabilities::empty() {
                return Err(IndexError::InvalidDefinition(
                    "vector fields do not use scalar field capabilities".into(),
                ));
            }
            return Ok(FieldComponents::VECTOR);
        }
        self.capabilities.validate()?;
        let capabilities = self.capabilities;
        let mut components = match self.field_type {
            FieldType::Boolean => {
                reject_capabilities(
                    capabilities,
                    FieldCapabilities::EXACT.union(FieldCapabilities::FACET),
                    "Boolean",
                )?;
                if capabilities.contains(FieldCapabilities::EXACT) {
                    FieldComponents::TERMS
                } else {
                    FieldComponents(0)
                }
            }
            FieldType::SignedInteger | FieldType::UnsignedInteger | FieldType::Float => {
                reject_capabilities(
                    capabilities,
                    FieldCapabilities::EXACT
                        .union(FieldCapabilities::RANGE)
                        .union(FieldCapabilities::ORDER)
                        .union(FieldCapabilities::FACET)
                        .union(FieldCapabilities::AGGREGATE),
                    "numeric",
                )?;
                let mut value = FieldComponents(0);
                if capabilities.contains(FieldCapabilities::EXACT)
                    || capabilities.contains(FieldCapabilities::RANGE)
                {
                    value = value.union(FieldComponents::POINTS);
                }
                value
            }
            FieldType::Date => {
                reject_capabilities(
                    capabilities,
                    FieldCapabilities::EXACT
                        .union(FieldCapabilities::RANGE)
                        .union(FieldCapabilities::ORDER)
                        .union(FieldCapabilities::FACET),
                    "date",
                )?;
                if capabilities.contains(FieldCapabilities::EXACT)
                    || capabilities.contains(FieldCapabilities::RANGE)
                {
                    FieldComponents::POINTS
                } else {
                    FieldComponents(0)
                }
            }
            FieldType::Keyword => {
                reject_capabilities(
                    capabilities,
                    FieldCapabilities::EXACT
                        .union(FieldCapabilities::PREFIX)
                        .union(FieldCapabilities::RANGE)
                        .union(FieldCapabilities::ORDER)
                        .union(FieldCapabilities::FACET),
                    "keyword",
                )?;
                if capabilities.contains(FieldCapabilities::EXACT)
                    || capabilities.contains(FieldCapabilities::PREFIX)
                    || capabilities.contains(FieldCapabilities::RANGE)
                {
                    FieldComponents::TERMS
                } else {
                    FieldComponents(0)
                }
            }
            FieldType::Text => {
                if capabilities != FieldCapabilities::FULL_TEXT {
                    return Err(IndexError::InvalidDefinition(
                        "text fields support only FULL_TEXT".into(),
                    ));
                }
                FieldComponents::TERMS
                    .union(FieldComponents::POSITIONS)
                    .union(FieldComponents::NORMS)
            }
            FieldType::Vector => unreachable!("handled before scalar capability validation"),
        };
        if capabilities.contains(FieldCapabilities::ORDER)
            || capabilities.contains(FieldCapabilities::FACET)
            || capabilities.contains(FieldCapabilities::AGGREGATE)
        {
            components = components.union(FieldComponents::DOC_VALUES);
        }
        Ok(components)
    }
}

fn reject_capabilities(
    capabilities: FieldCapabilities,
    allowed: FieldCapabilities,
    kind: &str,
) -> Result<(), IndexError> {
    if capabilities.bits() & !allowed.bits() != 0 {
        return Err(IndexError::InvalidDefinition(format!(
            "{kind} field declares an unsupported capability"
        )));
    }
    Ok(())
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

/// Definition-neutral identities used by the logical index catalogue.
///
/// Public field IDs and names are intentionally absent from field recipe
/// fingerprints. They belong to the logical binding; the physical recipe is
/// selected solely by source and query semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecipeFingerprints {
    pub membership: [u8; 32],
    pub fields: Vec<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentSchemaShape {
    pub field_count: usize,
    pub component_count: usize,
    pub component_statistics_count: usize,
    pub statistics_payload_bytes: usize,
}

impl Schema {
    /// Assign definition-neutral dense field IDs from physical recipe identity.
    ///
    /// Public field names remain on the returned schema so query compilation
    /// and result labelling retain the logical contract. They do not influence
    /// physical ordering or the segment fingerprint. Definitions which differ
    /// only by field declaration order or public aliases can therefore read the
    /// same immutable components without a query-time name registry.
    pub fn canonicalize_physical_fields(mut self) -> Result<Self, IndexError> {
        let recipes = self.recipe_fingerprints()?.fields;
        let mut fields = self
            .fields
            .into_iter()
            .enumerate()
            .zip(recipes)
            .map(|((ordinal, field), recipe)| (recipe, ordinal, field))
            .collect::<Vec<_>>();
        fields.sort_by_key(|(recipe, ordinal, _)| (*recipe, *ordinal));

        let mut old_to_new = vec![FieldId::new(0); fields.len()];
        let mut canonical = Vec::with_capacity(fields.len());
        for (new_ordinal, (_, old_ordinal, mut field)) in fields.into_iter().enumerate() {
            let field_id =
                FieldId::new(u32::try_from(new_ordinal).map_err(|_| IndexError::OffsetOverflow)?);
            old_to_new[old_ordinal] = field_id;
            field.id = field_id;
            canonical.push(field);
        }
        for order in &mut self.physical_order {
            order.field_id = *old_to_new
                .get(order.field_id.get() as usize)
                .ok_or_else(|| IndexError::InvalidDefinition("unknown order field".into()))?;
        }
        self.fields = canonical;
        self.validate()?;
        Ok(self)
    }

    pub fn recipe_fingerprints(&self) -> Result<RecipeFingerprints, IndexError> {
        self.validate()?;

        let mut membership = Encoder::default();
        membership.raw(MEMBERSHIP_RECIPE_DOMAIN);
        membership.u8(self.kind as u8);
        membership.string(&self.path_prefix)?;
        match &self.content_type_scope {
            Some(value) => {
                membership.bool(true);
                membership.string(value)?;
            }
            None => membership.bool(false),
        }
        encode_semantics(&mut membership, &self.semantics)?;

        let mut fields = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let mut recipe = Encoder::default();
            recipe.raw(FIELD_RECIPE_DOMAIN);
            recipe.u8(self.kind as u8);
            encode_semantics(&mut recipe, &self.semantics)?;
            recipe.string(&field.source_selector)?;
            recipe.u8(field.field_type as u8);
            recipe.u8(field.cardinality as u8);
            recipe.bool(field.allow_missing);
            recipe.bool(field.allow_null);
            recipe.u8(field.collation as u8);
            recipe.u16(field.capabilities.bits());
            match field.analyzer {
                Some(analyzer) => {
                    recipe.bool(true);
                    recipe.u8(analyzer as u8);
                }
                None => recipe.bool(false),
            }
            match field.date_format.as_ref() {
                Some(DateFormat::Iso8601) => {
                    recipe.bool(true);
                    recipe.bool(false);
                }
                Some(DateFormat::Strftime(pattern)) => {
                    recipe.bool(true);
                    recipe.bool(true);
                    recipe.string(pattern)?;
                }
                None => recipe.bool(false),
            }
            recipe.u16(field.components.bits());
            encode_field_component_versions(&mut recipe, self, field.components)?;
            fields.push(*blake3::hash(&recipe.finish()).as_bytes());
        }

        Ok(RecipeFingerprints {
            membership: *blake3::hash(&membership.finish()).as_bytes(),
            fields,
        })
    }

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
            field.components.validate()?;
            if field.components != field.compiled_components()? {
                return Err(IndexError::InvalidDefinition(
                    "field components do not match its declared type and capabilities".into(),
                ));
            }
            match (field.field_type, field.analyzer) {
                (FieldType::Text, Some(Analyzer::UnicodeAlphanumericLowercase)) => {}
                (FieldType::Text, _) => {
                    return Err(IndexError::InvalidDefinition(
                        "text field requires a supported analyzer".into(),
                    ));
                }
                (_, None) => {}
                (_, Some(_)) => {
                    return Err(IndexError::InvalidDefinition(
                        "only text fields may declare an analyzer".into(),
                    ));
                }
            }
            match (field.field_type, field.date_format.as_ref()) {
                (FieldType::Date, Some(DateFormat::Iso8601)) => {}
                (FieldType::Date, Some(DateFormat::Strftime(pattern)))
                    if !pattern.is_empty()
                        && !pattern.contains('\0')
                        && pattern.len() <= INDEX_ROUTING_KEY_BYTES => {}
                (FieldType::Date, _) => {
                    return Err(IndexError::InvalidDefinition(
                        "date field requires a valid date format".into(),
                    ));
                }
                (_, None) => {}
                (_, Some(_)) => {
                    return Err(IndexError::InvalidDefinition(
                        "only date fields may declare a date format".into(),
                    ));
                }
            }
            if field.field_type == FieldType::Vector
                && !matches!(self.kind, IndexKind::Vector | IndexKind::Hybrid)
            {
                return Err(IndexError::InvalidDefinition(
                    "vector fields require a Vector or Hybrid index".into(),
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
                || !field.capabilities.contains(FieldCapabilities::ORDER)
                || !field.components.contains(FieldComponents::DOC_VALUES)
                || !ordered.insert(order.field_id)
            {
                return Err(IndexError::InvalidDefinition(
                    "physical order requires unique single-valued ORDER fields".into(),
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
        let maximum_physical_order_key_bytes = self.maximum_physical_order_key_bytes()?;
        if maximum_physical_order_key_bytes != 0 {
            statistics_payload_bytes = statistics_payload_bytes
                .checked_add(
                    maximum_physical_order_key_bytes
                        .checked_add(4)
                        .and_then(|bytes| bytes.checked_mul(2))
                        .ok_or(IndexError::OffsetOverflow)?,
                )
                .ok_or(IndexError::OffsetOverflow)?;
        }
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
            if field.components.contains(FieldComponents::DOC_VALUES) {
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
            if field.components.contains(FieldComponents::POINTS) {
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
        }
        Ok(SegmentSchemaShape {
            field_count: self.fields.len(),
            component_count,
            component_statistics_count,
            statistics_payload_bytes,
        })
    }

    pub(crate) fn maximum_physical_order_key_bytes(&self) -> Result<usize, IndexError> {
        self.physical_order.iter().try_fold(0usize, |total, order| {
            let field = self
                .fields
                .get(order.field_id.get() as usize)
                .ok_or_else(|| IndexError::InvalidDefinition("unknown order field".into()))?;
            // One outer presence byte precedes every encoded scalar. Keyword
            // bytes are zero-escaped and terminated, so an all-NUL value is
            // the exact worst case. Other scalar widths are fixed.
            let field_bytes = match field.field_type {
                FieldType::Boolean => 3,
                FieldType::SignedInteger
                | FieldType::UnsignedInteger
                | FieldType::Float
                | FieldType::Date => 10,
                FieldType::Keyword => INDEX_TERM_BYTES
                    .checked_mul(2)
                    .and_then(|bytes| bytes.checked_add(4))
                    .ok_or(IndexError::OffsetOverflow)?,
                FieldType::Text | FieldType::Vector => {
                    return Err(IndexError::InvalidDefinition(
                        "physical order field type is not orderable".into(),
                    ));
                }
            };
            total
                .checked_add(field_bytes)
                .ok_or(IndexError::OffsetOverflow)
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
            out.string(&field.source_selector)?;
            out.u8(field.field_type as u8);
            out.u8(field.cardinality as u8);
            out.bool(field.allow_missing);
            out.bool(field.allow_null);
            out.u8(field.collation as u8);
            out.u16(field.capabilities.bits());
            match field.analyzer {
                Some(analyzer) => {
                    out.bool(true);
                    out.u8(analyzer as u8);
                }
                None => out.bool(false),
            }
            if field.field_type == FieldType::Date {
                match field.date_format.as_ref().expect("validated date format") {
                    DateFormat::Iso8601 => out.bool(false),
                    DateFormat::Strftime(pattern) => {
                        out.bool(true);
                        out.string(pattern)?;
                    }
                }
            }
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

fn encode_field_component_versions(
    out: &mut Encoder,
    schema: &Schema,
    components: FieldComponents,
) -> Result<(), IndexError> {
    let mut kinds = Vec::with_capacity(7);
    if components.contains(FieldComponents::TERMS) {
        kinds.extend([ComponentKind::TERM_DICTIONARY, ComponentKind::POSTINGS]);
    }
    if components.contains(FieldComponents::POINTS) {
        kinds.push(ComponentKind::POINTS);
    }
    if components.contains(FieldComponents::DOC_VALUES) {
        kinds.push(ComponentKind::DOC_VALUES);
    }
    if components.contains(FieldComponents::POSITIONS) {
        kinds.push(ComponentKind::POSITIONS);
    }
    if components.contains(FieldComponents::NORMS) {
        kinds.push(ComponentKind::NORMS);
        kinds.push(ComponentKind::SCORING_STATISTICS);
    }
    if components.contains(FieldComponents::VECTOR) {
        kinds.push(ComponentKind::VECTORS);
    }
    kinds.sort_unstable();
    kinds.dedup();
    out.usize_u32(kinds.len())?;
    for kind in kinds {
        out.u16(kind.get());
        out.u16(schema.codec_version(kind)?);
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
        ComponentStatistics, FieldStatistics, INDEX_DECODE_BYTES, PhysicalOrderBounds, ScalarValue,
        SegmentStatistics, SortValue, encode_physical_order_key,
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
        for field in &schema.fields {
            if field.components.contains(FieldComponents::TERMS) {
                components.push(component(ComponentKind::POSTINGS, Some(field.id)));
                if field.components.contains(FieldComponents::POSITIONS) {
                    components.push(component(ComponentKind::POSITIONS, Some(field.id)));
                }
            }
            if field.components.contains(FieldComponents::POINTS) {
                components.push(component(ComponentKind::POINTS, Some(field.id)));
            }
            if field.components.contains(FieldComponents::DOC_VALUES) {
                components.push(component(ComponentKind::DOC_VALUES, Some(field.id)));
            }
            if field.components.contains(FieldComponents::VECTOR) {
                components.push(component(ComponentKind::VECTORS, Some(field.id)));
            }
        }
        components.sort_by_key(|value| (value.role, value.field_id));
        SegmentStatistics::new(
            1,
            1,
            0,
            (!schema.physical_order.is_empty()).then(|| PhysicalOrderBounds {
                minimum_key: vec![0; schema.maximum_physical_order_key_bytes().unwrap()],
                maximum_key: vec![0; schema.maximum_physical_order_key_bytes().unwrap()],
            }),
            fields,
            components,
        )
        .unwrap()
    }

    fn fields(
        count: usize,
        field_type: FieldType,
        capabilities: FieldCapabilities,
    ) -> Vec<FieldSchema> {
        (0..count)
            .map(|ordinal| {
                let mut field = FieldSchema {
                    id: FieldId::new(u32::try_from(ordinal).unwrap()),
                    name: format!("field-{ordinal}"),
                    source_selector: format!("/field-{ordinal}"),
                    field_type,
                    cardinality: Cardinality::Single,
                    allow_missing: true,
                    allow_null: false,
                    collation: Collation::BinaryUtf8,
                    capabilities,
                    analyzer: (field_type == FieldType::Text)
                        .then_some(Analyzer::UnicodeAlphanumericLowercase),
                    date_format: (field_type == FieldType::Date).then_some(DateFormat::Iso8601),
                    components: FieldComponents(0),
                };
                field.components = field.compiled_components().unwrap();
                field
            })
            .collect()
    }

    fn admission_schema(
        count: usize,
        field_type: FieldType,
        capabilities: FieldCapabilities,
        ordered: bool,
    ) -> Schema {
        Schema {
            kind: IndexKind::TypedJson,
            path_prefix: "/objects".into(),
            content_type_scope: Some("application/json".into()),
            fields: fields(count, field_type, capabilities),
            semantics: IndexSemantics::TypedJson,
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
                field_type: FieldType::SignedInteger,
                cardinality: Cardinality::Single,
                allow_missing: true,
                allow_null: false,
                collation: Collation::BinaryUtf8,
                capabilities: FieldCapabilities::EXACT.union(FieldCapabilities::ORDER),
                analyzer: None,
                date_format: None,
                components: FieldComponents::POINTS.union(FieldComponents::DOC_VALUES),
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

    fn recipe_schema() -> Schema {
        let mut value = schema();
        value.component_versions = [
            ComponentKind::SEGMENT_ROOT,
            ComponentKind::ROUTING_NODE,
            ComponentKind::IDENTITY_TABLE,
            ComponentKind::LIVE_MASK,
            ComponentKind::PATH_LOCATOR,
            ComponentKind::TERM_DICTIONARY,
            ComponentKind::POSTINGS,
            ComponentKind::POINTS,
            ComponentKind::DOC_VALUES,
            ComponentKind::POSITIONS,
            ComponentKind::NORMS,
            ComponentKind::VECTORS,
            ComponentKind::SCORING_STATISTICS,
        ]
        .into_iter()
        .map(|component_kind| ComponentVersion {
            component_kind,
            codec_version: 1,
        })
        .collect();
        value
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
    fn recipe_fingerprints_separate_logical_names_from_physical_semantics() {
        let first = recipe_schema();
        let first_recipes = first.recipe_fingerprints().unwrap();

        let mut renamed = first.clone();
        renamed.fields[0].name = "publicly-renamed".into();
        assert_eq!(first.fingerprint().unwrap(), renamed.fingerprint().unwrap());
        assert_eq!(first_recipes, renamed.recipe_fingerprints().unwrap());

        let mut rescope = first.clone();
        rescope.path_prefix = "/another-membership".into();
        let rescope = rescope.recipe_fingerprints().unwrap();
        assert_ne!(first_recipes.membership, rescope.membership);
        assert_eq!(first_recipes.fields, rescope.fields);

        let mut reselect = first.clone();
        reselect.fields[0].source_selector = "/another-value".into();
        let reselect = reselect.recipe_fingerprints().unwrap();
        assert_eq!(first_recipes.membership, reselect.membership);
        assert_ne!(first_recipes.fields, reselect.fields);
    }

    #[test]
    fn canonical_field_ids_ignore_public_names_and_declaration_order() {
        let mut source = recipe_schema();
        let mut second = source.fields[0].clone();
        second.id = FieldId::new(1);
        second.name = "severity".into();
        second.source_selector = "/severity".into();
        source.fields.push(second);
        let first = source.clone().canonicalize_physical_fields().unwrap();
        let mut reordered = source;
        reordered.fields.swap(0, 1);
        for (ordinal, field) in reordered.fields.iter_mut().enumerate() {
            field.id = FieldId::new(u32::try_from(ordinal).unwrap());
        }
        for order in &mut reordered.physical_order {
            order.field_id = FieldId::new(1 - order.field_id.get());
        }
        reordered.fields[0].name = "second-public-alias".into();
        reordered.fields[1].name = "first-public-alias".into();
        let reordered = reordered.canonicalize_physical_fields().unwrap();

        assert_eq!(
            first.fingerprint().unwrap(),
            reordered.fingerprint().unwrap()
        );
        assert_eq!(
            first.fields[0].source_selector,
            reordered.fields[0].source_selector
        );
        assert_eq!(
            first.fields[1].source_selector,
            reordered.fields[1].source_selector
        );
        assert_ne!(first.fields[0].name, reordered.fields[0].name);
    }

    #[test]
    fn capability_selection_builds_only_required_components() {
        let cases = [
            (
                FieldType::Boolean,
                FieldCapabilities::EXACT,
                FieldComponents::TERMS,
            ),
            (
                FieldType::Boolean,
                FieldCapabilities::FACET,
                FieldComponents::DOC_VALUES,
            ),
            (
                FieldType::SignedInteger,
                FieldCapabilities::EXACT,
                FieldComponents::POINTS,
            ),
            (
                FieldType::SignedInteger,
                FieldCapabilities::ORDER,
                FieldComponents::DOC_VALUES,
            ),
            (
                FieldType::Date,
                FieldCapabilities::EXACT.union(FieldCapabilities::RANGE),
                FieldComponents::POINTS,
            ),
            (
                FieldType::Date,
                FieldCapabilities::ORDER.union(FieldCapabilities::FACET),
                FieldComponents::DOC_VALUES,
            ),
            (
                FieldType::Keyword,
                FieldCapabilities::PREFIX,
                FieldComponents::TERMS,
            ),
            (
                FieldType::Keyword,
                FieldCapabilities::FACET,
                FieldComponents::DOC_VALUES,
            ),
            (
                FieldType::Text,
                FieldCapabilities::FULL_TEXT,
                FieldComponents::TERMS
                    .union(FieldComponents::POSITIONS)
                    .union(FieldComponents::NORMS),
            ),
        ];
        for (field_type, capabilities, expected) in cases {
            let field = fields(1, field_type, capabilities).pop().unwrap();
            assert_eq!(field.components, expected);
        }
    }

    #[test]
    fn date_format_is_semantic_and_date_rejects_aggregate() {
        let mut iso = admission_schema(
            1,
            FieldType::Date,
            FieldCapabilities::EXACT.union(FieldCapabilities::FACET),
            false,
        );
        iso.validate().unwrap();
        let mut custom = iso.clone();
        custom.fields[0].date_format = Some(DateFormat::Strftime("%Y-%m-%d".into()));
        assert_ne!(iso.fingerprint().unwrap(), custom.fingerprint().unwrap());

        iso.fields[0].capabilities = FieldCapabilities::AGGREGATE;
        assert!(iso.fields[0].compiled_components().is_err());
    }

    #[test]
    fn statistics_admission_is_exact_and_bounded() {
        let capabilities = FieldCapabilities::EXACT
            .union(FieldCapabilities::RANGE)
            .union(FieldCapabilities::ORDER)
            .union(FieldCapabilities::FACET)
            .union(FieldCapabilities::AGGREGATE);
        let (mut accepted, mut rejected) = (1usize, 6_000usize);
        while accepted + 1 < rejected {
            let midpoint = accepted + (rejected - accepted) / 2;
            if admission_schema(midpoint, FieldType::SignedInteger, capabilities, true)
                .validate()
                .is_ok()
            {
                accepted = midpoint;
            } else {
                rejected = midpoint;
            }
        }
        let previous = admission_schema(accepted, FieldType::SignedInteger, capabilities, true);
        previous.validate().unwrap();
        assert!(
            admission_schema(rejected, FieldType::SignedInteger, capabilities, true)
                .validate()
                .is_err()
        );
        assert_eq!(
            maximum_statistics(&previous)
                .encode_payload()
                .unwrap()
                .len(),
            previous.segment_shape().unwrap().statistics_payload_bytes
        );
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

    #[test]
    fn physical_order_admits_one_maximum_keyword_and_rejects_oversized_composites() {
        let capabilities = FieldCapabilities::ORDER;
        let mut one = admission_schema(1, FieldType::Keyword, capabilities, true);
        one.physical_order[0].field_id = FieldId::new(0);
        one.validate().unwrap();
        assert_eq!(
            one.maximum_physical_order_key_bytes().unwrap(),
            2 * INDEX_TERM_BYTES + 4
        );
        let maximum_key = encode_physical_order_key(&[(
            SortValue::Value(ScalarValue::String("\0".repeat(INDEX_TERM_BYTES))),
            OrderDirection::Ascending,
        )])
        .unwrap();
        assert_eq!(
            maximum_key.len(),
            one.maximum_physical_order_key_bytes().unwrap()
        );
        assert_eq!(
            maximum_statistics(&one).encode_payload().unwrap().len(),
            one.segment_shape().unwrap().statistics_payload_bytes
        );

        let mut composite = admission_schema(4, FieldType::Keyword, capabilities, false);
        composite.physical_order = composite
            .fields
            .iter()
            .map(|field| OrderField {
                field_id: field.id,
                direction: OrderDirection::Ascending,
            })
            .collect();
        assert!(matches!(
            composite.validate(),
            Err(IndexError::ResourceLimit { .. })
        ));
    }
}
