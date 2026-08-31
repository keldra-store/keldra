//! Storage-neutral Typed JSON definition and query vocabulary.
//!
//! These types describe the public Typed JSON contract. They intentionally do
//! not describe a segment layout, a cursor encoding, or an execution engine.
//! The v6 projection pipeline consumes their canonical recipe fingerprints;
//! query materializers may consume the predicate, facet, aggregate, and order
//! shapes without inheriting a durable index format.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::IndexError;

const TYPED_JSON_SCHEMA_DOMAIN: &[u8] = b"keldra.index.typed-json-schema/v1";
const MEMBERSHIP_RECIPE_DOMAIN: &[u8] = b"keldra.index.membership-recipe/v2";
const FIELD_RECIPE_DOMAIN: &[u8] = b"keldra.index.field-recipe/v2";
const MAX_SELECTOR_BYTES: usize = 16 * 1024;
const MAX_TERM_BYTES: usize = 32_766;

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
pub enum FieldType {
    Boolean = 1,
    SignedInteger = 2,
    UnsignedInteger = 3,
    Float = 4,
    Keyword = 5,
    Text = 6,
    /// A signed Unix-epoch millisecond scalar whose public spelling is
    /// governed by [`DateFormat`].
    Date = 7,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Analyzer {
    Keyword = 1,
    UnicodeAlphanumericLowercase = 2,
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
}

impl FieldSchema {
    /// The public format for date values. An omitted date format is explicitly
    /// the Typed JSON default, ISO 8601; non-date fields return `None`.
    pub fn effective_date_format(&self) -> Option<DateFormat> {
        match self.field_type {
            FieldType::Date => Some(self.date_format.clone().unwrap_or(DateFormat::Iso8601)),
            _ => None,
        }
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        self.capabilities.validate()?;
        validate_string(&self.name, "field name")?;
        validate_string(&self.source_selector, "field selector")?;
        match self.field_type {
            FieldType::Boolean => reject_capabilities(
                self.capabilities,
                FieldCapabilities::EXACT.union(FieldCapabilities::FACET),
                "Boolean",
            )?,
            FieldType::SignedInteger | FieldType::UnsignedInteger | FieldType::Float => {
                reject_capabilities(
                    self.capabilities,
                    FieldCapabilities::EXACT
                        .union(FieldCapabilities::RANGE)
                        .union(FieldCapabilities::ORDER)
                        .union(FieldCapabilities::FACET)
                        .union(FieldCapabilities::AGGREGATE),
                    "numeric",
                )?
            }
            FieldType::Date => reject_capabilities(
                self.capabilities,
                FieldCapabilities::EXACT
                    .union(FieldCapabilities::RANGE)
                    .union(FieldCapabilities::ORDER)
                    .union(FieldCapabilities::FACET),
                "date",
            )?,
            FieldType::Keyword => reject_capabilities(
                self.capabilities,
                FieldCapabilities::EXACT
                    .union(FieldCapabilities::PREFIX)
                    .union(FieldCapabilities::RANGE)
                    .union(FieldCapabilities::ORDER)
                    .union(FieldCapabilities::FACET),
                "keyword",
            )?,
            FieldType::Text if self.capabilities != FieldCapabilities::FULL_TEXT => {
                return Err(IndexError::InvalidDefinition(
                    "text fields support only FULL_TEXT".into(),
                ));
            }
            FieldType::Text => {}
        }
        match (self.field_type, self.analyzer) {
            (FieldType::Text, Some(Analyzer::UnicodeAlphanumericLowercase)) => {}
            (FieldType::Text, _) => {
                return Err(IndexError::InvalidDefinition(
                    "text fields require the UnicodeAlphanumericLowercase analyzer".into(),
                ));
            }
            (_, None) => {}
            (_, Some(_)) => {
                return Err(IndexError::InvalidDefinition(
                    "only text fields may declare an analyzer".into(),
                ));
            }
        }
        match (self.field_type, self.date_format.as_ref()) {
            (FieldType::Date, None | Some(DateFormat::Iso8601)) => {}
            (FieldType::Date, Some(DateFormat::Strftime(pattern))) => {
                validate_string(pattern, "date format")?;
            }
            (_, None) => {}
            (_, Some(_)) => {
                return Err(IndexError::InvalidDefinition(
                    "only date fields may declare a date format".into(),
                ));
            }
        }
        Ok(())
    }
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

/// Typed JSON source scope and logical field contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedJsonSchema {
    pub path_prefix: String,
    pub content_type_scope: Option<String>,
    pub fields: Vec<FieldSchema>,
    pub physical_order: Vec<OrderField>,
}

/// Definition-neutral identities used by the logical index catalogue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecipeFingerprints {
    pub membership: [u8; 32],
    pub fields: Vec<[u8; 32]>,
}

impl TypedJsonSchema {
    pub fn validate(&self) -> Result<(), IndexError> {
        validate_selector(&self.path_prefix, "path prefix")?;
        if let Some(content_type) = &self.content_type_scope {
            validate_string(content_type, "content type")?;
        }
        let mut names = BTreeSet::new();
        for (ordinal, field) in self.fields.iter().enumerate() {
            if field.id.get() != u32::try_from(ordinal).map_err(|_| IndexError::OffsetOverflow)?
                || !names.insert(field.name.as_str())
            {
                return Err(IndexError::InvalidDefinition(
                    "fields require dense IDs and unique names".into(),
                ));
            }
            field.validate()?;
        }
        let mut ordered = BTreeSet::new();
        for order in &self.physical_order {
            let field = self
                .fields
                .get(order.field_id.get() as usize)
                .ok_or_else(|| IndexError::InvalidDefinition("unknown order field".into()))?;
            if field.cardinality != Cardinality::Single
                || !field.capabilities.contains(FieldCapabilities::ORDER)
                || !ordered.insert(order.field_id)
            {
                return Err(IndexError::InvalidDefinition(
                    "order requires unique single-valued ORDER fields".into(),
                ));
            }
        }
        Ok(())
    }

    /// Assign dense IDs from recipe identity so public aliases and declaration
    /// order do not decide physical field ownership.
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
            let id =
                FieldId::new(u32::try_from(new_ordinal).map_err(|_| IndexError::OffsetOverflow)?);
            old_to_new[old_ordinal] = id;
            field.id = id;
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
        let mut membership = CanonicalBytes::new(MEMBERSHIP_RECIPE_DOMAIN);
        membership.string(&self.path_prefix)?;
        membership.optional_string(self.content_type_scope.as_deref())?;
        let membership = *blake3::hash(&membership.finish()).as_bytes();
        let fields = self
            .fields
            .iter()
            .map(|field| field_recipe(field, membership))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecipeFingerprints { membership, fields })
    }

    pub fn fingerprint(&self) -> Result<[u8; 32], IndexError> {
        self.validate()?;
        let mut out = CanonicalBytes::new(TYPED_JSON_SCHEMA_DOMAIN);
        out.string(&self.path_prefix)?;
        out.optional_string(self.content_type_scope.as_deref())?;
        out.usize(self.fields.len())?;
        for field in &self.fields {
            encode_field_contract(&mut out, field)?;
        }
        out.usize(self.physical_order.len())?;
        for order in &self.physical_order {
            out.u32(order.field_id.get());
            out.u8(order.direction as u8);
        }
        Ok(*blake3::hash(&out.finish()).as_bytes())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Signed(i64),
    /// Canonical finite IEEE-754 binary64 bits. Negative zero is normalized.
    Number(u64),
    Unsigned(u64),
    String(String),
}

impl ScalarValue {
    pub fn number(value: f64) -> Result<Self, IndexError> {
        if !value.is_finite() {
            return Err(IndexError::InvalidDefinition(
                "Typed JSON numbers must be finite".into(),
            ));
        }
        Ok(Self::Number(if value == 0.0 {
            0.0f64.to_bits()
        } else {
            value.to_bits()
        }))
    }

    pub fn as_number(&self) -> Option<f64> {
        match self {
            Self::Number(bits) => Some(f64::from_bits(*bits)),
            _ => None,
        }
    }

    pub fn exact_number_from_i64(value: i64) -> Option<Self> {
        let number = value as f64;
        ((number as i128) == i128::from(value)).then(|| Self::Number(number.to_bits()))
    }

    pub fn exact_number_from_u64(value: u64) -> Option<Self> {
        let number = value as f64;
        ((number as u128) == u128::from(value)).then(|| Self::Number(number.to_bits()))
    }

    fn tag(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::Signed(_) => 2,
            Self::Number(_) => 3,
            Self::Unsigned(_) => 4,
            Self::String(_) => 5,
        }
    }
}

impl Ord for ScalarValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tag()
            .cmp(&other.tag())
            .then_with(|| match (self, other) {
                (Self::Null, Self::Null) => Ordering::Equal,
                (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
                (Self::Signed(left), Self::Signed(right)) => left.cmp(right),
                (Self::Number(left), Self::Number(right)) => {
                    f64::from_bits(*left).total_cmp(&f64::from_bits(*right))
                }
                (Self::Unsigned(left), Self::Unsigned(right)) => left.cmp(right),
                (Self::String(left), Self::String(right)) => left.as_bytes().cmp(right.as_bytes()),
                _ => Ordering::Equal,
            })
    }
}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Encode one scalar into a self-delimiting byte sequence whose lexical byte
/// order is exactly [`ScalarValue`]'s canonical order. Query point and term
/// blocks use this representation for range seeks; it deliberately contains
/// no field or persistence-format coupling.
pub fn encode_scalar_sort_key(value: &ScalarValue) -> Result<Vec<u8>, IndexError> {
    let mut out = Vec::new();
    match value {
        ScalarValue::Null => out.push(0),
        ScalarValue::Boolean(value) => {
            out.extend_from_slice(&[1, u8::from(*value)]);
        }
        ScalarValue::Signed(value) => {
            out.push(2);
            out.extend_from_slice(&((*value as u64) ^ (1u64 << 63)).to_be_bytes());
        }
        ScalarValue::Number(bits) => {
            let number = f64::from_bits(*bits);
            if !number.is_finite() {
                return Err(IndexError::InvalidDefinition(
                    "Typed JSON numbers must be finite".into(),
                ));
            }
            out.push(3);
            let normalized = if number == 0.0 {
                0.0f64.to_bits()
            } else {
                *bits
            };
            let sortable = if normalized & (1u64 << 63) != 0 {
                !normalized
            } else {
                normalized ^ (1u64 << 63)
            };
            out.extend_from_slice(&sortable.to_be_bytes());
        }
        ScalarValue::Unsigned(value) => {
            out.push(4);
            out.extend_from_slice(&value.to_be_bytes());
        }
        ScalarValue::String(value) => {
            out.push(5);
            // Zero is escaped as `00 01` and `00 00` terminates the string.
            // Besides preserving bytewise string order, the distinct second
            // byte makes the key self-delimiting when a block-specific suffix
            // (for example a stable document key) follows it.
            for byte in value.as_bytes() {
                match byte {
                    0 => out.extend_from_slice(&[0, 1]),
                    _ => out.push(*byte),
                }
            }
            out.extend_from_slice(&[0, 0]);
        }
    }
    Ok(out)
}

/// Decode exactly one [`encode_scalar_sort_key`] value and return its consumed
/// length. The trailing bytes remain available for a stable document key or a
/// block-specific value.
pub fn decode_scalar_sort_key(bytes: &[u8]) -> Result<(ScalarValue, usize), IndexError> {
    let tag = *bytes.first().ok_or(IndexError::UnexpectedEof {
        expected: 1,
        actual: 0,
    })?;
    match tag {
        0 => Ok((ScalarValue::Null, 1)),
        1 => match bytes.get(1) {
            Some(0) => Ok((ScalarValue::Boolean(false), 2)),
            Some(1) => Ok((ScalarValue::Boolean(true), 2)),
            _ => Err(IndexError::InvalidFormat("Typed JSON scalar boolean")),
        },
        2 => {
            let encoded: [u8; 8] = bytes
                .get(1..9)
                .ok_or(IndexError::UnexpectedEof {
                    expected: 9,
                    actual: bytes.len() as u64,
                })?
                .try_into()
                .map_err(|_| IndexError::Integrity)?;
            Ok((
                ScalarValue::Signed((u64::from_be_bytes(encoded) ^ (1u64 << 63)) as i64),
                9,
            ))
        }
        3 => {
            let encoded: [u8; 8] = bytes
                .get(1..9)
                .ok_or(IndexError::UnexpectedEof {
                    expected: 9,
                    actual: bytes.len() as u64,
                })?
                .try_into()
                .map_err(|_| IndexError::Integrity)?;
            let sortable = u64::from_be_bytes(encoded);
            let bits = if sortable & (1u64 << 63) != 0 {
                sortable ^ (1u64 << 63)
            } else {
                !sortable
            };
            let value = ScalarValue::number(f64::from_bits(bits))?;
            if encode_scalar_sort_key(&value)? != bytes[..9] {
                return Err(IndexError::InvalidFormat("Typed JSON scalar number"));
            }
            Ok((value, 9))
        }
        4 => {
            let encoded: [u8; 8] = bytes
                .get(1..9)
                .ok_or(IndexError::UnexpectedEof {
                    expected: 9,
                    actual: bytes.len() as u64,
                })?
                .try_into()
                .map_err(|_| IndexError::Integrity)?;
            Ok((ScalarValue::Unsigned(u64::from_be_bytes(encoded)), 9))
        }
        5 => {
            let mut cursor = 1;
            let mut value = Vec::new();
            while let Some(byte) = bytes.get(cursor) {
                cursor += 1;
                match byte {
                    0 => match bytes.get(cursor) {
                        Some(0) => {
                            cursor += 1;
                            let value = String::from_utf8(value).map_err(|_| {
                                IndexError::InvalidFormat("Typed JSON scalar string")
                            })?;
                            return Ok((ScalarValue::String(value), cursor));
                        }
                        Some(1) => {
                            value.push(0);
                            cursor += 1;
                        }
                        _ => return Err(IndexError::InvalidFormat("Typed JSON scalar string")),
                    },
                    byte => value.push(*byte),
                }
            }
            Err(IndexError::UnexpectedEof {
                expected: cursor as u64 + 1,
                actual: bytes.len() as u64,
            })
        }
        _ => Err(IndexError::InvalidFormat("Typed JSON scalar tag")),
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct PredicateId(u32);

impl PredicateId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeBound {
    pub value: ScalarValue,
    pub inclusive: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    Equal {
        id: PredicateId,
        field_id: FieldId,
        value: ScalarValue,
    },
    In {
        id: PredicateId,
        field_id: FieldId,
        values: Vec<ScalarValue>,
    },
    Prefix {
        id: PredicateId,
        field_id: FieldId,
        prefix: String,
    },
    Range {
        id: PredicateId,
        field_id: FieldId,
        lower: Option<RangeBound>,
        upper: Option<RangeBound>,
    },
    Exists {
        id: PredicateId,
        field_id: FieldId,
    },
    FullText {
        id: PredicateId,
        field_id: FieldId,
        text: String,
    },
    Phrase {
        id: PredicateId,
        field_id: FieldId,
        text: String,
    },
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    pub fn validate(&self) -> Result<(), IndexError> {
        self.collect_ids(&mut BTreeSet::new())
    }

    pub fn leaf_id(&self) -> Option<PredicateId> {
        match self {
            Self::Equal { id, .. }
            | Self::In { id, .. }
            | Self::Prefix { id, .. }
            | Self::Range { id, .. }
            | Self::Exists { id, .. }
            | Self::FullText { id, .. }
            | Self::Phrase { id, .. } => Some(*id),
            Self::And(_) | Self::Or(_) | Self::Not(_) => None,
        }
    }

    fn collect_ids(&self, output: &mut BTreeSet<PredicateId>) -> Result<(), IndexError> {
        if let Some(id) = self.leaf_id() {
            if !output.insert(id) {
                return Err(IndexError::InvalidQuery(
                    "predicate IDs must be unique".into(),
                ));
            }
        }
        match self {
            Self::In { values, .. } if values.is_empty() => {
                Err(IndexError::InvalidQuery("IN requires a value".into()))
            }
            Self::Prefix { prefix, .. } if prefix.is_empty() || prefix.len() > MAX_TERM_BYTES => {
                Err(IndexError::InvalidQuery(
                    "prefix is empty or too long".into(),
                ))
            }
            Self::Range { lower, upper, .. } if lower.is_none() && upper.is_none() => {
                Err(IndexError::InvalidQuery("range requires a bound".into()))
            }
            Self::FullText { text, .. } | Self::Phrase { text, .. } if text.trim().is_empty() => {
                Err(IndexError::InvalidQuery("text predicate is empty".into()))
            }
            Self::And(children) | Self::Or(children) => {
                if children.is_empty() {
                    return Err(IndexError::InvalidQuery(
                        "Boolean predicate requires a child".into(),
                    ));
                }
                for child in children {
                    child.collect_ids(output)?;
                }
                Ok(())
            }
            Self::Not(child) => child.collect_ids(output),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FacetRequest {
    pub field_id: FieldId,
    pub limit: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FacetBucket {
    pub value: ScalarValue,
    pub count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FacetResult {
    pub field_id: FieldId,
    pub buckets: Vec<FacetBucket>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateOperation {
    Count,
    Minimum,
    Maximum,
    Sum,
    Average,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateRequest {
    pub field_id: FieldId,
    pub operation: AggregateOperation,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateResult {
    pub field_id: FieldId,
    pub operation: AggregateOperation,
    pub value: Option<ScalarValue>,
    pub contributing_count: u64,
}

fn field_recipe(field: &FieldSchema, membership: [u8; 32]) -> Result<[u8; 32], IndexError> {
    let mut out = CanonicalBytes::new(FIELD_RECIPE_DOMAIN);
    out.raw(&membership);
    encode_field_contract(&mut out, field)?;
    Ok(*blake3::hash(&out.finish()).as_bytes())
}

fn encode_field_contract(out: &mut CanonicalBytes, field: &FieldSchema) -> Result<(), IndexError> {
    out.string(&field.source_selector)?;
    out.u8(field.field_type as u8);
    out.u8(field.cardinality as u8);
    out.bool(field.allow_missing);
    out.bool(field.allow_null);
    out.u8(field.collation as u8);
    out.u16(field.capabilities.bits());
    out.optional_u8(field.analyzer.map(|value| value as u8));
    match field.effective_date_format().as_ref() {
        None => out.u8(0),
        Some(DateFormat::Iso8601) => out.u8(1),
        Some(DateFormat::Strftime(value)) => {
            out.u8(2);
            out.string(value)?;
        }
    }
    Ok(())
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

fn validate_selector(value: &str, name: &str) -> Result<(), IndexError> {
    if value.len() > MAX_SELECTOR_BYTES || value.contains('\0') {
        return Err(IndexError::InvalidDefinition(format!("{name} is invalid")));
    }
    Ok(())
}

fn validate_string(value: &str, name: &str) -> Result<(), IndexError> {
    if value.is_empty() || value.len() > MAX_SELECTOR_BYTES || value.contains('\0') {
        return Err(IndexError::InvalidDefinition(format!("{name} is invalid")));
    }
    Ok(())
}

const FIELD_STATE_MAGIC: &[u8; 8] = b"KTJSF001";
const FIELD_STATE_FORMAT: u8 = 1;

/// Exact selected values for one Typed JSON field at one document key.
///
/// It is the canonical value carried by a v6 field-recipe component. It has
/// no segment, object-path, or definition identity: the recipe component is
/// already bound to one validated [`FieldSchema`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedJsonFieldState {
    pub present: bool,
    pub null: bool,
    pub values: Vec<ScalarValue>,
}

impl TypedJsonFieldState {
    pub const fn missing() -> Self {
        Self {
            present: false,
            null: false,
            values: Vec::new(),
        }
    }

    /// Normalize selected scalar values into the one field-state contract.
    /// `None` is a missing selector; explicit JSON null stays distinct from a
    /// present empty multi-value field.
    pub fn from_selected(
        field: &FieldSchema,
        selected: Option<Vec<ScalarValue>>,
    ) -> Result<Self, IndexError> {
        let Some(selected) = selected else {
            return Ok(Self::missing());
        };
        let mut state = Self {
            present: true,
            null: false,
            values: Vec::with_capacity(selected.len()),
        };
        for value in selected {
            match value {
                ScalarValue::Null => state.null = true,
                value => state.values.push(value),
            }
        }
        canonicalize_field_state(field, &mut state)?;
        Ok(state)
    }
}

/// Encode one exact field state using a small storage-neutral canonical codec.
pub fn encode_typed_json_field_state(
    field: &FieldSchema,
    state: &TypedJsonFieldState,
) -> Result<Vec<u8>, IndexError> {
    let mut normalized = state.clone();
    canonicalize_field_state(field, &mut normalized)?;
    if &normalized != state {
        return Err(IndexError::InvalidDefinition(
            "Typed JSON field state is not canonical".into(),
        ));
    }
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(FIELD_STATE_MAGIC);
    out.push(FIELD_STATE_FORMAT);
    out.push(u8::from(state.present));
    out.push(u8::from(state.null));
    put_len(&mut out, state.values.len())?;
    for value in &state.values {
        encode_scalar(&mut out, value)?;
    }
    Ok(out)
}

/// Decode and validate one exact canonical field state.
pub fn decode_typed_json_field_state(
    field: &FieldSchema,
    bytes: &[u8],
) -> Result<TypedJsonFieldState, IndexError> {
    field.validate()?;
    let mut input = FieldStateDecoder::new(bytes);
    input.expect(FIELD_STATE_MAGIC)?;
    if input.byte()? != FIELD_STATE_FORMAT {
        return Err(IndexError::InvalidFormat(
            "Typed JSON field state format is unsupported",
        ));
    }
    let present = input.bool()?;
    let null = input.bool()?;
    let count = input.len()?;
    if count > input.remaining() {
        return Err(IndexError::InvalidFormat(
            "Typed JSON field-state value count is unbounded",
        ));
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(decode_scalar(&mut input)?);
    }
    input.finish()?;
    let state = TypedJsonFieldState {
        present,
        null,
        values,
    };
    let canonical = encode_typed_json_field_state(field, &state)?;
    if canonical != bytes {
        return Err(IndexError::InvalidFormat(
            "Typed JSON field state is not canonical",
        ));
    }
    Ok(state)
}

/// Verify one leaf predicate against a candidate selected by a query run.
///
/// This is intentionally a single-candidate verifier, not a document scan:
/// v6 query runs first seek a term, point, or positional posting structure and
/// call this only to discard stale L0 candidates after a newer field update.
pub fn matches_typed_json_leaf(
    field: &FieldSchema,
    state: &TypedJsonFieldState,
    predicate: &Predicate,
) -> Result<bool, IndexError> {
    let mut normalized = state.clone();
    canonicalize_field_state(field, &mut normalized)?;
    if normalized != *state {
        return Err(IndexError::InvalidDefinition(
            "Typed JSON candidate field state is not canonical".into(),
        ));
    }
    match predicate {
        Predicate::Equal {
            field_id, value, ..
        } => {
            require_leaf_capability(field, *field_id, FieldCapabilities::EXACT)?;
            value_matches_field(field, value)?;
            Ok((matches!(value, ScalarValue::Null) && state.null)
                || state.values.iter().any(|candidate| candidate == value))
        }
        Predicate::In {
            field_id, values, ..
        } => {
            require_leaf_capability(field, *field_id, FieldCapabilities::EXACT)?;
            for value in values {
                value_matches_field(field, value)?;
            }
            Ok(values.iter().any(|value| {
                (matches!(value, ScalarValue::Null) && state.null)
                    || state.values.iter().any(|candidate| candidate == value)
            }))
        }
        Predicate::Prefix {
            field_id, prefix, ..
        } => {
            require_leaf_capability(field, *field_id, FieldCapabilities::PREFIX)?;
            Ok(state.values.iter().any(
                |value| matches!(value, ScalarValue::String(value) if value.starts_with(prefix)),
            ))
        }
        Predicate::Range {
            field_id,
            lower,
            upper,
            ..
        } => {
            require_leaf_capability(field, *field_id, FieldCapabilities::RANGE)?;
            for bound in lower.iter().chain(upper.iter()) {
                value_matches_field(field, &bound.value)?;
                if matches!(bound.value, ScalarValue::Null) {
                    return Err(IndexError::InvalidQuery(
                        "range does not accept null".into(),
                    ));
                }
            }
            Ok(state.values.iter().any(|value| {
                lower.as_ref().is_none_or(|bound| {
                    value > &bound.value || (bound.inclusive && value == &bound.value)
                }) && upper.as_ref().is_none_or(|bound| {
                    value < &bound.value || (bound.inclusive && value == &bound.value)
                })
            }))
        }
        Predicate::Exists { field_id, .. } => {
            require_leaf_capability(field, *field_id, FieldCapabilities::EXACT)?;
            Ok(state.present)
        }
        Predicate::FullText { field_id, text, .. } => {
            require_leaf_capability(field, *field_id, FieldCapabilities::FULL_TEXT)?;
            let query = analyze_typed_json_text(text);
            Ok(!query.is_empty()
                && state.values.iter().any(|value| match value {
                    ScalarValue::String(value) => {
                        let terms = analyze_typed_json_text(value);
                        query.iter().all(|term| terms.contains(term))
                    }
                    _ => false,
                }))
        }
        Predicate::Phrase { field_id, text, .. } => {
            require_leaf_capability(field, *field_id, FieldCapabilities::FULL_TEXT)?;
            let query = analyze_typed_json_text(text);
            Ok(!query.is_empty()
                && state.values.iter().any(|value| match value {
                    ScalarValue::String(value) => analyze_typed_json_text(value)
                        .windows(query.len())
                        .any(|window| window == query.as_slice()),
                    _ => false,
                }))
        }
        Predicate::And(_) | Predicate::Or(_) | Predicate::Not(_) => Err(IndexError::InvalidQuery(
            "candidate verifier requires a leaf predicate".into(),
        )),
    }
}

fn canonicalize_field_state(
    field: &FieldSchema,
    state: &mut TypedJsonFieldState,
) -> Result<(), IndexError> {
    field.validate()?;
    if !state.present {
        if state.null || !state.values.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "missing field state contains values".into(),
            ));
        }
        if !field.allow_missing {
            return Err(IndexError::InvalidDefinition(
                "required field is missing".into(),
            ));
        }
        return Ok(());
    }
    if state.null && !field.allow_null {
        return Err(IndexError::InvalidDefinition(
            "field state contains a disallowed null".into(),
        ));
    }
    if field.cardinality == Cardinality::Single
        && state
            .values
            .len()
            .checked_add(usize::from(state.null))
            .ok_or(IndexError::OffsetOverflow)?
            > 1
    {
        return Err(IndexError::InvalidDefinition(
            "single-valued field state has multiple values".into(),
        ));
    }
    for value in &state.values {
        value_matches_field(field, value)?;
    }
    if field.field_type == FieldType::Keyword && field.cardinality == Cardinality::Multi {
        state.values.sort_unstable();
        state.values.dedup();
    }
    Ok(())
}

fn require_leaf_capability(
    field: &FieldSchema,
    field_id: FieldId,
    capability: FieldCapabilities,
) -> Result<(), IndexError> {
    if field.id != field_id || !field.capabilities.contains(capability) {
        return Err(IndexError::InvalidQuery(
            "predicate is unsupported by the field".into(),
        ));
    }
    Ok(())
}

fn value_matches_field(field: &FieldSchema, value: &ScalarValue) -> Result<(), IndexError> {
    let valid = matches!(
        (field.field_type, value),
        (_, ScalarValue::Null)
            | (FieldType::Boolean, ScalarValue::Boolean(_))
            | (
                FieldType::SignedInteger | FieldType::Date,
                ScalarValue::Signed(_)
            )
            | (FieldType::UnsignedInteger, ScalarValue::Unsigned(_))
            | (FieldType::Float, ScalarValue::Number(_))
            | (FieldType::Keyword | FieldType::Text, ScalarValue::String(_))
    );
    if !valid {
        return Err(IndexError::InvalidQuery(
            "scalar does not match its Typed JSON field".into(),
        ));
    }
    if let ScalarValue::String(value) = value {
        if value.len() > MAX_TERM_BYTES && field.field_type == FieldType::Keyword {
            return Err(IndexError::InvalidQuery("keyword value is too long".into()));
        }
    }
    Ok(())
}

/// Canonical analyzer used by Typed JSON text recipes and v6 text postings.
pub fn analyze_typed_json_text(value: &str) -> Vec<String> {
    value
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn encode_scalar(out: &mut Vec<u8>, value: &ScalarValue) -> Result<(), IndexError> {
    match value {
        ScalarValue::Null => Err(IndexError::InvalidDefinition(
            "field state must represent null separately".into(),
        )),
        ScalarValue::Boolean(value) => {
            out.push(1);
            out.push(u8::from(*value));
            Ok(())
        }
        ScalarValue::Signed(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_be_bytes());
            Ok(())
        }
        ScalarValue::Unsigned(value) => {
            out.push(3);
            out.extend_from_slice(&value.to_be_bytes());
            Ok(())
        }
        ScalarValue::Number(value) => {
            out.push(4);
            out.extend_from_slice(&value.to_be_bytes());
            Ok(())
        }
        ScalarValue::String(value) => {
            out.push(5);
            put_len(out, value.len())?;
            out.extend_from_slice(value.as_bytes());
            Ok(())
        }
    }
}

fn decode_scalar(input: &mut FieldStateDecoder<'_>) -> Result<ScalarValue, IndexError> {
    match input.byte()? {
        1 => Ok(ScalarValue::Boolean(input.bool()?)),
        2 => Ok(ScalarValue::Signed(i64::from_be_bytes(input.array_8()?))),
        3 => Ok(ScalarValue::Unsigned(u64::from_be_bytes(input.array_8()?))),
        4 => ScalarValue::number(f64::from_bits(u64::from_be_bytes(input.array_8()?))),
        5 => Ok(ScalarValue::String(input.string()?)),
        _ => Err(IndexError::InvalidFormat("Typed JSON scalar tag")),
    }
}

fn put_len(out: &mut Vec<u8>, value: usize) -> Result<(), IndexError> {
    out.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| IndexError::OffsetOverflow)?
            .to_be_bytes(),
    );
    Ok(())
}

struct FieldStateDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FieldStateDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], IndexError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(IndexError::UnexpectedEof {
                expected: end as u64,
                actual: self.bytes.len() as u64,
            })?;
        self.offset = end;
        Ok(value)
    }
    fn expect(&mut self, expected: &[u8]) -> Result<(), IndexError> {
        if self.take(expected.len())? != expected {
            return Err(IndexError::InvalidFormat("Typed JSON field-state magic"));
        }
        Ok(())
    }
    fn byte(&mut self) -> Result<u8, IndexError> {
        Ok(self.take(1)?[0])
    }
    fn bool(&mut self) -> Result<bool, IndexError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(IndexError::InvalidFormat("Typed JSON boolean")),
        }
    }
    fn array_8(&mut self) -> Result<[u8; 8], IndexError> {
        self.take(8)?.try_into().map_err(|_| IndexError::Integrity)
    }
    fn len(&mut self) -> Result<usize, IndexError> {
        Ok(usize::try_from(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        ))
        .map_err(|_| IndexError::OffsetOverflow)?)
    }
    fn string(&mut self) -> Result<String, IndexError> {
        let length = self.len()?;
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| IndexError::InvalidFormat("Typed JSON string"))
    }
    fn finish(self) -> Result<(), IndexError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(IndexError::InvalidFormat(
                "trailing Typed JSON field-state bytes",
            ))
        }
    }
}

struct CanonicalBytes(Vec<u8>);

impl CanonicalBytes {
    fn new(domain: &[u8]) -> Self {
        Self(domain.to_vec())
    }
    fn finish(self) -> Vec<u8> {
        self.0
    }
    fn raw(&mut self, value: &[u8]) {
        self.0.extend_from_slice(value);
    }
    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn u16(&mut self, value: u16) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }
    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }
    fn usize(&mut self, value: usize) -> Result<(), IndexError> {
        self.u32(u32::try_from(value).map_err(|_| IndexError::OffsetOverflow)?);
        Ok(())
    }
    fn optional_u8(&mut self, value: Option<u8>) {
        self.bool(value.is_some());
        if let Some(value) = value {
            self.u8(value);
        }
    }
    fn optional_string(&mut self, value: Option<&str>) -> Result<(), IndexError> {
        self.bool(value.is_some());
        if let Some(value) = value {
            self.string(value)?;
        }
        Ok(())
    }
    fn string(&mut self, value: &str) -> Result<(), IndexError> {
        self.usize(value.len())?;
        self.raw(value.as_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyword_field(id: u32, name: &str, selector: &str) -> FieldSchema {
        FieldSchema {
            id: FieldId::new(id),
            name: name.into(),
            source_selector: selector.into(),
            field_type: FieldType::Keyword,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::EXACT,
            analyzer: None,
            date_format: None,
        }
    }

    #[test]
    fn aliases_and_declaration_order_share_recipe_identity() {
        let first = TypedJsonSchema {
            path_prefix: "/objects".into(),
            content_type_scope: None,
            fields: vec![
                keyword_field(0, "one", "/status"),
                keyword_field(1, "two", "/kind"),
            ],
            physical_order: Vec::new(),
        };
        let second = TypedJsonSchema {
            path_prefix: "/objects".into(),
            content_type_scope: None,
            fields: vec![
                keyword_field(0, "anything", "/kind"),
                keyword_field(1, "else", "/status"),
            ],
            physical_order: Vec::new(),
        };
        let first_recipes = first.recipe_fingerprints().unwrap();
        let second_recipes = second.recipe_fingerprints().unwrap();
        assert_eq!(first_recipes.membership, second_recipes.membership);
        let mut first_fields = first_recipes.fields;
        first_fields.sort_unstable();
        let mut second_fields = second_recipes.fields;
        second_fields.sort_unstable();
        assert_eq!(first_fields, second_fields);
    }

    #[test]
    fn rejects_non_typed_json_capability_combinations() {
        let mut field = keyword_field(0, "status", "/status");
        field.capabilities = FieldCapabilities::FULL_TEXT;
        assert!(field.validate().is_err());
    }

    #[test]
    fn omitted_date_format_is_the_iso8601_contract() {
        let base = FieldSchema {
            id: FieldId::new(0),
            name: "published".into(),
            source_selector: "/published".into(),
            field_type: FieldType::Date,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::RANGE,
            analyzer: None,
            date_format: None,
        };
        let explicit = FieldSchema {
            date_format: Some(DateFormat::Iso8601),
            ..base.clone()
        };
        assert!(base.validate().is_ok());
        assert_eq!(
            field_recipe(&base, [7; 32]).unwrap(),
            field_recipe(&explicit, [7; 32]).unwrap()
        );
    }

    #[test]
    fn predicate_ids_are_unique_across_boolean_tree() {
        let predicate = Predicate::And(vec![
            Predicate::Exists {
                id: PredicateId::new(1),
                field_id: FieldId::new(0),
            },
            Predicate::Not(Box::new(Predicate::Exists {
                id: PredicateId::new(1),
                field_id: FieldId::new(1),
            })),
        ]);
        assert!(predicate.validate().is_err());
    }

    #[test]
    fn field_state_codec_is_exact_and_rejects_noncanonical_keyword_values() {
        let mut field = keyword_field(0, "labels", "/labels");
        field.cardinality = Cardinality::Multi;
        let state = TypedJsonFieldState::from_selected(
            &field,
            Some(vec![
                ScalarValue::String("beta".into()),
                ScalarValue::String("alpha".into()),
                ScalarValue::String("alpha".into()),
            ]),
        )
        .unwrap();
        assert_eq!(
            state.values,
            vec![
                ScalarValue::String("alpha".into()),
                ScalarValue::String("beta".into()),
            ]
        );
        let encoded = encode_typed_json_field_state(&field, &state).unwrap();
        assert_eq!(
            decode_typed_json_field_state(&field, &encoded).unwrap(),
            state
        );
        let noncanonical = TypedJsonFieldState {
            present: true,
            null: false,
            values: vec![
                ScalarValue::String("beta".into()),
                ScalarValue::String("alpha".into()),
            ],
        };
        assert!(encode_typed_json_field_state(&field, &noncanonical).is_err());
    }

    #[test]
    fn scalar_sort_keys_round_trip_and_preserve_scalar_order() {
        let values = vec![
            ScalarValue::Null,
            ScalarValue::Boolean(false),
            ScalarValue::Boolean(true),
            ScalarValue::Signed(-1),
            ScalarValue::Signed(0),
            ScalarValue::Number((-1.5f64).to_bits()),
            ScalarValue::Number(0.0f64.to_bits()),
            ScalarValue::Number(1.5f64.to_bits()),
            ScalarValue::Unsigned(0),
            ScalarValue::Unsigned(1),
            ScalarValue::String(String::new()),
            ScalarValue::String("a".into()),
            ScalarValue::String("a\0".into()),
            ScalarValue::String("a\0b".into()),
            ScalarValue::String("a\u{1}".into()),
            ScalarValue::String("z".into()),
        ];
        let keys = values
            .iter()
            .map(encode_scalar_sort_key)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        for (value, key) in values.iter().zip(keys) {
            assert_eq!(
                decode_scalar_sort_key(&key).unwrap(),
                (value.clone(), key.len())
            );
            let mut suffixed = key.clone();
            suffixed.extend_from_slice(&[0xff; 32]);
            assert_eq!(
                decode_scalar_sort_key(&suffixed).unwrap(),
                (value.clone(), key.len())
            );
        }
    }
}
