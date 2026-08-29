//! Type-safe construction of Typed JSON index definitions.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;

use jiff::Timestamp;
use jiff::fmt::strtime::{self, BrokenDownTime};
use keldra_api::v1::index_field::FieldType;
use keldra_api::v1::index_specification::Specification;
use keldra_api::v1::{
    BooleanIndexField, CreateIndexRequest, DateIndexField, FloatIndexField, IndexField,
    IndexFieldCapability, IndexFieldCardinality, IndexOrder, IndexOrderDirection,
    IndexSpecification, KeywordIndexField, SignedIntegerIndexField, TextAnalyzer, TextIndexField,
    TypedJsonIndexSpec, UnsignedIntegerIndexField,
};

const MAX_INDEX_NAME_BYTES: usize = 128;
const MAX_OBJECT_BUCKET_BYTES: usize = 256;
const MAX_OBJECT_PATH_BYTES: usize = 4_096;
const MAX_CONTENT_TYPE_BYTES: usize = 512;
const MAX_COMMAND_ID_BYTES: usize = 256;

/// A definition rejected before a request is sent to Keldra.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexDefinitionError {
    InvalidBucket,
    InvalidIndexName,
    InvalidPathPrefix,
    InvalidContentType,
    InvalidCommandId,
    InvalidFieldName(String),
    InvalidJsonPointer(String),
    DuplicateFieldName(String),
    DuplicatePhysicalOrder(String),
    UnknownPhysicalOrderField(String),
    UnorderablePhysicalOrderField(String),
    InvalidDateFormat,
}

impl Display for IndexDefinitionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBucket => {
                formatter.write_str(
                    "bucket must be a 1..=256-byte canonical component without '/' or control characters",
                )
            }
            Self::InvalidIndexName => formatter
                .write_str("index name must be 1..=128 ASCII letters, digits, '.', '-' or '_'"),
            Self::InvalidPathPrefix => formatter
                .write_str("index path prefix must be at most 4096 bytes and contain no NUL"),
            Self::InvalidContentType => formatter
                .write_str("index content type must be at most 512 bytes and contain no NUL"),
            Self::InvalidCommandId => {
                formatter.write_str("command_id must contain 1 to 256 bytes and no NUL")
            }
            Self::InvalidFieldName(name) => write!(
                formatter,
                "index field name `{name}` must be non-empty and contain no NUL"
            ),
            Self::InvalidJsonPointer(pointer) => write!(
                formatter,
                "JSON pointer `{pointer}` must be empty or begin with '/' and contain no NUL"
            ),
            Self::DuplicateFieldName(name) => {
                write!(formatter, "typed JSON field name `{name}` is duplicated")
            }
            Self::DuplicatePhysicalOrder(name) => {
                write!(formatter, "physical-order field `{name}` is duplicated")
            }
            Self::UnknownPhysicalOrderField(name) => write!(
                formatter,
                "physical-order field `{name}` is not part of this definition"
            ),
            Self::UnorderablePhysicalOrderField(name) => write!(
                formatter,
                "physical-order field `{name}` must be single-valued and declare ORDER"
            ),
            Self::InvalidDateFormat => formatter.write_str(
                "date strftime pattern must be valid, bounded, and describe a complete date",
            ),
        }
    }
}

impl Error for IndexDefinitionError {}

#[derive(Clone, Debug)]
struct FieldCore {
    name: String,
    json_pointer: String,
}

impl FieldCore {
    fn new(name: impl Into<String>, json_pointer: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            json_pointer: json_pointer.into(),
        }
    }

    fn into_proto(self, multi: bool, capabilities: Vec<i32>, field_type: FieldType) -> IndexField {
        IndexField {
            name: self.name,
            json_pointer: self.json_pointer,
            cardinality: if multi {
                IndexFieldCardinality::Multi as i32
            } else {
                IndexFieldCardinality::Single as i32
            },
            capabilities,
            field_type: Some(field_type),
        }
    }
}

mod private {
    pub trait Sealed {}
}

/// Implemented only by complete typed field builders supplied by this crate.
#[doc(hidden)]
pub trait TypedJsonField: private::Sealed {
    #[doc(hidden)]
    fn into_index_field(self) -> IndexField;
}

/// An order declaration that can only be obtained from a single-valued field
/// carrying the `ORDER` capability.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexOrderToken {
    order: IndexOrder,
}

impl IndexOrderToken {
    fn new(name: &str, direction: IndexOrderDirection) -> Self {
        Self {
            order: IndexOrder {
                field: name.to_owned(),
                direction: direction as i32,
            },
        }
    }
}

fn capabilities<
    const E: bool,
    const P: bool,
    const R: bool,
    const O: bool,
    const F: bool,
    const A: bool,
    const T: bool,
>() -> Vec<i32> {
    let mut values = Vec::with_capacity(7);
    if E {
        values.push(IndexFieldCapability::Exact as i32);
    }
    if P {
        values.push(IndexFieldCapability::Prefix as i32);
    }
    if R {
        values.push(IndexFieldCapability::Range as i32);
    }
    if O {
        values.push(IndexFieldCapability::Order as i32);
    }
    if F {
        values.push(IndexFieldCapability::Facet as i32);
    }
    if A {
        values.push(IndexFieldCapability::Aggregate as i32);
    }
    if T {
        values.push(IndexFieldCapability::FullText as i32);
    }
    values
}

/// A Boolean field supporting exact matching and facets.
pub struct BooleanField<
    const MULTI: bool = false,
    const EXACT: bool = false,
    const FACET: bool = false,
    const CONFIGURED: bool = false,
> {
    core: FieldCore,
}

impl BooleanField {
    pub fn single(name: impl Into<String>, json_pointer: impl Into<String>) -> Self {
        Self {
            core: FieldCore::new(name, json_pointer),
        }
    }

    pub fn multi(name: impl Into<String>, json_pointer: impl Into<String>) -> BooleanField<true> {
        BooleanField {
            core: FieldCore::new(name, json_pointer),
        }
    }
}

impl<const M: bool, const F: bool, const C: bool> BooleanField<M, false, F, C> {
    pub fn exact(self) -> BooleanField<M, true, F, true> {
        BooleanField { core: self.core }
    }
}

impl<const M: bool, const E: bool, const C: bool> BooleanField<M, E, false, C> {
    pub fn facet(self) -> BooleanField<M, E, true, true> {
        BooleanField { core: self.core }
    }
}

impl<const M: bool, const E: bool, const F: bool> private::Sealed for BooleanField<M, E, F, true> {}
impl<const M: bool, const E: bool, const F: bool> TypedJsonField for BooleanField<M, E, F, true> {
    fn into_index_field(self) -> IndexField {
        self.core.into_proto(
            M,
            capabilities::<E, false, false, false, F, false, false>(),
            FieldType::Boolean(BooleanIndexField {}),
        )
    }
}

macro_rules! numeric_field {
    ($name:ident, $proto:ident, $variant:ident) => {
        pub struct $name<
            const MULTI: bool = false,
            const EXACT: bool = false,
            const RANGE: bool = false,
            const ORDER: bool = false,
            const FACET: bool = false,
            const AGGREGATE: bool = false,
            const CONFIGURED: bool = false,
        > {
            core: FieldCore,
        }

        impl $name {
            pub fn single(name: impl Into<String>, json_pointer: impl Into<String>) -> Self {
                Self {
                    core: FieldCore::new(name, json_pointer),
                }
            }
            pub fn multi(name: impl Into<String>, json_pointer: impl Into<String>) -> $name<true> {
                $name {
                    core: FieldCore::new(name, json_pointer),
                }
            }
        }

        impl<
            const M: bool,
            const R: bool,
            const O: bool,
            const F: bool,
            const A: bool,
            const C: bool,
        > $name<M, false, R, O, F, A, C>
        {
            pub fn exact(self) -> $name<M, true, R, O, F, A, true> {
                $name { core: self.core }
            }
        }

        impl<
            const M: bool,
            const E: bool,
            const O: bool,
            const F: bool,
            const A: bool,
            const C: bool,
        > $name<M, E, false, O, F, A, C>
        {
            pub fn range(self) -> $name<M, E, true, O, F, A, true> {
                $name { core: self.core }
            }
        }

        impl<const E: bool, const R: bool, const F: bool, const A: bool, const C: bool>
            $name<false, E, R, false, F, A, C>
        {
            pub fn order(self) -> $name<false, E, R, true, F, A, true> {
                $name { core: self.core }
            }
        }

        impl<
            const M: bool,
            const E: bool,
            const R: bool,
            const O: bool,
            const A: bool,
            const C: bool,
        > $name<M, E, R, O, false, A, C>
        {
            pub fn facet(self) -> $name<M, E, R, O, true, A, true> {
                $name { core: self.core }
            }
        }

        impl<
            const M: bool,
            const E: bool,
            const R: bool,
            const O: bool,
            const F: bool,
            const C: bool,
        > $name<M, E, R, O, F, false, C>
        {
            pub fn aggregate(self) -> $name<M, E, R, O, F, true, true> {
                $name { core: self.core }
            }
        }

        impl<const E: bool, const R: bool, const F: bool, const A: bool, const C: bool>
            $name<false, E, R, true, F, A, C>
        {
            pub fn ascending(&self) -> IndexOrderToken {
                IndexOrderToken::new(&self.core.name, IndexOrderDirection::Ascending)
            }
            pub fn descending(&self) -> IndexOrderToken {
                IndexOrderToken::new(&self.core.name, IndexOrderDirection::Descending)
            }
        }

        impl<
            const M: bool,
            const E: bool,
            const R: bool,
            const O: bool,
            const F: bool,
            const A: bool,
        > private::Sealed for $name<M, E, R, O, F, A, true>
        {
        }
        impl<
            const M: bool,
            const E: bool,
            const R: bool,
            const O: bool,
            const F: bool,
            const A: bool,
        > TypedJsonField for $name<M, E, R, O, F, A, true>
        {
            fn into_index_field(self) -> IndexField {
                self.core.into_proto(
                    M,
                    capabilities::<E, false, R, O, F, A, false>(),
                    FieldType::$variant($proto {}),
                )
            }
        }
    };
}

numeric_field!(SignedIntegerField, SignedIntegerIndexField, SignedInteger);
numeric_field!(
    UnsignedIntegerField,
    UnsignedIntegerIndexField,
    UnsignedInteger
);
numeric_field!(FloatField, FloatIndexField, Float);

/// Input and facet presentation format for a Date field.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DateFormat(DateFormatKind);

#[derive(Clone, Debug, Default, Eq, PartialEq)]
enum DateFormatKind {
    #[default]
    Iso8601,
    Strftime(String),
}

impl DateFormat {
    /// ISO-8601 input and canonical UTC ISO-8601 facet output.
    pub fn iso8601() -> Self {
        Self(DateFormatKind::Iso8601)
    }

    /// Construct one validated POSIX strftime/strptime pattern.
    pub fn strftime(pattern: impl Into<String>) -> Result<Self, IndexDefinitionError> {
        let pattern = pattern.into();
        if !valid_date_pattern(&pattern) {
            return Err(IndexDefinitionError::InvalidDateFormat);
        }
        Ok(Self(DateFormatKind::Strftime(pattern)))
    }

    fn strftime_pattern(&self) -> String {
        match &self.0 {
            DateFormatKind::Iso8601 => String::new(),
            DateFormatKind::Strftime(pattern) => pattern.clone(),
        }
    }
}

fn valid_date_pattern(pattern: &str) -> bool {
    if pattern.is_empty()
        || pattern.len() > 256
        || pattern.contains('\0')
        || uses_unsupported_date_directive(pattern)
    {
        return false;
    }
    let Ok(sample) = Timestamp::from_millisecond(1_721_016_123_456) else {
        return false;
    };
    let Ok(encoded) = strtime::format(pattern, sample) else {
        return false;
    };
    let Ok(parsed) = BrokenDownTime::parse(pattern, encoded) else {
        return false;
    };
    parsed.to_date().is_ok() && parsed.to_datetime().is_ok()
}

fn uses_unsupported_date_directive(pattern: &str) -> bool {
    let bytes = pattern.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            index += 1;
            continue;
        }
        index += 1;
        if index == bytes.len() {
            return true;
        }
        if bytes[index] == b'%' {
            index += 1;
            continue;
        }
        while index < bytes.len() && !bytes[index].is_ascii_alphabetic() && bytes[index] != b'+' {
            index += 1;
        }
        if index == bytes.len()
            || matches!(
                bytes[index],
                b'a' | b'A'
                    | b'b'
                    | b'B'
                    | b'h'
                    | b'c'
                    | b'x'
                    | b'X'
                    | b'p'
                    | b'P'
                    | b's'
                    | b'Z'
                    | b'Q'
                    | b'E'
                    | b'O'
            )
        {
            return true;
        }
        index += 1;
    }
    false
}

/// A timestamp field stored as signed Unix epoch milliseconds.
pub struct DateField<
    const MULTI: bool = false,
    const EXACT: bool = false,
    const RANGE: bool = false,
    const ORDER: bool = false,
    const FACET: bool = false,
    const CONFIGURED: bool = false,
> {
    core: FieldCore,
    format: DateFormat,
}

impl DateField {
    pub fn single(name: impl Into<String>, json_pointer: impl Into<String>) -> Self {
        Self {
            core: FieldCore::new(name, json_pointer),
            format: DateFormat::iso8601(),
        }
    }

    pub fn multi(name: impl Into<String>, json_pointer: impl Into<String>) -> DateField<true> {
        DateField {
            core: FieldCore::new(name, json_pointer),
            format: DateFormat::iso8601(),
        }
    }
}

impl<const M: bool, const E: bool, const R: bool, const O: bool, const F: bool, const C: bool>
    DateField<M, E, R, O, F, C>
{
    pub fn format(mut self, format: DateFormat) -> Self {
        self.format = format;
        self
    }
}

impl<const M: bool, const R: bool, const O: bool, const F: bool, const C: bool>
    DateField<M, false, R, O, F, C>
{
    pub fn exact(self) -> DateField<M, true, R, O, F, true> {
        DateField {
            core: self.core,
            format: self.format,
        }
    }
}

impl<const M: bool, const E: bool, const O: bool, const F: bool, const C: bool>
    DateField<M, E, false, O, F, C>
{
    pub fn range(self) -> DateField<M, E, true, O, F, true> {
        DateField {
            core: self.core,
            format: self.format,
        }
    }
}

impl<const E: bool, const R: bool, const F: bool, const C: bool>
    DateField<false, E, R, false, F, C>
{
    pub fn order(self) -> DateField<false, E, R, true, F, true> {
        DateField {
            core: self.core,
            format: self.format,
        }
    }
}

impl<const M: bool, const E: bool, const R: bool, const O: bool, const C: bool>
    DateField<M, E, R, O, false, C>
{
    pub fn facet(self) -> DateField<M, E, R, O, true, true> {
        DateField {
            core: self.core,
            format: self.format,
        }
    }
}

impl<const E: bool, const R: bool, const F: bool, const C: bool>
    DateField<false, E, R, true, F, C>
{
    pub fn ascending(&self) -> IndexOrderToken {
        IndexOrderToken::new(&self.core.name, IndexOrderDirection::Ascending)
    }

    pub fn descending(&self) -> IndexOrderToken {
        IndexOrderToken::new(&self.core.name, IndexOrderDirection::Descending)
    }
}

impl<const M: bool, const E: bool, const R: bool, const O: bool, const F: bool> private::Sealed
    for DateField<M, E, R, O, F, true>
{
}

impl<const M: bool, const E: bool, const R: bool, const O: bool, const F: bool> TypedJsonField
    for DateField<M, E, R, O, F, true>
{
    fn into_index_field(self) -> IndexField {
        self.core.into_proto(
            M,
            capabilities::<E, false, R, O, F, false, false>(),
            FieldType::Date(DateIndexField {
                strftime_pattern: self.format.strftime_pattern(),
            }),
        )
    }
}

/// An uninterpreted UTF-8 field with binary UTF-8 collation.
pub struct KeywordField<
    const MULTI: bool = false,
    const EXACT: bool = false,
    const PREFIX: bool = false,
    const RANGE: bool = false,
    const ORDER: bool = false,
    const FACET: bool = false,
    const CONFIGURED: bool = false,
> {
    core: FieldCore,
}

impl KeywordField {
    pub fn single(name: impl Into<String>, json_pointer: impl Into<String>) -> Self {
        Self {
            core: FieldCore::new(name, json_pointer),
        }
    }
    pub fn multi(name: impl Into<String>, json_pointer: impl Into<String>) -> KeywordField<true> {
        KeywordField {
            core: FieldCore::new(name, json_pointer),
        }
    }
}

impl<const M: bool, const P: bool, const R: bool, const O: bool, const F: bool, const C: bool>
    KeywordField<M, false, P, R, O, F, C>
{
    pub fn exact(self) -> KeywordField<M, true, P, R, O, F, true> {
        KeywordField { core: self.core }
    }
}
impl<const M: bool, const E: bool, const R: bool, const O: bool, const F: bool, const C: bool>
    KeywordField<M, E, false, R, O, F, C>
{
    pub fn prefix(self) -> KeywordField<M, E, true, R, O, F, true> {
        KeywordField { core: self.core }
    }
}
impl<const M: bool, const E: bool, const P: bool, const O: bool, const F: bool, const C: bool>
    KeywordField<M, E, P, false, O, F, C>
{
    pub fn range(self) -> KeywordField<M, E, P, true, O, F, true> {
        KeywordField { core: self.core }
    }
}
impl<const E: bool, const P: bool, const R: bool, const F: bool, const C: bool>
    KeywordField<false, E, P, R, false, F, C>
{
    pub fn order(self) -> KeywordField<false, E, P, R, true, F, true> {
        KeywordField { core: self.core }
    }
}
impl<const M: bool, const E: bool, const P: bool, const R: bool, const O: bool, const C: bool>
    KeywordField<M, E, P, R, O, false, C>
{
    pub fn facet(self) -> KeywordField<M, E, P, R, O, true, true> {
        KeywordField { core: self.core }
    }
}
impl<const E: bool, const P: bool, const R: bool, const F: bool, const C: bool>
    KeywordField<false, E, P, R, true, F, C>
{
    pub fn ascending(&self) -> IndexOrderToken {
        IndexOrderToken::new(&self.core.name, IndexOrderDirection::Ascending)
    }
    pub fn descending(&self) -> IndexOrderToken {
        IndexOrderToken::new(&self.core.name, IndexOrderDirection::Descending)
    }
}
impl<const M: bool, const E: bool, const P: bool, const R: bool, const O: bool, const F: bool>
    private::Sealed for KeywordField<M, E, P, R, O, F, true>
{
}
impl<const M: bool, const E: bool, const P: bool, const R: bool, const O: bool, const F: bool>
    TypedJsonField for KeywordField<M, E, P, R, O, F, true>
{
    fn into_index_field(self) -> IndexField {
        self.core.into_proto(
            M,
            capabilities::<E, P, R, O, F, false, false>(),
            FieldType::Keyword(KeywordIndexField {}),
        )
    }
}

/// An analyzed UTF-8 field. Analysis must be selected before full-text search.
pub struct TextField<
    const MULTI: bool = false,
    const ANALYZER: bool = false,
    const FULL_TEXT: bool = false,
> {
    core: FieldCore,
    analyzer: Option<TextAnalyzer>,
}

impl TextField {
    pub fn single(name: impl Into<String>, json_pointer: impl Into<String>) -> Self {
        Self {
            core: FieldCore::new(name, json_pointer),
            analyzer: None,
        }
    }
    pub fn multi(name: impl Into<String>, json_pointer: impl Into<String>) -> TextField<true> {
        TextField {
            core: FieldCore::new(name, json_pointer),
            analyzer: None,
        }
    }
}
impl<const M: bool> TextField<M, false, false> {
    pub fn analyzer(self, analyzer: TextAnalyzer) -> TextField<M, true, false> {
        TextField {
            core: self.core,
            analyzer: Some(analyzer),
        }
    }
}
impl<const M: bool> TextField<M, true, false> {
    pub fn full_text(self) -> TextField<M, true, true> {
        TextField {
            core: self.core,
            analyzer: self.analyzer,
        }
    }
}
impl<const M: bool> private::Sealed for TextField<M, true, true> {}
impl<const M: bool> TypedJsonField for TextField<M, true, true> {
    fn into_index_field(self) -> IndexField {
        self.core.into_proto(
            M,
            capabilities::<false, false, false, false, false, false, true>(),
            FieldType::Text(TextIndexField {
                analyzer: self.analyzer.expect("typestate guarantees an analyzer") as i32,
            }),
        )
    }
}

#[doc(hidden)]
pub mod state {
    pub struct Empty;
    pub struct NonEmpty;
}

/// Builds one complete Typed JSON index definition.
///
/// Invalid capability and cardinality combinations have no builder method:
///
/// ```compile_fail
/// use keldra::BooleanField;
/// let _ = BooleanField::single("enabled", "/enabled").range();
/// ```
///
/// Multi-valued fields cannot produce order tokens:
///
/// ```compile_fail
/// use keldra::KeywordField;
/// let _ = KeywordField::multi("tags", "/tags").order();
/// ```
///
/// A definition cannot be finished until it contains a complete field:
///
/// ```compile_fail
/// use keldra::TypedJsonIndexBuilder;
/// let _ = TypedJsonIndexBuilder::new("documents", "search").finish("create-search");
/// ```
///
/// An incomplete field cannot be added to a definition:
///
/// ```compile_fail
/// use keldra::{KeywordField, TypedJsonIndexBuilder};
/// let _ = TypedJsonIndexBuilder::new("documents", "search")
///     .field(KeywordField::single("id", "/id"));
/// ```
pub struct TypedJsonIndexBuilder<State = state::Empty> {
    bucket: String,
    name: String,
    path_prefix: String,
    content_type: String,
    fields: Vec<IndexField>,
    physical_order: Vec<IndexOrder>,
    state: PhantomData<State>,
}

impl TypedJsonIndexBuilder<state::Empty> {
    pub fn new(bucket: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            name: name.into(),
            path_prefix: String::new(),
            content_type: String::new(),
            fields: Vec::new(),
            physical_order: Vec::new(),
            state: PhantomData,
        }
    }
}

impl<State> TypedJsonIndexBuilder<State> {
    pub fn path_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.path_prefix = prefix.into();
        self
    }

    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn field<F: TypedJsonField>(mut self, field: F) -> TypedJsonIndexBuilder<state::NonEmpty> {
        self.fields.push(field.into_index_field());
        TypedJsonIndexBuilder {
            bucket: self.bucket,
            name: self.name,
            path_prefix: self.path_prefix,
            content_type: self.content_type,
            fields: self.fields,
            physical_order: self.physical_order,
            state: PhantomData,
        }
    }

    pub fn physical_order(mut self, order: impl IntoIterator<Item = IndexOrderToken>) -> Self {
        self.physical_order
            .extend(order.into_iter().map(|token| token.order));
        self
    }
}

impl TypedJsonIndexBuilder<state::NonEmpty> {
    pub fn finish(
        self,
        command_id: impl Into<String>,
    ) -> Result<CreateIndexRequest, IndexDefinitionError> {
        let command_id = command_id.into();
        validate_definition(&self, &command_id)?;
        Ok(CreateIndexRequest {
            bucket: self.bucket,
            name: self.name,
            path_prefix: self.path_prefix,
            content_type: self.content_type,
            specification: Some(IndexSpecification {
                specification: Some(Specification::TypedJson(TypedJsonIndexSpec {
                    fields: self.fields,
                    physical_order: self.physical_order,
                })),
            }),
            command_id,
        })
    }
}

fn validate_definition(
    builder: &TypedJsonIndexBuilder<state::NonEmpty>,
    command_id: &str,
) -> Result<(), IndexDefinitionError> {
    if builder.bucket.is_empty()
        || builder.bucket.len() > MAX_OBJECT_BUCKET_BYTES
        || builder.bucket.contains('/')
        || builder.bucket.chars().any(char::is_control)
    {
        return Err(IndexDefinitionError::InvalidBucket);
    }
    if builder.name.is_empty()
        || builder.name.len() > MAX_INDEX_NAME_BYTES
        || !builder
            .name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || builder.name == "."
        || builder.name == ".."
    {
        return Err(IndexDefinitionError::InvalidIndexName);
    }
    if builder.path_prefix.len() > MAX_OBJECT_PATH_BYTES || builder.path_prefix.contains('\0') {
        return Err(IndexDefinitionError::InvalidPathPrefix);
    }
    if builder.content_type.len() > MAX_CONTENT_TYPE_BYTES || builder.content_type.contains('\0') {
        return Err(IndexDefinitionError::InvalidContentType);
    }
    if command_id.is_empty() || command_id.len() > MAX_COMMAND_ID_BYTES || command_id.contains('\0')
    {
        return Err(IndexDefinitionError::InvalidCommandId);
    }

    let mut names = BTreeSet::new();
    for field in &builder.fields {
        if field.name.is_empty() || field.name.contains('\0') {
            return Err(IndexDefinitionError::InvalidFieldName(field.name.clone()));
        }
        if !field.json_pointer.is_empty()
            && (!field.json_pointer.starts_with('/') || field.json_pointer.contains('\0'))
        {
            return Err(IndexDefinitionError::InvalidJsonPointer(
                field.json_pointer.clone(),
            ));
        }
        if !names.insert(field.name.as_str()) {
            return Err(IndexDefinitionError::DuplicateFieldName(field.name.clone()));
        }
    }

    let mut ordered = BTreeSet::new();
    for order in &builder.physical_order {
        if !ordered.insert(order.field.as_str()) {
            return Err(IndexDefinitionError::DuplicatePhysicalOrder(
                order.field.clone(),
            ));
        }
        if !names.contains(order.field.as_str()) {
            return Err(IndexDefinitionError::UnknownPhysicalOrderField(
                order.field.clone(),
            ));
        }
        let field = builder
            .fields
            .iter()
            .find(|field| field.name == order.field)
            .expect("field-name set and field list agree");
        if field.cardinality != IndexFieldCardinality::Single as i32
            || !field
                .capabilities
                .contains(&(IndexFieldCapability::Order as i32))
        {
            return Err(IndexDefinitionError::UnorderablePhysicalOrderField(
                order.field.clone(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use keldra_api::v1::index_field::FieldType;
    use keldra_api::v1::index_specification::Specification;
    use keldra_api::v1::{IndexFieldCapability, IndexFieldCardinality, TextAnalyzer};

    use super::{
        BooleanField, DateField, DateFormat, FloatField, IndexDefinitionError, KeywordField,
        SignedIntegerField, TextField, TypedJsonIndexBuilder, UnsignedIntegerField,
    };

    #[test]
    fn representative_definition_has_exact_typed_wire_shape() {
        let advisory_id = KeywordField::single("advisory_id", "/id").exact();
        let ecosystem = KeywordField::single("ecosystem", "/ecosystem")
            .facet()
            .exact();
        let modified = SignedIntegerField::single("modified_at", "/modified")
            .aggregate()
            .order()
            .range()
            .exact();
        let modified_descending = modified.descending();
        let summary = TextField::single("summary", "/summary")
            .analyzer(TextAnalyzer::UnicodeAlphanumericLowercase)
            .full_text();

        let request = TypedJsonIndexBuilder::new("intelligence", "advisories")
            .path_prefix("advisories/")
            .content_type("application/json")
            .field(advisory_id)
            .field(ecosystem)
            .field(modified)
            .field(summary)
            .physical_order([modified_descending])
            .finish("create-advisories")
            .unwrap();

        let Specification::TypedJson(specification) =
            request.specification.unwrap().specification.unwrap()
        else {
            panic!("expected typed JSON specification")
        };
        assert_eq!(specification.fields.len(), 4);
        assert!(matches!(
            specification.fields[0].field_type,
            Some(FieldType::Keyword(_))
        ));
        assert_eq!(
            specification.fields[0].cardinality,
            IndexFieldCardinality::Single as i32
        );
        assert_eq!(
            specification.fields[0].capabilities,
            [IndexFieldCapability::Exact as i32]
        );
        assert_eq!(
            specification.fields[2].capabilities,
            [
                IndexFieldCapability::Exact as i32,
                IndexFieldCapability::Range as i32,
                IndexFieldCapability::Order as i32,
                IndexFieldCapability::Aggregate as i32,
            ]
        );
        assert_eq!(specification.physical_order[0].field, "modified_at");
    }

    #[test]
    fn runtime_names_and_pointers_are_checked_at_finish() {
        let duplicate = TypedJsonIndexBuilder::new("tenant", "objects")
            .field(KeywordField::single("id", "/id").exact())
            .field(KeywordField::single("id", "/other").exact())
            .finish("command")
            .unwrap_err();
        assert_eq!(
            duplicate,
            IndexDefinitionError::DuplicateFieldName("id".into())
        );

        let bad_pointer = TypedJsonIndexBuilder::new("tenant", "objects")
            .field(KeywordField::single("id", "id").exact())
            .finish("command")
            .unwrap_err();
        assert_eq!(
            bad_pointer,
            IndexDefinitionError::InvalidJsonPointer("id".into())
        );
    }

    #[test]
    fn bucket_is_checked_as_an_object_address_component() {
        for bucket in [
            "nested/bucket".to_owned(),
            "line\nbreak".to_owned(),
            "b".repeat(super::MAX_OBJECT_BUCKET_BYTES + 1),
        ] {
            let error = TypedJsonIndexBuilder::new(bucket, "objects")
                .field(KeywordField::single("id", "/id").exact())
                .finish("command")
                .unwrap_err();
            assert_eq!(error, IndexDefinitionError::InvalidBucket);
        }
    }

    #[test]
    fn order_token_must_name_a_field_in_the_same_definition() {
        let ordered = SignedIntegerField::single("created_at", "/created_at").order();
        let token = ordered.ascending();
        let error = TypedJsonIndexBuilder::new("tenant", "objects")
            .field(KeywordField::single("id", "/id").exact())
            .physical_order([token])
            .finish("command")
            .unwrap_err();
        assert_eq!(
            error,
            IndexDefinitionError::UnknownPhysicalOrderField("created_at".into())
        );

        let orderable_alias = SignedIntegerField::single("id", "/numeric_id").order();
        let error = TypedJsonIndexBuilder::new("tenant", "objects")
            .field(KeywordField::single("id", "/id").exact())
            .physical_order([orderable_alias.ascending()])
            .finish("command")
            .unwrap_err();
        assert_eq!(
            error,
            IndexDefinitionError::UnorderablePhysicalOrderField("id".into())
        );
    }

    #[test]
    fn every_concrete_builder_emits_only_its_valid_capabilities() {
        let ordered_float = FloatField::single("score", "/score")
            .range()
            .order()
            .aggregate();
        let score_order = ordered_float.descending();
        let request = TypedJsonIndexBuilder::new("tenant", "typed-fields")
            .field(BooleanField::multi("flags", "/flags").facet().exact())
            .field(
                UnsignedIntegerField::multi("sizes", "/sizes")
                    .facet()
                    .range()
                    .exact(),
            )
            .field(ordered_float)
            .physical_order([score_order])
            .finish("typed-fields")
            .unwrap();
        let Specification::TypedJson(specification) =
            request.specification.unwrap().specification.unwrap()
        else {
            panic!("expected typed JSON specification")
        };

        assert!(matches!(
            specification.fields[0].field_type,
            Some(FieldType::Boolean(_))
        ));
        assert_eq!(
            specification.fields[0].cardinality,
            IndexFieldCardinality::Multi as i32
        );
        assert!(matches!(
            specification.fields[1].field_type,
            Some(FieldType::UnsignedInteger(_))
        ));
        assert!(matches!(
            specification.fields[2].field_type,
            Some(FieldType::Float(_))
        ));
    }

    #[test]
    fn date_builder_emits_format_and_non_aggregate_capabilities() {
        let published = DateField::single("published", "/published")
            .format(DateFormat::strftime("%Y-%m-%dT%H:%M:%S%:z").unwrap())
            .exact()
            .range()
            .order()
            .facet();
        let order = published.descending();
        let request = TypedJsonIndexBuilder::new("tenant", "dated")
            .field(published)
            .physical_order([order])
            .finish("dated")
            .unwrap();
        let Specification::TypedJson(specification) =
            request.specification.unwrap().specification.unwrap()
        else {
            panic!("expected typed JSON specification")
        };
        let Some(FieldType::Date(date)) = specification.fields[0].field_type.as_ref() else {
            panic!("expected Date field")
        };
        assert_eq!(date.strftime_pattern, "%Y-%m-%dT%H:%M:%S%:z");
        assert_eq!(
            specification.fields[0].capabilities,
            [
                IndexFieldCapability::Exact as i32,
                IndexFieldCapability::Range as i32,
                IndexFieldCapability::Order as i32,
                IndexFieldCapability::Facet as i32,
            ]
        );
        assert_eq!(specification.physical_order[0].field, "published");
    }

    #[test]
    fn date_format_is_opaque_and_defaults_to_iso8601() {
        assert_eq!(
            DateFormat::strftime("").unwrap_err(),
            IndexDefinitionError::InvalidDateFormat
        );
        assert_eq!(
            DateFormat::strftime("%Y\0%m").unwrap_err(),
            IndexDefinitionError::InvalidDateFormat
        );
        assert_eq!(
            DateFormat::strftime("%Y-%B-%d").unwrap_err(),
            IndexDefinitionError::InvalidDateFormat
        );

        let request = TypedJsonIndexBuilder::new("tenant", "dated")
            .field(DateField::single("published", "/published").exact())
            .finish("dated")
            .unwrap();
        let Specification::TypedJson(specification) =
            request.specification.unwrap().specification.unwrap()
        else {
            panic!("expected typed JSON specification")
        };
        let Some(FieldType::Date(date)) = specification.fields[0].field_type.as_ref() else {
            panic!("expected Date field")
        };
        assert!(date.strftime_pattern.is_empty());
    }
}
