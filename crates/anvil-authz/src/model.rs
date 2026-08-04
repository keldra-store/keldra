use std::{error::Error, fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

pub const DEFAULT_REALM_ID: &str = "default";
pub const SYSTEM_REALM_ID: &str = "_anvil/system";
pub const PUBLIC_SUBJECT_NAMESPACE: &str = "app";
pub const PUBLIC_SUBJECT_ID: &str = "_anvil/public";
/// Reserved non-credentialed application identity used at the public service
/// boundary when a request omits authentication.
pub const ANONYMOUS_SUBJECT_ID: &str = "_anvil/anonymous";

pub const MAX_REALM_ID_BYTES: usize = 256;
pub const MAX_NAMESPACE_BYTES: usize = 256;
pub const MAX_RELATION_BYTES: usize = 256;
pub const MAX_OPAQUE_ID_BYTES: usize = 4096;
pub const MAX_PATH_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizationError {
    InvalidRealm(String),
    InvalidLimits(String),
    InvalidSchema(String),
    InvalidTuple { index: usize, reason: String },
    InvalidCheck(String),
    EvaluationLimit { limit: &'static str, maximum: usize },
}

impl fmt::Display for AuthorizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRealm(reason) => {
                write!(formatter, "invalid authorization realm: {reason}")
            }
            Self::InvalidLimits(reason) => {
                write!(formatter, "invalid authorization limits: {reason}")
            }
            Self::InvalidSchema(reason) => {
                write!(formatter, "invalid authorization schema: {reason}")
            }
            Self::InvalidTuple { index, reason } => {
                write!(formatter, "invalid authorization tuple {index}: {reason}")
            }
            Self::InvalidCheck(reason) => {
                write!(formatter, "invalid authorization check: {reason}")
            }
            Self::EvaluationLimit { limit, maximum } => write!(
                formatter,
                "authorization evaluation exceeded {limit} limit {maximum}"
            ),
        }
    }
}

impl Error for AuthorizationError {}

/// The structural boundary for one independent authorization graph.
///
/// `default` and `_anvil/system` are valid persisted realm IDs. Callers that
/// create third-party realms should use [`RealmId::custom`], which rejects only
/// the protected system spelling while imposing no legacy aliases.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RealmId(String);

impl RealmId {
    pub fn parse(value: impl Into<String>) -> crate::Result<Self> {
        let value = value.into();
        validate_realm_id(&value).map_err(AuthorizationError::InvalidRealm)?;
        Ok(Self(value))
    }

    pub fn custom(value: impl Into<String>) -> crate::Result<Self> {
        let value = value.into();
        if value == SYSTEM_REALM_ID {
            return Err(AuthorizationError::InvalidRealm(format!(
                "`{value}` is reserved"
            )));
        }
        Self::parse(value)
    }

    pub fn default_realm() -> Self {
        Self(DEFAULT_REALM_ID.to_owned())
    }

    pub fn system() -> Self {
        Self(SYSTEM_REALM_ID.to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_system(&self) -> bool {
        self.0 == SYSTEM_REALM_ID
    }
}

impl Default for RealmId {
    fn default() -> Self {
        Self::default_realm()
    }
}

impl fmt::Display for RealmId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for RealmId {
    type Err = AuthorizationError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl Serialize for RealmId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RealmId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationLimits {
    pub max_namespaces: usize,
    pub max_relations_per_namespace: usize,
    pub max_items_per_relation: usize,
    pub max_tuples: usize,
    pub max_depth: usize,
    pub max_steps: usize,
}

impl Default for AuthorizationLimits {
    fn default() -> Self {
        Self {
            max_namespaces: 256,
            max_relations_per_namespace: 256,
            max_items_per_relation: 256,
            max_tuples: 65_536,
            max_depth: 32,
            max_steps: 16_384,
        }
    }
}

impl AuthorizationLimits {
    pub(crate) fn validate(self) -> crate::Result<()> {
        for (name, value) in [
            ("max_namespaces", self.max_namespaces),
            (
                "max_relations_per_namespace",
                self.max_relations_per_namespace,
            ),
            ("max_items_per_relation", self.max_items_per_relation),
            ("max_tuples", self.max_tuples),
            ("max_depth", self.max_depth),
            ("max_steps", self.max_steps),
        ] {
            if value == 0 {
                return Err(AuthorizationError::InvalidLimits(format!(
                    "{name} must be nonzero"
                )));
            }
        }
        Ok(())
    }
}

/// An exact Anvil object path. No ancestor or prefix grant is implied.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExactPath {
    pub tenant: String,
    pub bucket: String,
    pub path: String,
}

impl ExactPath {
    pub fn new(
        tenant: impl Into<String>,
        bucket: impl Into<String>,
        path: impl Into<String>,
    ) -> crate::Result<Self> {
        let value = Self {
            tenant: tenant.into(),
            bucket: bucket.into(),
            path: path.into(),
        };
        validate_exact_path(&value).map_err(AuthorizationError::InvalidCheck)?;
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ObjectId {
    Opaque(String),
    ExactPath(ExactPath),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectRef {
    pub namespace: String,
    pub id: ObjectId,
}

impl ObjectRef {
    pub fn opaque(namespace: impl Into<String>, id: impl Into<String>) -> crate::Result<Self> {
        let value = Self {
            namespace: namespace.into(),
            id: ObjectId::Opaque(id.into()),
        };
        validate_object(&value).map_err(AuthorizationError::InvalidCheck)?;
        Ok(value)
    }

    pub fn exact_path(namespace: impl Into<String>, path: ExactPath) -> crate::Result<Self> {
        let value = Self {
            namespace: namespace.into(),
            id: ObjectId::ExactPath(path),
        };
        validate_object(&value).map_err(AuthorizationError::InvalidCheck)?;
        Ok(value)
    }

    pub fn public() -> Self {
        Self {
            namespace: PUBLIC_SUBJECT_NAMESPACE.to_owned(),
            id: ObjectId::Opaque(PUBLIC_SUBJECT_ID.to_owned()),
        }
    }

    pub fn anonymous() -> Self {
        Self {
            namespace: PUBLIC_SUBJECT_NAMESPACE.to_owned(),
            id: ObjectId::Opaque(ANONYMOUS_SUBJECT_ID.to_owned()),
        }
    }

    pub fn is_anonymous(&self) -> bool {
        self.namespace == PUBLIC_SUBJECT_NAMESPACE
            && self.id == ObjectId::Opaque(ANONYMOUS_SUBJECT_ID.to_owned())
    }

    pub fn is_public(&self) -> bool {
        self.namespace == PUBLIC_SUBJECT_NAMESPACE
            && self.id == ObjectId::Opaque(PUBLIC_SUBJECT_ID.to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UsersetRef {
    pub object: ObjectRef,
    pub relation: String,
}

impl UsersetRef {
    pub fn new(object: ObjectRef, relation: impl Into<String>) -> crate::Result<Self> {
        let value = Self {
            object,
            relation: relation.into(),
        };
        validate_userset(&value).map_err(AuthorizationError::InvalidCheck)?;
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TupleSubject {
    Object(ObjectRef),
    Userset(UsersetRef),
}

impl From<ObjectRef> for TupleSubject {
    fn from(value: ObjectRef) -> Self {
        Self::Object(value)
    }
}

impl From<UsersetRef> for TupleSubject {
    fn from(value: UsersetRef) -> Self {
        Self::Userset(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Tuple {
    pub object: ObjectRef,
    pub relation: String,
    pub subject: TupleSubject,
}

impl Tuple {
    pub fn new(object: ObjectRef, relation: impl Into<String>, subject: ObjectRef) -> Self {
        Self {
            object,
            relation: relation.into(),
            subject: TupleSubject::Object(subject),
        }
    }

    pub fn userset(object: ObjectRef, relation: impl Into<String>, subject: UsersetRef) -> Self {
        Self {
            object,
            relation: relation.into(),
            subject: TupleSubject::Userset(subject),
        }
    }
}

/// A schema selector accepted by a writable direct relation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AllowedSubject {
    /// Any canonical non-public object in the named namespace.
    AnyObject { namespace: String },
    /// Any userset with this object namespace and relation.
    AnyUserset { namespace: String, relation: String },
    /// One exact typed object or userset.
    Exact { subject: TupleSubject },
    /// An object in this namespace whose ID equals the tuple resource ID.
    SameResourceId { namespace: String },
    /// Only Anvil's reserved `app:_anvil/public` principal.
    Public,
}

impl AllowedSubject {
    pub fn any_object(namespace: impl Into<String>) -> Self {
        Self::AnyObject {
            namespace: namespace.into(),
        }
    }

    pub fn any_userset(namespace: impl Into<String>, relation: impl Into<String>) -> Self {
        Self::AnyUserset {
            namespace: namespace.into(),
            relation: relation.into(),
        }
    }

    pub fn exact(subject: impl Into<TupleSubject>) -> Self {
        Self::Exact {
            subject: subject.into(),
        }
    }

    pub fn same_resource_id(namespace: impl Into<String>) -> Self {
        Self::SameResourceId {
            namespace: namespace.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RewriteRule {
    /// Union in another relation or permission on the same object.
    Inherit { relation: String },
    /// Follow objects named by a direct relation, then evaluate their relation.
    TupleToUserset {
        tuple_relation: String,
        target_relation: String,
    },
}

impl RewriteRule {
    /// The old `computed` and `tuple_to_userset` spellings had identical
    /// evaluation semantics. The 0.5 model stores one canonical rule.
    pub fn computed(tuple_relation: impl Into<String>, target_relation: impl Into<String>) -> Self {
        Self::TupleToUserset {
            tuple_relation: tuple_relation.into(),
            target_relation: target_relation.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationKind {
    Direct {
        allowed_subjects: Vec<AllowedSubject>,
    },
    Permission {
        rules: Vec<RewriteRule>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationDefinition {
    pub name: String,
    pub kind: RelationKind,
}

impl RelationDefinition {
    pub fn direct(
        name: impl Into<String>,
        allowed_subjects: impl IntoIterator<Item = AllowedSubject>,
    ) -> Self {
        Self {
            name: name.into(),
            kind: RelationKind::Direct {
                allowed_subjects: allowed_subjects.into_iter().collect(),
            },
        }
    }

    pub fn permission(
        name: impl Into<String>,
        rules: impl IntoIterator<Item = RewriteRule>,
    ) -> Self {
        Self {
            name: name.into(),
            kind: RelationKind::Permission {
                rules: rules.into_iter().collect(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceDefinition {
    pub name: String,
    pub relations: Vec<RelationDefinition>,
}

impl NamespaceDefinition {
    pub fn new(
        name: impl Into<String>,
        relations: impl IntoIterator<Item = RelationDefinition>,
    ) -> Self {
        Self {
            name: name.into(),
            relations: relations.into_iter().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub namespaces: Vec<NamespaceDefinition>,
}

impl Schema {
    pub fn new(namespaces: impl IntoIterator<Item = NamespaceDefinition>) -> Self {
        Self {
            namespaces: namespaces.into_iter().collect(),
        }
    }

    pub fn validate(&self, limits: AuthorizationLimits) -> crate::Result<()> {
        crate::schema::CompiledSchema::compile(self, limits).map(|_| ())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationCheck {
    pub subject: ObjectRef,
    pub object: ObjectRef,
    pub relation: String,
}

impl AuthorizationCheck {
    pub fn new(subject: ObjectRef, object: ObjectRef, relation: impl Into<String>) -> Self {
        Self {
            subject,
            object,
            relation: relation.into(),
        }
    }
}

pub(crate) fn validate_object(object: &ObjectRef) -> std::result::Result<(), String> {
    validate_namespace(&object.namespace)?;
    match &object.id {
        ObjectId::Opaque(id) => validate_opaque_id(id),
        ObjectId::ExactPath(path) => validate_exact_path(path),
    }
}

pub(crate) fn validate_tuple_subject(subject: &TupleSubject) -> std::result::Result<(), String> {
    match subject {
        TupleSubject::Object(object) => validate_object(object),
        TupleSubject::Userset(userset) => validate_userset(userset),
    }
}

pub(crate) fn validate_userset(userset: &UsersetRef) -> std::result::Result<(), String> {
    validate_object(&userset.object)?;
    validate_relation(&userset.relation)
}

pub(crate) fn validate_namespace(value: &str) -> std::result::Result<(), String> {
    validate_bounded(value, "namespace", MAX_NAMESPACE_BYTES)?;
    if matches!(value, "." | "..")
        || value
            .chars()
            .any(|character| matches!(character, '/' | ':' | '#'))
    {
        return Err("namespace must be one safe component".into());
    }
    Ok(())
}

pub(crate) fn validate_relation(value: &str) -> std::result::Result<(), String> {
    validate_bounded(value, "relation", MAX_RELATION_BYTES)?;
    if matches!(value, "." | "..")
        || value
            .chars()
            .any(|character| matches!(character, '/' | '#'))
    {
        return Err("relation must be one safe component".into());
    }
    Ok(())
}

fn validate_realm_id(value: &str) -> std::result::Result<(), String> {
    if value == SYSTEM_REALM_ID {
        return Ok(());
    }
    validate_bounded(value, "realm id", MAX_REALM_ID_BYTES)?;
    if matches!(value, "." | "..")
        || value
            .chars()
            .any(|character| matches!(character, '/' | ':' | '#'))
    {
        return Err("realm id must be one canonical non-reserved component".into());
    }
    Ok(())
}

fn validate_opaque_id(value: &str) -> std::result::Result<(), String> {
    validate_bounded(value, "object id", MAX_OPAQUE_ID_BYTES)
}

fn validate_exact_path(value: &ExactPath) -> std::result::Result<(), String> {
    validate_bounded(&value.tenant, "tenant", MAX_NAMESPACE_BYTES)?;
    validate_bounded(&value.bucket, "bucket", MAX_NAMESPACE_BYTES)?;
    if value.tenant.contains('/') || value.bucket.contains('/') {
        return Err("tenant and bucket must not contain `/`".into());
    }
    validate_bounded(&value.path, "path", MAX_PATH_BYTES)?;
    if value.path.starts_with('/')
        || value.path.ends_with('/')
        || value
            .path
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
    {
        return Err("path must contain canonical non-empty relative segments".into());
    }
    Ok(())
}

fn validate_bounded(value: &str, label: &str, maximum: usize) -> std::result::Result<(), String> {
    if value.is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    if value.len() > maximum {
        return Err(format!("{label} exceeds {maximum} bytes"));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{label} must not contain control characters"));
    }
    Ok(())
}
