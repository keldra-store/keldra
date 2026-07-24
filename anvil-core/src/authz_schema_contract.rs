use crate::anvil_api::{
    AuthzAllowedSubject, AuthzNamespaceSchema, AuthzRelationSchema, AuthzSchemaMemberKind,
    AuthzSubjectSelectorKind,
};
use crate::authz_scope::{
    DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace, parse_userset_subject, split_realm_namespace,
};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

pub const PUBLIC_SUBJECT_KIND: &str = "app";
pub const PUBLIC_SUBJECT_ID: &str = "_anvil/public";
pub const MAX_AUTHZ_SCHEMA_NAMESPACES: usize = 256;
pub const MAX_AUTHZ_SCHEMA_MEMBERS_PER_NAMESPACE: usize = 256;
pub const MAX_AUTHZ_SCHEMA_RULES_PER_MEMBER: usize = 256;
pub const MAX_AUTHZ_SCHEMA_SUBJECTS_PER_MEMBER: usize = 256;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct AuthzSchemaContractError {
    message: String,
}

impl AuthzSchemaContractError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AuthzTupleShape<'a> {
    pub namespace: &'a str,
    pub object_id: &'a str,
    pub relation: &'a str,
    pub subject_kind: &'a str,
    pub subject_id: &'a str,
    pub operation: &'a str,
}

struct SchemaIndex<'a> {
    namespaces: BTreeMap<&'a str, BTreeMap<&'a str, &'a AuthzRelationSchema>>,
}

pub fn validate_schema_set(namespaces: &[AuthzNamespaceSchema]) -> Result<()> {
    if namespaces.is_empty() {
        return invalid("authorization schema must contain at least one namespace");
    }
    if namespaces.len() > MAX_AUTHZ_SCHEMA_NAMESPACES {
        return invalid(format!(
            "authorization schema must contain no more than {MAX_AUTHZ_SCHEMA_NAMESPACES} namespaces"
        ));
    }

    let mut index = SchemaIndex {
        namespaces: BTreeMap::new(),
    };
    for namespace in namespaces {
        validate_namespace_shape(namespace)?;
        let mut members = BTreeMap::new();
        for member in &namespace.relations {
            if members.insert(member.relation.as_str(), member).is_some() {
                return invalid(format!(
                    "duplicate authorization schema member {}#{}",
                    namespace.namespace, member.relation
                ));
            }
        }
        if index
            .namespaces
            .insert(namespace.namespace.as_str(), members)
            .is_some()
        {
            return invalid(format!(
                "duplicate authorization schema namespace {}",
                namespace.namespace
            ));
        }
    }

    for namespace in namespaces {
        for member in &namespace.relations {
            validate_member_targets(&index, namespace, member)?;
        }
    }
    Ok(())
}

pub fn validate_namespace_shape(namespace: &AuthzNamespaceSchema) -> Result<()> {
    validate_component(&namespace.namespace, "authorization namespace")?;
    if namespace.relations.is_empty() {
        return invalid(format!(
            "authorization namespace {} must contain at least one member",
            namespace.namespace
        ));
    }
    if namespace.relations.len() > MAX_AUTHZ_SCHEMA_MEMBERS_PER_NAMESPACE {
        return invalid(format!(
            "authorization namespace {} must contain no more than {MAX_AUTHZ_SCHEMA_MEMBERS_PER_NAMESPACE} members",
            namespace.namespace
        ));
    }

    let mut members = BTreeSet::new();
    for member in &namespace.relations {
        if member.rules.len() > MAX_AUTHZ_SCHEMA_RULES_PER_MEMBER {
            return invalid(format!(
                "authorization member {}#{} must contain no more than {MAX_AUTHZ_SCHEMA_RULES_PER_MEMBER} rules",
                namespace.namespace, member.relation
            ));
        }
        if member.allowed_subjects.len() > MAX_AUTHZ_SCHEMA_SUBJECTS_PER_MEMBER {
            return invalid(format!(
                "authorization member {}#{} must contain no more than {MAX_AUTHZ_SCHEMA_SUBJECTS_PER_MEMBER} allowed subjects",
                namespace.namespace, member.relation
            ));
        }
        validate_component(&member.relation, "authorization member")?;
        if !members.insert(member.relation.as_str()) {
            return invalid(format!(
                "duplicate authorization schema member {}#{}",
                namespace.namespace, member.relation
            ));
        }
        let member_kind = AuthzSchemaMemberKind::try_from(member.member_kind).map_err(|_| {
            contract_error(format!(
                "authorization member {}#{} has an unknown member kind",
                namespace.namespace, member.relation
            ))
        })?;
        match member_kind {
            AuthzSchemaMemberKind::DirectRelation => {
                if !member.rules.is_empty() {
                    return invalid(format!(
                        "direct relation {}#{} must not contain rewrite rules",
                        namespace.namespace, member.relation
                    ));
                }
                if member.allowed_subjects.is_empty() {
                    return invalid(format!(
                        "direct relation {}#{} must declare at least one allowed subject selector",
                        namespace.namespace, member.relation
                    ));
                }
                validate_allowed_subjects(namespace, member)?;
            }
            AuthzSchemaMemberKind::Permission => {
                if member.rules.is_empty() {
                    return invalid(format!(
                        "permission {}#{} must contain at least one rewrite rule",
                        namespace.namespace, member.relation
                    ));
                }
                if !member.allowed_subjects.is_empty() {
                    return invalid(format!(
                        "permission {}#{} must not declare allowed subjects",
                        namespace.namespace, member.relation
                    ));
                }
                validate_rules(namespace, member)?;
            }
            AuthzSchemaMemberKind::Unspecified => {
                return invalid(format!(
                    "authorization member {}#{} must declare direct relation or permission kind",
                    namespace.namespace, member.relation
                ));
            }
        }
    }
    if !namespace.schema_json.is_empty() {
        serde_json::from_str::<serde_json::Value>(&namespace.schema_json).map_err(|error| {
            contract_error(format!("authorization schema_json is invalid: {error}"))
        })?;
    }
    Ok(())
}

pub fn canonicalize_schema_set(namespaces: &mut Vec<AuthzNamespaceSchema>) {
    namespaces.sort_by(|left, right| left.namespace.cmp(&right.namespace));
    for namespace in namespaces {
        namespace
            .relations
            .sort_by(|left, right| left.relation.cmp(&right.relation));
        for member in &mut namespace.relations {
            member.rules.sort_by(|left, right| {
                (
                    &left.kind,
                    &left.relation,
                    &left.tuple_relation,
                    &left.target_relation,
                )
                    .cmp(&(
                        &right.kind,
                        &right.relation,
                        &right.tuple_relation,
                        &right.target_relation,
                    ))
            });
            member.allowed_subjects.sort_by(|left, right| {
                (left.selector_kind, &left.subject_kind, &left.subject_id).cmp(&(
                    right.selector_kind,
                    &right.subject_kind,
                    &right.subject_id,
                ))
            });
        }
    }
}

pub fn validate_tuple_batch(
    namespaces: &[AuthzNamespaceSchema],
    expected_realm_id: &str,
    tuples: &[AuthzTupleShape<'_>],
) -> Result<()> {
    validate_schema_set(namespaces)?;
    if tuples.is_empty() {
        return invalid("authorization tuple batch must not be empty");
    }
    let index = build_index(namespaces);
    for tuple in tuples {
        validate_tuple(&index, expected_realm_id, tuple)?;
    }
    Ok(())
}

pub fn is_direct_relation(member: &AuthzRelationSchema) -> bool {
    member.member_kind == AuthzSchemaMemberKind::DirectRelation as i32
}

fn validate_allowed_subjects(
    namespace: &AuthzNamespaceSchema,
    member: &AuthzRelationSchema,
) -> Result<()> {
    let mut selectors = BTreeSet::new();
    for selector in &member.allowed_subjects {
        let selector_kind = selector_kind(selector, namespace, member)?;
        match selector_kind {
            AuthzSubjectSelectorKind::AnyCanonicalId | AuthzSubjectSelectorKind::SameResourceId => {
                validate_component(&selector.subject_kind, "allowed subject kind")?;
                require_empty(
                    &selector.subject_id,
                    "allowed subject id",
                    namespace,
                    member,
                )?;
                if selector_kind == AuthzSubjectSelectorKind::SameResourceId
                    && selector.subject_kind == "userset"
                {
                    return invalid(format!(
                        "same-resource selector on {}#{} cannot target userset subjects",
                        namespace.namespace, member.relation
                    ));
                }
            }
            AuthzSubjectSelectorKind::Exact => {
                validate_component(&selector.subject_kind, "allowed subject kind")?;
                validate_id(&selector.subject_id, "allowed exact subject id")?;
                if is_public_subject(&selector.subject_kind, &selector.subject_id) {
                    return invalid(format!(
                        "reserved public subject on {}#{} must use the public selector",
                        namespace.namespace, member.relation
                    ));
                }
                if selector.subject_kind == "userset" {
                    parse_canonical_userset(&selector.subject_id)?;
                }
            }
            AuthzSubjectSelectorKind::Public => {
                require_empty(
                    &selector.subject_kind,
                    "public selector subject kind",
                    namespace,
                    member,
                )?;
                require_empty(
                    &selector.subject_id,
                    "public selector subject id",
                    namespace,
                    member,
                )?;
            }
            AuthzSubjectSelectorKind::Unspecified => unreachable!(),
        }
        if !selectors.insert((
            selector.selector_kind,
            selector.subject_kind.as_str(),
            selector.subject_id.as_str(),
        )) {
            return invalid(format!(
                "duplicate allowed subject selector on {}#{}",
                namespace.namespace, member.relation
            ));
        }
    }
    Ok(())
}

fn validate_rules(namespace: &AuthzNamespaceSchema, member: &AuthzRelationSchema) -> Result<()> {
    let mut rules = BTreeSet::new();
    for rule in &member.rules {
        match rule.kind.as_str() {
            "inherit" => {
                validate_component(&rule.relation, "inherited member")?;
                require_rule_empty(&rule.tuple_relation, "tuple_relation", namespace, member)?;
                require_rule_empty(&rule.target_relation, "target_relation", namespace, member)?;
            }
            "computed" | "tuple_to_userset" => {
                require_rule_empty(&rule.relation, "relation", namespace, member)?;
                validate_component(&rule.tuple_relation, "tuple relation")?;
                validate_component(&rule.target_relation, "target relation")?;
            }
            _ => {
                return invalid(format!(
                    "permission {}#{} has unknown rewrite rule kind {}",
                    namespace.namespace, member.relation, rule.kind
                ));
            }
        }
        if !rules.insert((
            rule.kind.as_str(),
            rule.relation.as_str(),
            rule.tuple_relation.as_str(),
            rule.target_relation.as_str(),
        )) {
            return invalid(format!(
                "duplicate rewrite rule on {}#{}",
                namespace.namespace, member.relation
            ));
        }
    }
    Ok(())
}

fn validate_member_targets(
    index: &SchemaIndex<'_>,
    namespace: &AuthzNamespaceSchema,
    member: &AuthzRelationSchema,
) -> Result<()> {
    if is_direct_relation(member) {
        for selector in &member.allowed_subjects {
            if selector.subject_kind == "userset"
                && selector.selector_kind == AuthzSubjectSelectorKind::Exact as i32
            {
                let userset = parse_canonical_userset(&selector.subject_id)?;
                require_member(
                    index,
                    userset.namespace,
                    userset.relation,
                    "userset selector",
                )?;
            }
        }
        return Ok(());
    }

    for rule in &member.rules {
        match rule.kind.as_str() {
            "inherit" => {
                require_member(
                    index,
                    &namespace.namespace,
                    &rule.relation,
                    "inherited member",
                )?;
            }
            "computed" | "tuple_to_userset" => {
                let tuple_member = require_member(
                    index,
                    &namespace.namespace,
                    &rule.tuple_relation,
                    "tuple-to-userset source",
                )?;
                if !is_direct_relation(tuple_member) {
                    return invalid(format!(
                        "tuple-to-userset source {}#{} must be a direct relation",
                        namespace.namespace, rule.tuple_relation
                    ));
                }
                for selector in &tuple_member.allowed_subjects {
                    let (target_namespace, exact_userset_relation) =
                        selector_target_namespace(selector, namespace, member)?;
                    if let Some(exact_relation) = exact_userset_relation
                        && exact_relation != rule.target_relation
                    {
                        return invalid(format!(
                            "tuple-to-userset target {}#{} conflicts with exact userset relation {}",
                            target_namespace, rule.target_relation, exact_relation
                        ));
                    }
                    require_member(
                        index,
                        target_namespace,
                        &rule.target_relation,
                        "tuple-to-userset target",
                    )?;
                }
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

fn validate_tuple(
    index: &SchemaIndex<'_>,
    expected_realm_id: &str,
    tuple: &AuthzTupleShape<'_>,
) -> Result<()> {
    let (realm_id, local_namespace) = tuple_namespace_parts(tuple.namespace)?;
    if realm_id != expected_realm_id {
        return invalid("authorization tuple batch must target one bound realm");
    }
    validate_id(tuple.object_id, "authorization resource id")?;
    validate_component(tuple.relation, "authorization relation")?;
    validate_component(tuple.subject_kind, "authorization subject kind")?;
    validate_id(tuple.subject_id, "authorization subject id")?;
    if !matches!(tuple.operation, "add" | "remove") {
        return invalid("authorization tuple operation must be add or remove");
    }

    let member = require_member(index, local_namespace, tuple.relation, "tuple relation")?;
    if !is_direct_relation(member) {
        return invalid(format!(
            "authorization permission {local_namespace}#{} cannot be written as a tuple",
            tuple.relation
        ));
    }

    let comparable_subject_id = if tuple.subject_kind == "userset" {
        let userset = local_userset(expected_realm_id, tuple.subject_id)?;
        require_member(
            index,
            userset.namespace,
            userset.relation,
            "userset subject",
        )?;
        format!(
            "{}/{}#{}",
            userset.namespace, userset.object_id, userset.relation
        )
    } else {
        tuple.subject_id.to_string()
    };
    let allowed = member
        .allowed_subjects
        .iter()
        .any(|selector| selector_matches(selector, tuple, comparable_subject_id.as_str()));
    if !allowed {
        return invalid(format!(
            "subject {}:{} is not allowed on direct relation {local_namespace}#{}",
            tuple.subject_kind, tuple.subject_id, tuple.relation
        ));
    }
    Ok(())
}

fn selector_matches(
    selector: &AuthzAllowedSubject,
    tuple: &AuthzTupleShape<'_>,
    comparable_subject_id: &str,
) -> bool {
    match AuthzSubjectSelectorKind::try_from(selector.selector_kind) {
        Ok(AuthzSubjectSelectorKind::AnyCanonicalId) => {
            selector.subject_kind == tuple.subject_kind
                && !is_public_subject(tuple.subject_kind, tuple.subject_id)
        }
        Ok(AuthzSubjectSelectorKind::Exact) => {
            selector.subject_kind == tuple.subject_kind
                && selector.subject_id == comparable_subject_id
        }
        Ok(AuthzSubjectSelectorKind::SameResourceId) => {
            selector.subject_kind == tuple.subject_kind && tuple.subject_id == tuple.object_id
        }
        Ok(AuthzSubjectSelectorKind::Public) => {
            is_public_subject(tuple.subject_kind, tuple.subject_id)
        }
        _ => false,
    }
}

fn selector_target_namespace<'a>(
    selector: &'a AuthzAllowedSubject,
    namespace: &AuthzNamespaceSchema,
    member: &AuthzRelationSchema,
) -> Result<(&'a str, Option<&'a str>)> {
    let selector_kind = selector_kind(selector, namespace, member)?;
    if selector_kind == AuthzSubjectSelectorKind::Public {
        return invalid(format!(
            "tuple-to-userset source {} cannot allow the public subject",
            member.relation
        ));
    }
    if selector.subject_kind != "userset" {
        return Ok((&selector.subject_kind, None));
    }
    if selector_kind != AuthzSubjectSelectorKind::Exact {
        return invalid(format!(
            "tuple-to-userset source {} has an unresolved userset selector",
            member.relation
        ));
    }
    let userset = parse_canonical_userset(&selector.subject_id)?;
    Ok((userset.namespace, Some(userset.relation)))
}

fn selector_kind(
    selector: &AuthzAllowedSubject,
    namespace: &AuthzNamespaceSchema,
    member: &AuthzRelationSchema,
) -> Result<AuthzSubjectSelectorKind> {
    let kind = AuthzSubjectSelectorKind::try_from(selector.selector_kind).map_err(|_| {
        contract_error(format!(
            "direct relation {}#{} has an unknown allowed subject selector",
            namespace.namespace, member.relation
        ))
    })?;
    if kind == AuthzSubjectSelectorKind::Unspecified {
        return invalid(format!(
            "direct relation {}#{} has an unspecified allowed subject selector",
            namespace.namespace, member.relation
        ));
    }
    Ok(kind)
}

fn build_index(namespaces: &[AuthzNamespaceSchema]) -> SchemaIndex<'_> {
    SchemaIndex {
        namespaces: namespaces
            .iter()
            .map(|namespace| {
                (
                    namespace.namespace.as_str(),
                    namespace
                        .relations
                        .iter()
                        .map(|member| (member.relation.as_str(), member))
                        .collect(),
                )
            })
            .collect(),
    }
}

fn require_member<'a>(
    index: &'a SchemaIndex<'a>,
    namespace: &str,
    member: &str,
    label: &str,
) -> Result<&'a AuthzRelationSchema> {
    index
        .namespaces
        .get(namespace)
        .and_then(|members| members.get(member))
        .copied()
        .ok_or_else(|| {
            contract_error(format!(
                "{label} {namespace}#{member} is not declared by the authorization schema"
            ))
        })
}

fn tuple_namespace_parts(namespace: &str) -> Result<(String, &str)> {
    if let Some((realm_id, local_namespace)) = split_realm_namespace(namespace) {
        if encode_realm_namespace(&realm_id, local_namespace) != namespace {
            return invalid("authorization tuple namespace is not canonically realm encoded");
        }
        validate_component(local_namespace, "authorization namespace")?;
        return Ok((realm_id, local_namespace));
    }
    validate_component(namespace, "authorization namespace")?;
    Ok((DEFAULT_AUTHZ_REALM_ID.to_string(), namespace))
}

fn local_userset<'a>(realm_id: &str, subject_id: &'a str) -> Result<LocalUserset<'a>> {
    let userset = parse_canonical_userset(subject_id)?;
    let namespace = if let Some((subject_realm_id, local_namespace)) =
        split_realm_namespace(userset.namespace)
    {
        if subject_realm_id != realm_id
            || encode_realm_namespace(&subject_realm_id, local_namespace) != userset.namespace
        {
            return invalid("userset subject must target the tuple's bound realm");
        }
        local_namespace
    } else {
        userset.namespace
    };
    Ok(LocalUserset {
        namespace,
        object_id: userset.object_id,
        relation: userset.relation,
    })
}

struct LocalUserset<'a> {
    namespace: &'a str,
    object_id: &'a str,
    relation: &'a str,
}

fn parse_canonical_userset(value: &str) -> Result<LocalUserset<'_>> {
    let userset = parse_userset_subject(value)
        .ok_or_else(|| contract_error("userset subject id is not canonical"))?;
    validate_component(userset.namespace, "userset namespace")?;
    validate_id(userset.object_id, "userset object id")?;
    validate_component(userset.relation, "userset relation")?;
    Ok(LocalUserset {
        namespace: userset.namespace,
        object_id: userset.object_id,
        relation: userset.relation,
    })
}

fn validate_component(value: &str, label: &str) -> Result<()> {
    validate_id(value, label)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains(':')
        || value.contains('#')
    {
        return invalid(format!("{label} must be a safe component"));
    }
    Ok(())
}

fn validate_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        return invalid(format!("{label} must not be empty"));
    }
    if value.chars().any(char::is_control) {
        return invalid(format!("{label} is not canonical"));
    }
    Ok(())
}

fn require_empty(
    value: &str,
    label: &str,
    namespace: &AuthzNamespaceSchema,
    member: &AuthzRelationSchema,
) -> Result<()> {
    if value.is_empty() {
        Ok(())
    } else {
        invalid(format!(
            "{label} must be empty on {}#{}",
            namespace.namespace, member.relation
        ))
    }
}

fn require_rule_empty(
    value: &str,
    label: &str,
    namespace: &AuthzNamespaceSchema,
    member: &AuthzRelationSchema,
) -> Result<()> {
    if value.is_empty() {
        Ok(())
    } else {
        invalid(format!(
            "{label} must be empty for this rule on {}#{}",
            namespace.namespace, member.relation
        ))
    }
}

fn is_public_subject(subject_kind: &str, subject_id: &str) -> bool {
    subject_kind == PUBLIC_SUBJECT_KIND && subject_id == PUBLIC_SUBJECT_ID
}

fn contract_error(message: impl Into<String>) -> anyhow::Error {
    AuthzSchemaContractError::new(message).into()
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(contract_error(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anvil_api::{AuthzRelationRule, AuthzSchemaMemberKind};

    fn selector(
        kind: AuthzSubjectSelectorKind,
        subject_kind: &str,
        subject_id: &str,
    ) -> AuthzAllowedSubject {
        AuthzAllowedSubject {
            selector_kind: kind as i32,
            subject_kind: subject_kind.to_string(),
            subject_id: subject_id.to_string(),
        }
    }

    fn direct(name: &str, allowed_subjects: Vec<AuthzAllowedSubject>) -> AuthzRelationSchema {
        AuthzRelationSchema {
            relation: name.to_string(),
            rules: Vec::new(),
            member_kind: AuthzSchemaMemberKind::DirectRelation as i32,
            allowed_subjects,
        }
    }

    fn permission(name: &str, rules: Vec<AuthzRelationRule>) -> AuthzRelationSchema {
        AuthzRelationSchema {
            relation: name.to_string(),
            rules,
            member_kind: AuthzSchemaMemberKind::Permission as i32,
            allowed_subjects: Vec::new(),
        }
    }

    fn namespace(name: &str, relations: Vec<AuthzRelationSchema>) -> AuthzNamespaceSchema {
        AuthzNamespaceSchema {
            namespace: name.to_string(),
            relations,
            schema_json: String::new(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        }
    }

    fn inherit(relation: &str) -> AuthzRelationRule {
        AuthzRelationRule {
            kind: "inherit".to_string(),
            relation: relation.to_string(),
            tuple_relation: String::new(),
            target_relation: String::new(),
        }
    }

    fn computed(tuple_relation: &str, target_relation: &str) -> AuthzRelationRule {
        AuthzRelationRule {
            kind: "tuple_to_userset".to_string(),
            relation: String::new(),
            tuple_relation: tuple_relation.to_string(),
            target_relation: target_relation.to_string(),
        }
    }

    fn valid_schema() -> Vec<AuthzNamespaceSchema> {
        vec![
            namespace(
                "document",
                vec![
                    direct(
                        "owner",
                        vec![selector(
                            AuthzSubjectSelectorKind::AnyCanonicalId,
                            "user",
                            "",
                        )],
                    ),
                    direct(
                        "service",
                        vec![selector(
                            AuthzSubjectSelectorKind::Exact,
                            "service",
                            "indexer",
                        )],
                    ),
                    direct(
                        "self",
                        vec![selector(
                            AuthzSubjectSelectorKind::SameResourceId,
                            "account",
                            "",
                        )],
                    ),
                    direct(
                        "public_reader",
                        vec![selector(AuthzSubjectSelectorKind::Public, "", "")],
                    ),
                    direct(
                        "parent",
                        vec![selector(
                            AuthzSubjectSelectorKind::AnyCanonicalId,
                            "folder",
                            "",
                        )],
                    ),
                    permission(
                        "viewer",
                        vec![inherit("owner"), computed("parent", "viewer")],
                    ),
                ],
            ),
            namespace(
                "folder",
                vec![direct(
                    "viewer",
                    vec![selector(
                        AuthzSubjectSelectorKind::AnyCanonicalId,
                        "user",
                        "",
                    )],
                )],
            ),
        ]
    }

    fn tuple<'a>(
        relation: &'a str,
        object_id: &'a str,
        subject_kind: &'a str,
        subject_id: &'a str,
    ) -> AuthzTupleShape<'a> {
        AuthzTupleShape {
            namespace: "realm__workspace__document",
            object_id,
            relation,
            subject_kind,
            subject_id,
            operation: "add",
        }
    }

    #[test]
    fn accepts_typed_members_and_all_subject_selector_forms() {
        let schema = valid_schema();
        validate_schema_set(&schema).unwrap();
        validate_tuple_batch(
            &schema,
            "workspace",
            &[
                tuple("owner", "doc-1", "user", "alice"),
                tuple("service", "doc-1", "service", "indexer"),
                tuple("self", "account-1", "account", "account-1"),
                tuple(
                    "public_reader",
                    "doc-1",
                    PUBLIC_SUBJECT_KIND,
                    PUBLIC_SUBJECT_ID,
                ),
                tuple("parent", "doc-1", "folder", "folder-1"),
            ],
        )
        .unwrap();
    }

    #[test]
    fn rejects_non_writable_members_and_selector_mismatches() {
        let schema = valid_schema();
        for (tuple, message) in [
            (
                tuple("viewer", "doc-1", "user", "alice"),
                "cannot be written",
            ),
            (
                tuple("missing", "doc-1", "user", "alice"),
                "is not declared",
            ),
            (
                tuple("owner", "doc-1", "service", "indexer"),
                "is not allowed",
            ),
            (
                tuple("service", "doc-1", "service", "other"),
                "is not allowed",
            ),
            (
                tuple("self", "account-1", "account", "account-2"),
                "is not allowed",
            ),
            (
                tuple("public_reader", "doc-1", PUBLIC_SUBJECT_KIND, "public"),
                "is not allowed",
            ),
        ] {
            let error = validate_tuple_batch(&schema, "workspace", &[tuple]).unwrap_err();
            assert!(
                error.to_string().contains(message),
                "unexpected error: {error:#}"
            );
        }
    }

    #[test]
    fn rejects_untyped_duplicate_and_structurally_mixed_members() {
        let mut cases = Vec::new();
        cases.push(namespace(
            "document",
            vec![AuthzRelationSchema {
                relation: "viewer".to_string(),
                ..Default::default()
            }],
        ));
        cases.push(namespace("document", vec![direct("viewer", Vec::new())]));
        let mut direct_with_rules = direct(
            "viewer",
            vec![selector(
                AuthzSubjectSelectorKind::AnyCanonicalId,
                "user",
                "",
            )],
        );
        direct_with_rules.rules.push(inherit("owner"));
        cases.push(namespace("document", vec![direct_with_rules]));
        cases.push(namespace(
            "document",
            vec![permission("viewer", Vec::new())],
        ));
        let mut permission_with_selector = permission("viewer", vec![inherit("owner")]);
        permission_with_selector.allowed_subjects.push(selector(
            AuthzSubjectSelectorKind::AnyCanonicalId,
            "user",
            "",
        ));
        cases.push(namespace("document", vec![permission_with_selector]));
        let duplicated = selector(AuthzSubjectSelectorKind::AnyCanonicalId, "user", "");
        cases.push(namespace(
            "document",
            vec![direct("viewer", vec![duplicated.clone(), duplicated])],
        ));
        cases.push(namespace(
            "document",
            vec![direct(
                "viewer",
                vec![AuthzAllowedSubject {
                    selector_kind: 999,
                    subject_kind: "user".to_string(),
                    subject_id: String::new(),
                }],
            )],
        ));

        for schema in cases {
            assert!(validate_schema_set(&[schema]).is_err());
        }
    }

    #[test]
    fn rejects_unresolved_or_incompatible_tuple_to_userset_targets() {
        let unresolved = vec![namespace(
            "document",
            vec![
                direct(
                    "parent",
                    vec![selector(
                        AuthzSubjectSelectorKind::AnyCanonicalId,
                        "missing_kind",
                        "",
                    )],
                ),
                permission("viewer", vec![computed("parent", "viewer")]),
            ],
        )];
        let error = validate_schema_set(&unresolved).unwrap_err();
        assert!(error.to_string().contains("tuple-to-userset target"));

        let incompatible = vec![namespace(
            "document",
            vec![
                permission("parent", vec![inherit("viewer")]),
                permission("viewer", vec![computed("parent", "viewer")]),
            ],
        )];
        let error = validate_schema_set(&incompatible).unwrap_err();
        assert!(error.to_string().contains("must be a direct relation"));
    }

    #[test]
    fn rejects_schema_collections_above_contract_limits() {
        let namespace = namespace(
            "document",
            vec![direct(
                "viewer",
                vec![selector(
                    AuthzSubjectSelectorKind::AnyCanonicalId,
                    "user",
                    "",
                )],
            )],
        );
        assert!(
            validate_schema_set(&vec![namespace; MAX_AUTHZ_SCHEMA_NAMESPACES + 1])
                .unwrap_err()
                .to_string()
                .contains("no more than")
        );
    }
}
