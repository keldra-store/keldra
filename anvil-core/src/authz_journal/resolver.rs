use super::TupleViewKey;
use crate::anvil_api::AuthzRelationRule;
use crate::authz_realm_schema;
use crate::authz_scope::{
    DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace, parse_userset_subject, split_realm_namespace,
};
use crate::persistence::AuthzTupleRecord;
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SubjectRef {
    pub(crate) kind: String,
    pub(crate) id: String,
    pub(crate) caveat_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct UsersetRef {
    pub(crate) namespace: String,
    pub(crate) object_id: String,
    pub(crate) relation: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SchemaRuleIndex {
    members_by_userset: BTreeMap<UsersetRuleKey, SchemaMember>,
    schema_bound_namespaces: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct MaterializedSchemaRelation {
    pub(crate) namespace: String,
    pub(crate) relation: String,
    pub(crate) direct_relation: bool,
    pub(crate) rules: Vec<AuthzRelationRule>,
}

#[derive(Debug, Clone)]
struct SchemaMember {
    direct_relation: bool,
    rules: Vec<AuthzRelationRule>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UsersetRuleKey {
    namespace: String,
    relation: String,
}

impl SchemaRuleIndex {
    pub(crate) fn from_materialized_relations(
        bound_namespaces: impl IntoIterator<Item = String>,
        relations: impl IntoIterator<Item = MaterializedSchemaRelation>,
    ) -> Self {
        let members_by_userset = relations
            .into_iter()
            .map(|relation| {
                (
                    UsersetRuleKey {
                        namespace: relation.namespace,
                        relation: relation.relation,
                    },
                    SchemaMember {
                        direct_relation: relation.direct_relation,
                        rules: relation.rules,
                    },
                )
            })
            .collect();
        Self {
            members_by_userset,
            schema_bound_namespaces: bound_namespaces.into_iter().collect(),
        }
    }

    pub(crate) async fn load<'a, I>(
        storage: &Storage,
        tenant_id: i64,
        current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
        seed_namespaces: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut namespaces = seed_namespaces
            .into_iter()
            .filter(|namespace| !namespace.is_empty())
            .map(str::to_string)
            .collect::<BTreeSet<_>>();
        for record in current.values().filter(|record| record.operation == "add") {
            namespaces.insert(record.namespace.clone());
            if record.subject_kind == "userset"
                && let Some(subject) = parse_userset_subject(&record.subject_id)
            {
                namespaces.insert(normalize_namespace_for_scope(
                    &record.namespace,
                    subject.namespace,
                ));
            } else if !record.subject_kind.is_empty() {
                namespaces.insert(normalize_namespace_for_scope(
                    &record.namespace,
                    &record.subject_kind,
                ));
            }
        }

        let mut members_by_userset = BTreeMap::new();
        let mut schema_bound_namespaces = BTreeSet::new();
        for namespace in namespaces {
            let (realm_id, local_namespace) = namespace_realm_parts(&namespace);
            let Some(schema) = authz_realm_schema::read_bound_namespace_schema(
                storage,
                tenant_id,
                &realm_id,
                &local_namespace,
            )
            .await?
            else {
                continue;
            };
            schema_bound_namespaces.insert(namespace.clone());
            for relation in schema.relations {
                let direct_relation = crate::authz_schema_contract::is_direct_relation(&relation);
                members_by_userset.insert(
                    UsersetRuleKey {
                        namespace: namespace.clone(),
                        relation: relation.relation,
                    },
                    SchemaMember {
                        direct_relation,
                        rules: relation.rules,
                    },
                );
            }
        }
        Ok(Self {
            members_by_userset,
            schema_bound_namespaces,
        })
    }

    fn relation_rules(&self, userset: &UsersetRef) -> &[AuthzRelationRule] {
        self.members_by_userset
            .get(&UsersetRuleKey {
                namespace: userset.namespace.clone(),
                relation: userset.relation.clone(),
            })
            .map(|member| member.rules.as_slice())
            .unwrap_or(&[])
    }

    fn is_direct_relation(&self, userset: &UsersetRef) -> bool {
        if let Some(member) = self.members_by_userset.get(&UsersetRuleKey {
            namespace: userset.namespace.clone(),
            relation: userset.relation.clone(),
        }) {
            return member.direct_relation;
        }
        !self.schema_bound_namespaces.contains(&userset.namespace)
    }
}

pub(crate) fn resolve_userset(
    current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
    schema_index: &SchemaRuleIndex,
    userset: &UsersetRef,
    subject: &SubjectRef,
) -> Result<bool> {
    let mut visited = BTreeSet::new();
    resolve_userset_inner(current, schema_index, userset, subject, &mut visited)
}

pub(crate) fn collect_subjects_for_userset(
    current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
    schema_index: &SchemaRuleIndex,
    userset: &UsersetRef,
) -> Result<BTreeSet<SubjectRef>> {
    let mut visited = BTreeSet::new();
    let mut subjects = BTreeSet::new();
    collect_subjects_inner(current, schema_index, userset, &mut visited, &mut subjects)?;
    Ok(subjects)
}

fn resolve_userset_inner(
    current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
    schema_index: &SchemaRuleIndex,
    userset: &UsersetRef,
    subject: &SubjectRef,
    visited: &mut BTreeSet<UsersetRef>,
) -> Result<bool> {
    if !visited.insert(userset.clone()) {
        return Ok(false);
    }

    if schema_index.is_direct_relation(userset) {
        if direct_tuple_grants(current, userset, subject) {
            visited.remove(userset);
            return Ok(true);
        }

        if explicit_userset_tuple_grants(current, schema_index, userset, subject, visited)? {
            visited.remove(userset);
            return Ok(true);
        }
    }

    for rule in schema_index.relation_rules(userset) {
        match rule.kind.as_str() {
            "inherit" => {
                let inherited = UsersetRef {
                    namespace: userset.namespace.clone(),
                    object_id: userset.object_id.clone(),
                    relation: rule.relation.clone(),
                };
                if resolve_userset_inner(current, schema_index, &inherited, subject, visited)? {
                    visited.remove(userset);
                    return Ok(true);
                }
            }
            "computed" | "tuple_to_userset" => {
                for edge in relation_edges(current, userset, &rule.tuple_relation) {
                    let Some(related) = userset_from_relation_edge(
                        edge,
                        &userset.namespace,
                        &rule.target_relation,
                    )?
                    else {
                        continue;
                    };
                    if resolve_userset_inner(current, schema_index, &related, subject, visited)? {
                        visited.remove(userset);
                        return Ok(true);
                    }
                }
            }
            _ => {}
        }
    }

    visited.remove(userset);
    Ok(false)
}

fn collect_subjects_inner(
    current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
    schema_index: &SchemaRuleIndex,
    userset: &UsersetRef,
    visited: &mut BTreeSet<UsersetRef>,
    subjects: &mut BTreeSet<SubjectRef>,
) -> Result<()> {
    if !visited.insert(userset.clone()) {
        return Ok(());
    }

    if schema_index.is_direct_relation(userset) {
        for record in direct_relation_records(current, userset, &userset.relation) {
            if record.subject_kind == "userset" {
                if record.caveat_hash.is_empty()
                    && let Some(next) =
                        userset_from_userset_subject(&record.subject_id, &userset.namespace, None)?
                {
                    collect_subjects_inner(current, schema_index, &next, visited, subjects)?;
                }
            } else {
                subjects.insert(SubjectRef {
                    kind: record.subject_kind.clone(),
                    id: record.subject_id.clone(),
                    caveat_hash: record.caveat_hash.clone(),
                });
            }
        }
    }

    for rule in schema_index.relation_rules(userset) {
        match rule.kind.as_str() {
            "inherit" => {
                let inherited = UsersetRef {
                    namespace: userset.namespace.clone(),
                    object_id: userset.object_id.clone(),
                    relation: rule.relation.clone(),
                };
                collect_subjects_inner(current, schema_index, &inherited, visited, subjects)?;
            }
            "computed" | "tuple_to_userset" => {
                for edge in relation_edges(current, userset, &rule.tuple_relation) {
                    let Some(related) = userset_from_relation_edge(
                        edge,
                        &userset.namespace,
                        &rule.target_relation,
                    )?
                    else {
                        continue;
                    };
                    collect_subjects_inner(current, schema_index, &related, visited, subjects)?;
                }
            }
            _ => {}
        }
    }

    visited.remove(userset);
    Ok(())
}

fn direct_tuple_grants(
    current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
    userset: &UsersetRef,
    subject: &SubjectRef,
) -> bool {
    let direct_key = TupleViewKey {
        namespace: userset.namespace.clone(),
        object_id: userset.object_id.clone(),
        relation: userset.relation.clone(),
        subject_kind: subject.kind.clone(),
        subject_id: subject.id.clone(),
        caveat_hash: subject.caveat_hash.clone(),
    };
    current
        .get(&direct_key)
        .is_some_and(|record| record.operation == "add")
}

fn explicit_userset_tuple_grants(
    current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
    schema_index: &SchemaRuleIndex,
    userset: &UsersetRef,
    subject: &SubjectRef,
    visited: &mut BTreeSet<UsersetRef>,
) -> Result<bool> {
    for record in relation_edges(current, userset, &userset.relation) {
        if record.subject_kind != "userset" {
            continue;
        }
        let Some(next) =
            userset_from_userset_subject(&record.subject_id, &userset.namespace, None)?
        else {
            continue;
        };
        if resolve_userset_inner(current, schema_index, &next, subject, visited)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn direct_relation_records<'a>(
    current: &'a BTreeMap<TupleViewKey, AuthzTupleRecord>,
    userset: &UsersetRef,
    relation: &str,
) -> impl Iterator<Item = &'a AuthzTupleRecord> {
    current.values().filter(move |record| {
        record.namespace == userset.namespace
            && record.object_id == userset.object_id
            && record.relation == relation
            && record.operation == "add"
    })
}

fn relation_edges<'a>(
    current: &'a BTreeMap<TupleViewKey, AuthzTupleRecord>,
    userset: &UsersetRef,
    relation: &str,
) -> impl Iterator<Item = &'a AuthzTupleRecord> {
    current.values().filter(move |record| {
        record.namespace == userset.namespace
            && record.object_id == userset.object_id
            && record.relation == relation
            && record.operation == "add"
            && record.caveat_hash.is_empty()
    })
}

fn userset_from_relation_edge(
    record: &AuthzTupleRecord,
    scope_namespace: &str,
    target_relation: &str,
) -> Result<Option<UsersetRef>> {
    if record.subject_kind == "userset" {
        return userset_from_userset_subject(
            &record.subject_id,
            scope_namespace,
            Some(target_relation),
        );
    }
    if record.subject_kind.is_empty() || record.subject_id.is_empty() || target_relation.is_empty()
    {
        return Ok(None);
    }
    Ok(Some(UsersetRef {
        namespace: normalize_namespace_for_scope(scope_namespace, &record.subject_kind),
        object_id: record.subject_id.clone(),
        relation: target_relation.to_string(),
    }))
}

fn userset_from_userset_subject(
    subject_id: &str,
    scope_namespace: &str,
    override_relation: Option<&str>,
) -> Result<Option<UsersetRef>> {
    let Some(parsed) = parse_userset_subject(subject_id) else {
        return Ok(None);
    };
    let relation = override_relation
        .filter(|relation| !relation.is_empty())
        .unwrap_or(parsed.relation);
    if relation.is_empty() {
        return Err(anyhow!("invalid userset subject reference"));
    }
    Ok(Some(UsersetRef {
        namespace: normalize_namespace_for_scope(scope_namespace, parsed.namespace),
        object_id: parsed.object_id.to_string(),
        relation: relation.to_string(),
    }))
}

fn normalize_namespace_for_scope(scope_namespace: &str, namespace: &str) -> String {
    if split_realm_namespace(namespace).is_some() {
        return namespace.to_string();
    }
    let Some((realm_id, _)) = split_realm_namespace(scope_namespace) else {
        return namespace.to_string();
    };
    encode_realm_namespace(&realm_id, namespace)
}

fn namespace_realm_parts(namespace: &str) -> (String, String) {
    split_realm_namespace(namespace)
        .map(|(realm, ns)| (realm, ns.to_string()))
        .unwrap_or_else(|| (DEFAULT_AUTHZ_REALM_ID.to_string(), namespace.to_string()))
}
