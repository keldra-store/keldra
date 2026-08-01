use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AllowedSubject, AuthorizationError, AuthorizationLimits, NamespaceDefinition,
    RelationDefinition, RelationKind, RewriteRule, Schema, TupleSubject, UsersetRef,
    model::{validate_namespace, validate_relation, validate_tuple_subject, validate_userset},
};

#[derive(Debug, Clone)]
pub(crate) struct CompiledSchema {
    namespaces: BTreeMap<String, CompiledNamespace>,
}

#[derive(Debug, Clone)]
struct CompiledNamespace {
    relations: BTreeMap<String, CompiledRelation>,
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledRelation {
    Direct {
        allowed_subjects: BTreeSet<AllowedSubject>,
    },
    Permission {
        rules: Vec<RewriteRule>,
    },
}

impl CompiledSchema {
    pub(crate) fn compile(schema: &Schema, limits: AuthorizationLimits) -> crate::Result<Self> {
        limits.validate()?;
        if schema.namespaces.is_empty() {
            return invalid("schema must contain at least one namespace");
        }
        if schema.namespaces.len() > limits.max_namespaces {
            return invalid(format!(
                "schema has {} namespaces, exceeding {}",
                schema.namespaces.len(),
                limits.max_namespaces
            ));
        }

        let mut namespaces = BTreeMap::new();
        for namespace in &schema.namespaces {
            let compiled = compile_namespace(namespace, limits)?;
            if namespaces
                .insert(namespace.name.clone(), compiled)
                .is_some()
            {
                return invalid(format!("duplicate namespace `{}`", namespace.name));
            }
        }
        let compiled = Self { namespaces };
        compiled.validate_references()?;
        Ok(compiled)
    }

    pub(crate) fn relation(&self, namespace: &str, relation: &str) -> Option<&CompiledRelation> {
        self.namespaces
            .get(namespace)
            .and_then(|namespace| namespace.relations.get(relation))
    }

    pub(crate) fn require_relation(
        &self,
        namespace: &str,
        relation: &str,
    ) -> crate::Result<&CompiledRelation> {
        self.relation(namespace, relation).ok_or_else(|| {
            AuthorizationError::InvalidCheck(format!(
                "relation `{namespace}#{relation}` is not declared"
            ))
        })
    }

    pub(crate) fn direct_allowed(
        &self,
        userset: &UsersetRef,
    ) -> crate::Result<&BTreeSet<AllowedSubject>> {
        match self
            .relation(&userset.object.namespace, &userset.relation)
            .ok_or_else(|| {
                AuthorizationError::InvalidSchema(format!(
                    "tuple targets undeclared relation `{}#{}`",
                    userset.object.namespace, userset.relation
                ))
            })? {
            CompiledRelation::Direct { allowed_subjects } => Ok(allowed_subjects),
            CompiledRelation::Permission { .. } => Err(AuthorizationError::InvalidSchema(format!(
                "permission `{}#{}` cannot be written as a tuple",
                userset.object.namespace, userset.relation
            ))),
        }
    }

    fn validate_references(&self) -> crate::Result<()> {
        for (namespace_name, namespace) in &self.namespaces {
            for (relation_name, relation) in &namespace.relations {
                let rules = match relation {
                    CompiledRelation::Direct { allowed_subjects } => {
                        for allowed in allowed_subjects {
                            match allowed {
                                AllowedSubject::AnyUserset {
                                    namespace,
                                    relation,
                                } => {
                                    self.require_schema_relation(
                                        namespace,
                                        relation,
                                        "allowed userset",
                                    )?;
                                }
                                AllowedSubject::Exact {
                                    subject: TupleSubject::Userset(userset),
                                } => {
                                    self.require_schema_relation(
                                        &userset.object.namespace,
                                        &userset.relation,
                                        "allowed userset",
                                    )?;
                                }
                                AllowedSubject::AnyObject { .. }
                                | AllowedSubject::Exact {
                                    subject: TupleSubject::Object(_),
                                }
                                | AllowedSubject::SameResourceId { .. }
                                | AllowedSubject::Public => {}
                            }
                        }
                        continue;
                    }
                    CompiledRelation::Permission { rules } => rules,
                };

                for rule in rules {
                    match rule {
                        RewriteRule::Inherit { relation } => {
                            self.require_schema_relation(namespace_name, relation, "inherited")?;
                        }
                        RewriteRule::TupleToUserset {
                            tuple_relation,
                            target_relation,
                        } => {
                            let source = self.require_schema_relation(
                                namespace_name,
                                tuple_relation,
                                "tuple-to-userset source",
                            )?;
                            let CompiledRelation::Direct { allowed_subjects } = source else {
                                return invalid(format!(
                                    "tuple-to-userset source `{namespace_name}#{tuple_relation}` for `{namespace_name}#{relation_name}` is not direct"
                                ));
                            };
                            for allowed in allowed_subjects {
                                let (target_namespace, constrained_relation) =
                                    tuple_target(allowed).ok_or_else(|| {
                                        AuthorizationError::InvalidSchema(format!(
                                            "tuple-to-userset source `{namespace_name}#{tuple_relation}` cannot allow the public subject"
                                        ))
                                    })?;
                                if constrained_relation
                                    .is_some_and(|relation| relation != target_relation.as_str())
                                {
                                    return invalid(format!(
                                        "tuple-to-userset target `{target_namespace}#{target_relation}` conflicts with allowed userset relation `{}`",
                                        constrained_relation.expect("checked as present")
                                    ));
                                }
                                self.require_schema_relation(
                                    target_namespace,
                                    target_relation,
                                    "tuple-to-userset target",
                                )?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn require_schema_relation(
        &self,
        namespace: &str,
        relation: &str,
        label: &str,
    ) -> crate::Result<&CompiledRelation> {
        self.relation(namespace, relation).ok_or_else(|| {
            AuthorizationError::InvalidSchema(format!(
                "{label} `{namespace}#{relation}` is not declared"
            ))
        })
    }
}

fn tuple_target(allowed: &AllowedSubject) -> Option<(&str, Option<&str>)> {
    match allowed {
        AllowedSubject::AnyObject { namespace } | AllowedSubject::SameResourceId { namespace } => {
            Some((namespace, None))
        }
        AllowedSubject::AnyUserset {
            namespace,
            relation,
        } => Some((namespace, Some(relation))),
        AllowedSubject::Exact {
            subject: TupleSubject::Object(object),
        } => Some((&object.namespace, None)),
        AllowedSubject::Exact {
            subject: TupleSubject::Userset(userset),
        } => Some((&userset.object.namespace, Some(&userset.relation))),
        AllowedSubject::Public => None,
    }
}

fn compile_namespace(
    namespace: &NamespaceDefinition,
    limits: AuthorizationLimits,
) -> crate::Result<CompiledNamespace> {
    validate_namespace(&namespace.name).map_err(AuthorizationError::InvalidSchema)?;
    if namespace.relations.is_empty() {
        return invalid(format!(
            "namespace `{}` must contain at least one relation",
            namespace.name
        ));
    }
    if namespace.relations.len() > limits.max_relations_per_namespace {
        return invalid(format!(
            "namespace `{}` has {} relations, exceeding {}",
            namespace.name,
            namespace.relations.len(),
            limits.max_relations_per_namespace
        ));
    }

    let mut relations = BTreeMap::new();
    for relation in &namespace.relations {
        let compiled = compile_relation(&namespace.name, relation, limits)?;
        if relations.insert(relation.name.clone(), compiled).is_some() {
            return invalid(format!(
                "duplicate relation `{}#{}`",
                namespace.name, relation.name
            ));
        }
    }
    Ok(CompiledNamespace { relations })
}

fn compile_relation(
    namespace: &str,
    relation: &RelationDefinition,
    limits: AuthorizationLimits,
) -> crate::Result<CompiledRelation> {
    validate_relation(&relation.name).map_err(AuthorizationError::InvalidSchema)?;
    match &relation.kind {
        RelationKind::Direct { allowed_subjects } => {
            validate_item_count(namespace, relation, allowed_subjects.len(), limits)?;
            let mut allowed = BTreeSet::new();
            for subject in allowed_subjects {
                validate_allowed_subject(subject)?;
                if !allowed.insert(subject.clone()) {
                    return invalid(format!(
                        "duplicate allowed subject on `{namespace}#{}`",
                        relation.name
                    ));
                }
            }
            Ok(CompiledRelation::Direct {
                allowed_subjects: allowed,
            })
        }
        RelationKind::Permission { rules } => {
            validate_item_count(namespace, relation, rules.len(), limits)?;
            let mut canonical = BTreeSet::new();
            for rule in rules {
                match rule {
                    RewriteRule::Inherit { relation } => validate_relation(relation),
                    RewriteRule::TupleToUserset {
                        tuple_relation,
                        target_relation,
                    } => validate_relation(tuple_relation)
                        .and_then(|()| validate_relation(target_relation)),
                }
                .map_err(AuthorizationError::InvalidSchema)?;
                if !canonical.insert(rule.clone()) {
                    return invalid(format!(
                        "duplicate rewrite rule on `{namespace}#{}`",
                        relation.name
                    ));
                }
            }
            Ok(CompiledRelation::Permission {
                rules: canonical.into_iter().collect(),
            })
        }
    }
}

fn validate_item_count(
    namespace: &str,
    relation: &RelationDefinition,
    count: usize,
    limits: AuthorizationLimits,
) -> crate::Result<()> {
    if count == 0 {
        return invalid(format!(
            "relation `{namespace}#{}` must not be empty",
            relation.name
        ));
    }
    if count > limits.max_items_per_relation {
        return invalid(format!(
            "relation `{namespace}#{}` has {count} items, exceeding {}",
            relation.name, limits.max_items_per_relation
        ));
    }
    Ok(())
}

fn validate_allowed_subject(subject: &AllowedSubject) -> crate::Result<()> {
    match subject {
        AllowedSubject::AnyObject { namespace } | AllowedSubject::SameResourceId { namespace } => {
            validate_namespace(namespace).map_err(AuthorizationError::InvalidSchema)
        }
        AllowedSubject::AnyUserset {
            namespace,
            relation,
        } => {
            validate_namespace(namespace).map_err(AuthorizationError::InvalidSchema)?;
            validate_relation(relation).map_err(AuthorizationError::InvalidSchema)
        }
        AllowedSubject::Exact { subject } => {
            validate_tuple_subject(subject).map_err(AuthorizationError::InvalidSchema)?;
            if matches!(subject, TupleSubject::Object(object) if object.is_public()) {
                return invalid("the reserved public subject must use the public selector");
            }
            if let TupleSubject::Userset(userset) = subject {
                validate_userset(userset).map_err(AuthorizationError::InvalidSchema)?;
            }
            Ok(())
        }
        AllowedSubject::Public => Ok(()),
    }
}

fn invalid<T>(reason: impl Into<String>) -> crate::Result<T> {
    Err(AuthorizationError::InvalidSchema(reason.into()))
}
