use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AllowedSubject, AuthorizationCheck, AuthorizationError, AuthorizationLimits, ObjectRef,
    RealmId, Schema, Tuple, TupleSubject, UsersetRef,
    model::{validate_object, validate_relation, validate_userset},
    schema::{CompiledRelation, CompiledSchema},
};

/// An immutable, deterministic view of one schema and its active grant tuples.
#[derive(Debug, Clone)]
pub struct Authorization {
    realm_id: RealmId,
    schema: CompiledSchema,
    tuples: BTreeMap<UsersetRef, BTreeSet<TupleSubject>>,
    tuple_count: usize,
    limits: AuthorizationLimits,
}

impl Authorization {
    pub fn new(
        realm_id: RealmId,
        schema: Schema,
        tuples: impl IntoIterator<Item = Tuple>,
        limits: AuthorizationLimits,
    ) -> crate::Result<Self> {
        let schema = CompiledSchema::compile(&schema, limits)?;
        let tuples = tuples.into_iter().collect::<Vec<_>>();
        if tuples.len() > limits.max_tuples {
            return Err(AuthorizationError::InvalidSchema(format!(
                "tuple set has {} entries, exceeding {}",
                tuples.len(),
                limits.max_tuples
            )));
        }

        let mut indexed: BTreeMap<UsersetRef, BTreeSet<TupleSubject>> = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for (index, tuple) in tuples.iter().enumerate() {
            validate_tuple(&schema, tuple)
                .map_err(|reason| AuthorizationError::InvalidTuple { index, reason })?;
            if !seen.insert(tuple.clone()) {
                return Err(AuthorizationError::InvalidTuple {
                    index,
                    reason: "duplicate tuple".into(),
                });
            }
            indexed
                .entry(UsersetRef {
                    object: tuple.object.clone(),
                    relation: tuple.relation.clone(),
                })
                .or_default()
                .insert(tuple.subject.clone());
        }

        Ok(Self {
            realm_id,
            schema,
            tuples: indexed,
            tuple_count: tuples.len(),
            limits,
        })
    }

    pub fn check(&self, check: &AuthorizationCheck) -> crate::Result<bool> {
        validate_object(&check.subject).map_err(AuthorizationError::InvalidCheck)?;
        validate_object(&check.object).map_err(AuthorizationError::InvalidCheck)?;
        validate_relation(&check.relation).map_err(AuthorizationError::InvalidCheck)?;
        self.schema
            .require_relation(&check.object.namespace, &check.relation)?;

        Evaluator {
            authorization: self,
            visited: BTreeSet::new(),
            steps: 0,
        }
        .resolve(
            &UsersetRef {
                object: check.object.clone(),
                relation: check.relation.clone(),
            },
            &check.subject,
            0,
        )
    }

    pub fn realm_id(&self) -> &RealmId {
        &self.realm_id
    }

    pub fn check_exact_path(
        &self,
        subject: &ObjectRef,
        namespace: impl Into<String>,
        path: crate::ExactPath,
        relation: impl Into<String>,
    ) -> crate::Result<bool> {
        self.check(&AuthorizationCheck::new(
            subject.clone(),
            ObjectRef::exact_path(namespace, path)?,
            relation,
        ))
    }

    pub fn tuple_count(&self) -> usize {
        self.tuple_count
    }
}

struct Evaluator<'a> {
    authorization: &'a Authorization,
    visited: BTreeSet<UsersetRef>,
    steps: usize,
}

impl Evaluator<'_> {
    fn resolve(
        &mut self,
        userset: &UsersetRef,
        subject: &ObjectRef,
        depth: usize,
    ) -> crate::Result<bool> {
        if depth >= self.authorization.limits.max_depth {
            return Err(AuthorizationError::EvaluationLimit {
                limit: "depth",
                maximum: self.authorization.limits.max_depth,
            });
        }
        self.step()?;
        if !self.visited.insert(userset.clone()) {
            return Ok(false);
        }

        let relation = self
            .authorization
            .schema
            .require_relation(&userset.object.namespace, &userset.relation)?;
        let allowed = match relation {
            CompiledRelation::Direct { .. } => self.resolve_direct(userset, subject, depth)?,
            CompiledRelation::Permission { rules } => {
                let rules = rules.clone();
                self.resolve_rules(userset, subject, depth, &rules)?
            }
        };
        self.visited.remove(userset);
        Ok(allowed)
    }

    fn resolve_direct(
        &mut self,
        userset: &UsersetRef,
        subject: &ObjectRef,
        depth: usize,
    ) -> crate::Result<bool> {
        let Some(tuple_subjects) = self.authorization.tuples.get(userset) else {
            return Ok(false);
        };
        for tuple_subject in tuple_subjects {
            self.step()?;
            match tuple_subject {
                TupleSubject::Object(candidate) if candidate == subject => return Ok(true),
                TupleSubject::Userset(candidate)
                    if self.resolve(candidate, subject, depth + 1)? =>
                {
                    return Ok(true);
                }
                TupleSubject::Object(_) | TupleSubject::Userset(_) => {}
            }
        }
        Ok(false)
    }

    fn resolve_rules(
        &mut self,
        userset: &UsersetRef,
        subject: &ObjectRef,
        depth: usize,
        rules: &[crate::RewriteRule],
    ) -> crate::Result<bool> {
        for rule in rules {
            self.step()?;
            match rule {
                crate::RewriteRule::Inherit { relation } => {
                    let inherited = UsersetRef {
                        object: userset.object.clone(),
                        relation: relation.clone(),
                    };
                    if self.resolve(&inherited, subject, depth + 1)? {
                        return Ok(true);
                    }
                }
                crate::RewriteRule::TupleToUserset {
                    tuple_relation,
                    target_relation,
                } => {
                    let edges = UsersetRef {
                        object: userset.object.clone(),
                        relation: tuple_relation.clone(),
                    };
                    let Some(targets) = self.authorization.tuples.get(&edges) else {
                        continue;
                    };
                    for target in targets {
                        self.step()?;
                        let target_object = match target {
                            TupleSubject::Object(object) => object,
                            TupleSubject::Userset(userset) => &userset.object,
                        };
                        let target = UsersetRef {
                            object: target_object.clone(),
                            relation: target_relation.clone(),
                        };
                        if self.resolve(&target, subject, depth + 1)? {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    fn step(&mut self) -> crate::Result<()> {
        self.steps = self.steps.saturating_add(1);
        if self.steps > self.authorization.limits.max_steps {
            return Err(AuthorizationError::EvaluationLimit {
                limit: "step",
                maximum: self.authorization.limits.max_steps,
            });
        }
        Ok(())
    }
}

fn validate_tuple(schema: &CompiledSchema, tuple: &Tuple) -> std::result::Result<(), String> {
    validate_object(&tuple.object)?;
    validate_relation(&tuple.relation)?;
    let userset = UsersetRef {
        object: tuple.object.clone(),
        relation: tuple.relation.clone(),
    };
    let allowed = schema
        .direct_allowed(&userset)
        .map_err(|error| error.to_string())?;

    match &tuple.subject {
        TupleSubject::Object(subject) => {
            validate_object(subject)?;
        }
        TupleSubject::Userset(subject) => {
            validate_userset(subject)?;
            schema
                .require_relation(&subject.object.namespace, &subject.relation)
                .map_err(|error| error.to_string())?;
        }
    }
    let matches = allowed
        .iter()
        .any(|selector| selector_matches(selector, tuple));
    if !matches {
        return Err(format!(
            "subject is not allowed on direct relation `{}#{}`",
            tuple.object.namespace, tuple.relation
        ));
    }
    Ok(())
}

fn selector_matches(selector: &AllowedSubject, tuple: &Tuple) -> bool {
    match (selector, &tuple.subject) {
        (AllowedSubject::AnyObject { namespace }, TupleSubject::Object(subject)) => {
            subject.namespace == *namespace && !subject.is_public()
        }
        (
            AllowedSubject::AnyUserset {
                namespace,
                relation,
            },
            TupleSubject::Userset(subject),
        ) => subject.object.namespace == *namespace && subject.relation == *relation,
        (AllowedSubject::Exact { subject }, candidate) => subject == candidate,
        (AllowedSubject::SameResourceId { namespace }, TupleSubject::Object(subject)) => {
            subject.namespace == *namespace && subject.id == tuple.object.id && !subject.is_public()
        }
        (AllowedSubject::Public, TupleSubject::Object(subject)) => subject.is_public(),
        (
            AllowedSubject::AnyObject { .. }
            | AllowedSubject::AnyUserset { .. }
            | AllowedSubject::SameResourceId { .. }
            | AllowedSubject::Public,
            TupleSubject::Object(_) | TupleSubject::Userset(_),
        ) => false,
    }
}
