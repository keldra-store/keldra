use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;

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
    tuples: BTreeMap<UsersetRef, DirectSubjects>,
    tuple_count: usize,
    limits: AuthorizationLimits,
}

/// Canonical direct-relation members. Keeping objects and nested usersets in
/// separate ordered sets makes exact object membership logarithmic without
/// scanning unrelated grantees; only graph edges are traversed recursively.
#[derive(Debug, Clone, Default)]
struct DirectSubjects {
    objects: BTreeSet<ObjectRef>,
    usersets: BTreeSet<UsersetRef>,
}

impl Authorization {
    pub fn new(
        realm_id: RealmId,
        schema: Schema,
        tuples: impl IntoIterator<Item = Tuple>,
        limits: AuthorizationLimits,
    ) -> crate::Result<Self> {
        let schema = CompiledSchema::compile(&schema, limits)?;
        let mut indexed: BTreeMap<UsersetRef, DirectSubjects> = BTreeMap::new();
        let mut tuple_count = 0usize;
        for (index, tuple) in tuples.into_iter().enumerate() {
            tuple_count = index + 1;
            if tuple_count > limits.max_tuples {
                return Err(AuthorizationError::InvalidSchema(format!(
                    "tuple set has {tuple_count} entries, exceeding {}",
                    limits.max_tuples
                )));
            }
            validate_tuple(&schema, &tuple)
                .map_err(|reason| AuthorizationError::InvalidTuple { index, reason })?;
            let userset = UsersetRef {
                object: tuple.object,
                relation: tuple.relation,
            };
            let members = indexed.entry(userset).or_default();
            let inserted = match tuple.subject {
                TupleSubject::Object(subject) => members.objects.insert(subject),
                TupleSubject::Userset(subject) => members.usersets.insert(subject),
            };
            if !inserted {
                return Err(AuthorizationError::InvalidTuple {
                    index,
                    reason: "duplicate tuple".into(),
                });
            }
        }

        Ok(Self {
            realm_id,
            schema,
            tuples: indexed,
            tuple_count,
            limits,
        })
    }

    pub fn check(&self, check: &AuthorizationCheck) -> crate::Result<bool> {
        self.check_with_memo(check, &mut BTreeSet::new(), 0)
    }

    /// Evaluate a bounded batch while reusing positive graph results within
    /// this immutable authorization revision. Limits remain per check.
    pub fn check_many(&self, checks: &[AuthorizationCheck]) -> crate::Result<Vec<bool>> {
        let mut allowed = BTreeSet::new();
        let max_allowed = checks
            .len()
            .saturating_mul(self.limits.max_depth)
            .min(self.limits.max_steps);
        checks
            .iter()
            .map(|check| self.check_with_memo(check, &mut allowed, max_allowed))
            .collect()
    }

    fn check_with_memo(
        &self,
        check: &AuthorizationCheck,
        allowed: &mut BTreeSet<(ObjectRef, UsersetRef)>,
        max_allowed: usize,
    ) -> crate::Result<bool> {
        validate_object(&check.subject).map_err(AuthorizationError::InvalidCheck)?;
        validate_object(&check.object).map_err(AuthorizationError::InvalidCheck)?;
        validate_relation(&check.relation).map_err(AuthorizationError::InvalidCheck)?;
        self.schema
            .require_relation(&check.object.namespace, &check.relation)?;

        Evaluator {
            authorization: self,
            visited: BTreeSet::new(),
            allowed,
            max_allowed,
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

    /// Conservative weight of the owned compiled projection used for bounded
    /// cache admission. This is not an allocator accounting API.
    pub fn estimated_heap_bytes(&self) -> usize {
        let mut bytes = self
            .realm_id
            .as_str()
            .len()
            .saturating_add(self.schema.estimated_heap_bytes());
        for (userset, subjects) in &self.tuples {
            bytes = bytes
                .saturating_add(size_of::<(UsersetRef, DirectSubjects)>())
                .saturating_add(userset_bytes(userset));
            for subject in &subjects.objects {
                bytes = bytes
                    .saturating_add(size_of::<ObjectRef>())
                    .saturating_add(object_bytes(subject));
            }
            for subject in &subjects.usersets {
                bytes = bytes
                    .saturating_add(size_of::<UsersetRef>())
                    .saturating_add(userset_bytes(subject));
            }
        }
        bytes
    }
}

fn userset_bytes(userset: &UsersetRef) -> usize {
    object_bytes(&userset.object).saturating_add(userset.relation.len())
}

fn object_bytes(object: &ObjectRef) -> usize {
    let id_bytes = match &object.id {
        crate::ObjectId::Opaque(id) => id.len(),
        crate::ObjectId::ExactPath(path) => path
            .tenant
            .len()
            .saturating_add(path.bucket.len())
            .saturating_add(path.path.len()),
    };
    object.namespace.len().saturating_add(id_bytes)
}

struct Evaluator<'a> {
    authorization: &'a Authorization,
    visited: BTreeSet<UsersetRef>,
    allowed: &'a mut BTreeSet<(ObjectRef, UsersetRef)>,
    max_allowed: usize,
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
        let memo_key = (self.max_allowed != 0).then(|| (subject.clone(), userset.clone()));
        if memo_key
            .as_ref()
            .is_some_and(|memo_key| self.allowed.contains(memo_key))
        {
            return Ok(true);
        }
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
        if allowed
            && self.allowed.len() < self.max_allowed
            && let Some(memo_key) = memo_key
        {
            self.allowed.insert(memo_key);
        }
        Ok(allowed)
    }

    fn resolve_direct(
        &mut self,
        userset: &UsersetRef,
        subject: &ObjectRef,
        depth: usize,
    ) -> crate::Result<bool> {
        let authorization = self.authorization;
        let Some(tuple_subjects) = authorization.tuples.get(userset) else {
            return Ok(false);
        };
        if tuple_subjects.objects.contains(subject) {
            return Ok(true);
        }
        for candidate in &tuple_subjects.usersets {
            self.step()?;
            if self.resolve(candidate, subject, depth + 1)? {
                return Ok(true);
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
                    let targets = targets
                        .objects
                        .iter()
                        .cloned()
                        .chain(
                            targets
                                .usersets
                                .iter()
                                .map(|userset| userset.object.clone()),
                        )
                        .collect::<Vec<_>>();
                    for target_object in targets {
                        self.step()?;
                        let target = UsersetRef {
                            object: target_object,
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
