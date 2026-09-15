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

/// Evaluate against a persistent Leopard adjacency projection without first
/// materializing every tuple in the realm. Permission rewrites use forward
/// adjacency only where tuple-to-userset semantics require it; direct and
/// nested-group membership walks reverse ancestry from the concrete subject,
/// avoiding scans of unrelated descendants. Each adjacency is loaded at most
/// once per batch. A schema-precomputed forward fallback preserves semantics
/// when a writable relation can contain a userset whose named relation is a
/// permission rewrite, because that edge cannot be inferred from raw reverse
/// tuples alone. Canonical tuple validation remains the responsibility of the
/// authority that atomically maintains or rebuilds that projection.
pub fn check_many_with_leopard<F, R>(
    realm_id: RealmId,
    schema: Schema,
    checks: &[AuthorizationCheck],
    limits: AuthorizationLimits,
    load_forward: F,
    load_reverse: R,
) -> crate::Result<Vec<bool>>
where
    F: FnMut(&UsersetRef) -> crate::Result<Vec<TupleSubject>>,
    R: FnMut(&TupleSubject) -> crate::Result<Vec<UsersetRef>>,
{
    LeopardAuthorization::new(realm_id, schema, limits)?.check_many(
        checks,
        load_forward,
        load_reverse,
    )
}

/// Reusable compiled schema for revision-pinned Leopard traversal. It owns no
/// tuples and is therefore safe to cache across tuple-only realm revisions.
#[derive(Debug, Clone)]
pub struct LeopardAuthorization {
    realm_id: RealmId,
    schema: CompiledSchema,
    reverse_incomplete_direct_relations: BTreeSet<(String, String)>,
    limits: AuthorizationLimits,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LeopardEvaluationStats {
    /// Number of non-memoized userset nodes entered by rewrite traversal.
    /// This includes permission usersets which do not require a direct-edge
    /// read, so it is intentionally distinct from either adjacency-load count.
    pub visited_usersets: u64,
    pub forward_prefix_reads: u64,
    pub forward_usersets_loaded: u64,
    pub forward_edges_loaded: u64,
    pub reverse_prefix_reads: u64,
    pub reverse_subjects_loaded: u64,
    pub reverse_edges_loaded: u64,
    pub adjacency_cache_hits: u64,
    pub adjacency_cache_misses: u64,
    pub evaluation_steps: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeopardEvaluation {
    pub allowed: Vec<bool>,
    pub stats: LeopardEvaluationStats,
}

impl LeopardAuthorization {
    pub fn new(
        realm_id: RealmId,
        schema: Schema,
        limits: AuthorizationLimits,
    ) -> crate::Result<Self> {
        let schema = CompiledSchema::compile(&schema, limits)?;
        let reverse_incomplete_direct_relations =
            schema.direct_relations_requiring_forward_membership();
        Ok(Self {
            realm_id,
            schema,
            reverse_incomplete_direct_relations,
            limits,
        })
    }

    pub fn check_many<F, R>(
        &self,
        checks: &[AuthorizationCheck],
        load_forward: F,
        load_reverse: R,
    ) -> crate::Result<Vec<bool>>
    where
        F: FnMut(&UsersetRef) -> crate::Result<Vec<TupleSubject>>,
        R: FnMut(&TupleSubject) -> crate::Result<Vec<UsersetRef>>,
    {
        Ok(self
            .check_many_with_evidence(checks, load_forward, load_reverse)?
            .allowed)
    }

    pub fn check_many_with_evidence<F, R>(
        &self,
        checks: &[AuthorizationCheck],
        mut load_forward: F,
        mut load_reverse: R,
    ) -> crate::Result<LeopardEvaluation>
    where
        F: FnMut(&UsersetRef) -> crate::Result<Vec<TupleSubject>>,
        R: FnMut(&TupleSubject) -> crate::Result<Vec<UsersetRef>>,
    {
        let schema = &self.schema;
        let limits = self.limits;
        let mut allowed = BTreeSet::new();
        let max_allowed = checks
            .len()
            .saturating_mul(limits.max_depth)
            .min(limits.max_steps);
        let mut loaded_forward = BTreeMap::new();
        let mut loaded_reverse = BTreeMap::new();
        let mut loaded_edges = BTreeSet::new();
        let mut stats = LeopardEvaluationStats::default();
        let mut results = Vec::with_capacity(checks.len());
        for check in checks {
            validate_object(&check.subject).map_err(AuthorizationError::InvalidCheck)?;
            validate_object(&check.object).map_err(AuthorizationError::InvalidCheck)?;
            validate_relation(&check.relation).map_err(AuthorizationError::InvalidCheck)?;
            schema.require_relation(&check.object.namespace, &check.relation)?;
            let mut evaluator = LeopardEvaluator {
                schema,
                reverse_incomplete_direct_relations: &self.reverse_incomplete_direct_relations,
                limits,
                load_forward: &mut load_forward,
                load_reverse: &mut load_reverse,
                loaded_forward: &mut loaded_forward,
                loaded_reverse: &mut loaded_reverse,
                loaded_edges: &mut loaded_edges,
                stats: &mut stats,
                visited: BTreeSet::new(),
                allowed: &mut allowed,
                max_allowed,
                steps: 0,
            };
            results.push(evaluator.resolve(
                &UsersetRef {
                    object: check.object.clone(),
                    relation: check.relation.clone(),
                },
                &check.subject,
                0,
            )?);
        }
        Ok(LeopardEvaluation {
            allowed: results,
            stats,
        })
    }

    pub fn realm_id(&self) -> &RealmId {
        &self.realm_id
    }

    pub fn estimated_heap_bytes(&self) -> usize {
        let relation_bytes = self
            .reverse_incomplete_direct_relations
            .iter()
            .map(|(namespace, relation)| {
                std::mem::size_of::<(String, String)>()
                    .saturating_add(namespace.len())
                    .saturating_add(relation.len())
            })
            .sum::<usize>();
        self.realm_id
            .as_str()
            .len()
            .saturating_add(self.schema.estimated_heap_bytes())
            .saturating_add(relation_bytes)
    }
}

struct LeopardEvaluator<'a, F, R> {
    schema: &'a CompiledSchema,
    reverse_incomplete_direct_relations: &'a BTreeSet<(String, String)>,
    limits: AuthorizationLimits,
    load_forward: &'a mut F,
    load_reverse: &'a mut R,
    loaded_forward: &'a mut BTreeMap<UsersetRef, std::sync::Arc<[TupleSubject]>>,
    loaded_reverse: &'a mut BTreeMap<TupleSubject, std::sync::Arc<[UsersetRef]>>,
    loaded_edges: &'a mut BTreeSet<(UsersetRef, TupleSubject)>,
    stats: &'a mut LeopardEvaluationStats,
    visited: BTreeSet<UsersetRef>,
    allowed: &'a mut BTreeSet<(ObjectRef, UsersetRef)>,
    max_allowed: usize,
    steps: usize,
}

impl<F, R> LeopardEvaluator<'_, F, R>
where
    F: FnMut(&UsersetRef) -> crate::Result<Vec<TupleSubject>>,
    R: FnMut(&TupleSubject) -> crate::Result<Vec<UsersetRef>>,
{
    fn resolve(
        &mut self,
        userset: &UsersetRef,
        subject: &ObjectRef,
        depth: usize,
    ) -> crate::Result<bool> {
        if depth >= self.limits.max_depth {
            return Err(AuthorizationError::EvaluationLimit {
                limit: "depth",
                maximum: self.limits.max_depth,
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
        self.stats.visited_usersets = self.stats.visited_usersets.saturating_add(1);
        let relation = self
            .schema
            .require_relation(&userset.object.namespace, &userset.relation)?;
        let permitted = match relation {
            CompiledRelation::Direct { .. } => self.resolve_direct(userset, subject, depth)?,
            CompiledRelation::Permission { rules } => {
                self.resolve_rules(userset, subject, depth, rules.clone())?
            }
        };
        self.visited.remove(userset);
        if permitted
            && self.allowed.len() < self.max_allowed
            && let Some(memo_key) = memo_key
        {
            self.allowed.insert(memo_key);
        }
        Ok(permitted)
    }

    fn forward_subjects(
        &mut self,
        userset: &UsersetRef,
    ) -> crate::Result<std::sync::Arc<[TupleSubject]>> {
        if let Some(subjects) = self.loaded_forward.get(userset) {
            self.stats.adjacency_cache_hits = self.stats.adjacency_cache_hits.saturating_add(1);
            return Ok(subjects.clone());
        }
        self.stats.adjacency_cache_misses = self.stats.adjacency_cache_misses.saturating_add(1);
        let mut subjects = (self.load_forward)(userset)?;
        self.stats.forward_prefix_reads = self.stats.forward_prefix_reads.saturating_add(1);
        self.stats.forward_usersets_loaded = self.stats.forward_usersets_loaded.saturating_add(1);
        self.stats.forward_edges_loaded = self
            .stats
            .forward_edges_loaded
            .saturating_add(u64::try_from(subjects.len()).unwrap_or(u64::MAX));
        subjects.sort();
        subjects.dedup();
        for (index, subject) in subjects.iter().enumerate() {
            validate_tuple(
                self.schema,
                &Tuple {
                    object: userset.object.clone(),
                    relation: userset.relation.clone(),
                    subject: subject.clone(),
                },
            )
            .map_err(|reason| AuthorizationError::InvalidTuple { index, reason })?;
            self.record_loaded_edge(userset.clone(), subject.clone())?;
        }
        let subjects = std::sync::Arc::<[TupleSubject]>::from(subjects);
        self.loaded_forward
            .insert(userset.clone(), subjects.clone());
        Ok(subjects)
    }

    fn containing_usersets(
        &mut self,
        subject: &TupleSubject,
    ) -> crate::Result<std::sync::Arc<[UsersetRef]>> {
        if let Some(usersets) = self.loaded_reverse.get(subject) {
            self.stats.adjacency_cache_hits = self.stats.adjacency_cache_hits.saturating_add(1);
            return Ok(usersets.clone());
        }
        self.stats.adjacency_cache_misses = self.stats.adjacency_cache_misses.saturating_add(1);
        let mut usersets = (self.load_reverse)(subject)?;
        self.stats.reverse_prefix_reads = self.stats.reverse_prefix_reads.saturating_add(1);
        self.stats.reverse_subjects_loaded = self.stats.reverse_subjects_loaded.saturating_add(1);
        self.stats.reverse_edges_loaded = self
            .stats
            .reverse_edges_loaded
            .saturating_add(u64::try_from(usersets.len()).unwrap_or(u64::MAX));
        usersets.sort();
        usersets.dedup();
        for (index, userset) in usersets.iter().enumerate() {
            validate_tuple(
                self.schema,
                &Tuple {
                    object: userset.object.clone(),
                    relation: userset.relation.clone(),
                    subject: subject.clone(),
                },
            )
            .map_err(|reason| AuthorizationError::InvalidTuple { index, reason })?;
            self.record_loaded_edge(userset.clone(), subject.clone())?;
        }
        let usersets = std::sync::Arc::<[UsersetRef]>::from(usersets);
        self.loaded_reverse
            .insert(subject.clone(), usersets.clone());
        Ok(usersets)
    }

    fn record_loaded_edge(
        &mut self,
        userset: UsersetRef,
        subject: TupleSubject,
    ) -> crate::Result<()> {
        self.loaded_edges.insert((userset, subject));
        if self.loaded_edges.len() > self.limits.max_tuples {
            return Err(AuthorizationError::EvaluationLimit {
                limit: "tuple",
                maximum: self.limits.max_tuples,
            });
        }
        Ok(())
    }

    fn resolve_direct(
        &mut self,
        userset: &UsersetRef,
        subject: &ObjectRef,
        depth: usize,
    ) -> crate::Result<bool> {
        if self.resolve_reverse_membership(
            userset,
            &TupleSubject::Object(subject.clone()),
            depth,
            &mut BTreeSet::new(),
        )? {
            return Ok(true);
        }
        if !self
            .reverse_incomplete_direct_relations
            .contains(&(userset.object.namespace.clone(), userset.relation.clone()))
        {
            return Ok(false);
        }
        self.resolve_direct_forward(userset, subject, depth)
    }

    fn resolve_direct_forward(
        &mut self,
        userset: &UsersetRef,
        subject: &ObjectRef,
        depth: usize,
    ) -> crate::Result<bool> {
        let candidates = self.forward_subjects(userset)?;
        for candidate in candidates.iter() {
            self.step()?;
            match candidate {
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

    fn resolve_reverse_membership(
        &mut self,
        target: &UsersetRef,
        subject: &TupleSubject,
        depth: usize,
        visited: &mut BTreeSet<TupleSubject>,
    ) -> crate::Result<bool> {
        if depth >= self.limits.max_depth {
            return Err(AuthorizationError::EvaluationLimit {
                limit: "depth",
                maximum: self.limits.max_depth,
            });
        }
        if !visited.insert(subject.clone()) {
            return Ok(false);
        }
        let parents = self.containing_usersets(subject)?;
        for parent in parents.iter() {
            self.step()?;
            if parent == target
                || self.resolve_reverse_membership(
                    target,
                    &TupleSubject::Userset(parent.clone()),
                    depth + 1,
                    visited,
                )?
            {
                visited.remove(subject);
                return Ok(true);
            }
        }
        visited.remove(subject);
        Ok(false)
    }

    fn resolve_rules(
        &mut self,
        userset: &UsersetRef,
        subject: &ObjectRef,
        depth: usize,
        rules: std::sync::Arc<[crate::RewriteRule]>,
    ) -> crate::Result<bool> {
        for rule in rules.iter() {
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
                    let subjects = self.forward_subjects(&edges)?;
                    for edge in subjects.iter() {
                        self.step()?;
                        let target = UsersetRef {
                            object: match edge {
                                TupleSubject::Object(object) => object.clone(),
                                TupleSubject::Userset(userset) => userset.object.clone(),
                            },
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
        self.stats.evaluation_steps = self.stats.evaluation_steps.saturating_add(1);
        if self.steps > self.limits.max_steps {
            return Err(AuthorizationError::EvaluationLimit {
                limit: "step",
                maximum: self.limits.max_steps,
            });
        }
        Ok(())
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
