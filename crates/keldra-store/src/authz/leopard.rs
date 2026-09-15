//! Persistent disposable userset adjacency indexes.
//!
//! Canonical tuples remain the sole authority. These keys make direct and
//! reverse userset expansion proportional to the visited graph rather than to
//! every tuple in a realm. They are rebuilt from canonical tuples whenever
//! their revision marker is absent or stale.

use std::sync::Arc;

use keldra_authz::{
    AuthorizationCheck, ExactPath, LeopardAuthorization, ObjectId, ObjectRef, Tuple, TupleSubject,
    UsersetRef,
};
use rocksdb::{Direction, IteratorMode, WriteBatch};

use super::{
    AuthzBatchCheck, AuthzConsistency, AuthzRepository, AuthzRevision, AuthzScope, AuthzStoreError,
    CompiledLeopardKey, StoredSchema, binding_key, decode_json, replication, require_consistency,
    schema_revision_key, storage_error, tenant_revision_key, validate_stored_realm_binding,
    validate_stored_schema,
};
use crate::store::{
    CF_AUTHZ_BINDINGS, CF_AUTHZ_LEOPARD_FORWARD, CF_AUTHZ_LEOPARD_REVERSE, CF_AUTHZ_SCHEMAS,
    CF_AUTHZ_TENANTS,
};

const KEY_FORMAT: u8 = 1;
const FORWARD: u8 = b'F';
const REVERSE: u8 = b'R';
pub(super) const BUILD_MARKER_KEY: &[u8] = b"\0keldra.leopard.v1";
pub(super) const BUILD_MARKER_VALUE: &[u8] = &[1];

pub(super) fn canonical_tuple_prefix(scope: &AuthzScope) -> Vec<u8> {
    let mut key = vec![b'T', KEY_FORMAT];
    put_bytes(&mut key, scope.storage_tenant.as_str().as_bytes());
    put_bytes(&mut key, scope.realm.as_str().as_bytes());
    key
}

pub(super) fn canonical_tuple_key(scope: &AuthzScope, tuple: &Tuple) -> Vec<u8> {
    let mut key = canonical_tuple_prefix(scope);
    put_userset(
        &mut key,
        &UsersetRef {
            object: tuple.object.clone(),
            relation: tuple.relation.clone(),
        },
    );
    put_subject(&mut key, &tuple.subject);
    key
}

impl AuthzRepository {
    pub fn batch_check(
        &self,
        scope: &AuthzScope,
        consistency: AuthzConsistency,
        checks: &[AuthorizationCheck],
    ) -> Result<AuthzBatchCheck, AuthzStoreError> {
        if checks.len() > self.limits.max_checks_per_batch {
            return Err(AuthzStoreError::InvalidInput(format!(
                "authorization check batch has {} entries, exceeding {}",
                checks.len(),
                self.limits.max_checks_per_batch
            )));
        }
        scope.validate()?;
        let snapshot = self.db.snapshot();
        let revision = snapshot
            .get_cf(
                self.cf(CF_AUTHZ_TENANTS)?,
                tenant_revision_key(&scope.storage_tenant),
            )
            .map_err(storage_error)?
            .map(|bytes| decode_json::<AuthzRevision>(&bytes))
            .transpose()?
            .unwrap_or(AuthzRevision::ZERO);
        require_consistency(revision, consistency)?;
        let stored_binding = snapshot
            .get_cf(self.cf(CF_AUTHZ_BINDINGS)?, binding_key(scope))
            .map_err(storage_error)?
            .map(|bytes| decode_json::<replication::StoredRealmBinding>(&bytes))
            .transpose()?
            .ok_or_else(|| {
                AuthzStoreError::MissingBinding(scope.storage_tenant.clone(), scope.realm.clone())
            })?;
        validate_stored_realm_binding(&stored_binding, scope)?;
        if stored_binding.revision > revision {
            return Err(AuthzStoreError::Storage(
                "persisted realm binding is ahead of the tenant authorization revision".into(),
            ));
        }
        let stored_schema = snapshot
            .get_cf(
                self.cf(CF_AUTHZ_SCHEMAS)?,
                schema_revision_key(&scope.storage_tenant, &stored_binding.binding.schema_ref),
            )
            .map_err(storage_error)?
            .map(|bytes| decode_json::<StoredSchema>(&bytes))
            .transpose()?
            .ok_or_else(|| {
                AuthzStoreError::SchemaNotFound(
                    stored_binding.binding.schema_ref.schema_id.clone(),
                    stored_binding.binding.schema_ref.schema_revision,
                )
            })?;
        validate_stored_schema(
            &stored_schema,
            &stored_binding.binding.schema_ref,
            self.limits.evaluator,
        )?;
        let leopard_key = CompiledLeopardKey {
            scope: scope.clone(),
            binding_generation: stored_binding.binding.generation,
            schema_ref: stored_binding.binding.schema_ref.clone(),
            limits: self.limits.evaluator,
        };
        let evaluator = self
            .leopard_cache
            .lock()
            .map_err(|_| AuthzStoreError::Storage("Leopard schema cache lock poisoned".into()))?
            .get(&leopard_key);
        let schema_cache_hit = evaluator.is_some();
        let evaluator = match evaluator {
            Some(evaluator) => evaluator,
            None => {
                let evaluator = Arc::new(LeopardAuthorization::new(
                    scope.realm.clone(),
                    stored_schema.schema,
                    self.limits.evaluator,
                )?);
                self.leopard_cache
                    .lock()
                    .map_err(|_| {
                        AuthzStoreError::Storage("Leopard schema cache lock poisoned".into())
                    })?
                    .insert(leopard_key, evaluator.clone());
                evaluator
            }
        };
        let forward_cf = self.cf(CF_AUTHZ_LEOPARD_FORWARD)?;
        let reverse_cf = self.cf(CF_AUTHZ_LEOPARD_REVERSE)?;
        let evaluation = evaluator.check_many_with_evidence(
            checks,
            |userset| {
                let prefix = leopard_forward_prefix(scope, userset);
                let mut subjects = Vec::new();
                for item in snapshot
                    .iterator_cf(forward_cf, IteratorMode::From(&prefix, Direction::Forward))
                {
                    let (key, _) = item.map_err(|error| {
                        keldra_authz::AuthorizationError::EvaluationSource(error.to_string())
                    })?;
                    if !key.starts_with(&prefix) {
                        break;
                    }
                    subjects.push(decode_subject_suffix(&key[prefix.len()..]).map_err(
                        |error| keldra_authz::AuthorizationError::EvaluationSource(error),
                    )?);
                }
                Ok(subjects)
            },
            |subject| {
                let prefix = leopard_reverse_prefix(scope, subject);
                let mut usersets = Vec::new();
                for item in snapshot
                    .iterator_cf(reverse_cf, IteratorMode::From(&prefix, Direction::Forward))
                {
                    let (key, _) = item.map_err(|error| {
                        keldra_authz::AuthorizationError::EvaluationSource(error.to_string())
                    })?;
                    if !key.starts_with(&prefix) {
                        break;
                    }
                    usersets.push(decode_userset_suffix(&key[prefix.len()..]).map_err(
                        |error| keldra_authz::AuthorizationError::EvaluationSource(error),
                    )?);
                }
                Ok(usersets)
            },
        );
        let evaluation = match evaluation {
            Ok(evaluation) => evaluation,
            Err(error) => {
                tracing::info!(
                    operation = "authz_leopard_batch_check",
                    realm = %scope.realm,
                    monotonic_counter.keldra_authz_leopard_limit_failures_total =
                        u64::from(matches!(error, keldra_authz::AuthorizationError::EvaluationLimit { .. })),
                    "Leopard authorization batch failed closed"
                );
                return Err(error.into());
            }
        };
        tracing::info!(
            operation = "authz_leopard_batch_check",
            realm = %scope.realm,
            monotonic_counter.keldra_authz_leopard_check_batches_total = 1_u64,
            monotonic_counter.keldra_authz_leopard_checks_total = u64::try_from(checks.len()).unwrap_or(u64::MAX),
            monotonic_counter.keldra_authz_leopard_visited_usersets_total = evaluation.stats.visited_usersets,
            monotonic_counter.keldra_authz_leopard_prefix_reads_total = evaluation.stats.forward_prefix_reads.saturating_add(evaluation.stats.reverse_prefix_reads),
            monotonic_counter.keldra_authz_leopard_usersets_loaded_total = evaluation.stats.forward_usersets_loaded.saturating_add(evaluation.stats.reverse_subjects_loaded),
            monotonic_counter.keldra_authz_leopard_edges_loaded_total = evaluation.stats.forward_edges_loaded.saturating_add(evaluation.stats.reverse_edges_loaded),
            monotonic_counter.keldra_authz_leopard_forward_prefix_reads_total = evaluation.stats.forward_prefix_reads,
            monotonic_counter.keldra_authz_leopard_forward_edges_loaded_total = evaluation.stats.forward_edges_loaded,
            monotonic_counter.keldra_authz_leopard_reverse_prefix_reads_total = evaluation.stats.reverse_prefix_reads,
            monotonic_counter.keldra_authz_leopard_reverse_edges_loaded_total = evaluation.stats.reverse_edges_loaded,
            monotonic_counter.keldra_authz_leopard_adjacency_cache_hits_total = evaluation.stats.adjacency_cache_hits,
            monotonic_counter.keldra_authz_leopard_adjacency_cache_misses_total = evaluation.stats.adjacency_cache_misses,
            monotonic_counter.keldra_authz_leopard_evaluation_steps_total = evaluation.stats.evaluation_steps,
            monotonic_counter.keldra_authz_leopard_schema_cache_hits_total = u64::from(schema_cache_hit),
            monotonic_counter.keldra_authz_leopard_schema_cache_misses_total = u64::from(!schema_cache_hit),
            monotonic_counter.keldra_authz_leopard_limit_failures_total = 0_u64,
            "evaluated a revision-pinned authorization batch through persistent userset adjacency indexes"
        );
        Ok(AuthzBatchCheck {
            revision,
            realm_revision: stored_binding.revision,
            binding_generation: stored_binding.binding.generation,
            schema_ref: stored_binding.binding.schema_ref,
            allowed: evaluation.allowed,
        })
    }

    /// Reconstruct disposable userset adjacency indexes from canonical tuples
    /// before serving checks. A marker is written only after the bounded
    /// rebuild completes, so interruption simply restarts reconstruction.
    pub(crate) fn ensure_leopard_indexes(&self) -> Result<(), AuthzStoreError> {
        let forward_cf = self.cf(CF_AUTHZ_LEOPARD_FORWARD)?;
        let reverse_cf = self.cf(CF_AUTHZ_LEOPARD_REVERSE)?;
        let current = |cf| self.db.get_cf(cf, BUILD_MARKER_KEY).map_err(storage_error);
        if current(forward_cf)?.as_deref() == Some(BUILD_MARKER_VALUE)
            && current(reverse_cf)?.as_deref() == Some(BUILD_MARKER_VALUE)
        {
            return Ok(());
        }
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| AuthzStoreError::Storage("authorization write lock poisoned".into()))?;
        let mut batch = WriteBatch::default();
        let mut operations = 0usize;
        for cf in [forward_cf, reverse_cf] {
            for item in self.db.iterator_cf(cf, IteratorMode::Start) {
                let (key, _) = item.map_err(storage_error)?;
                batch.delete_cf(cf, key);
                operations += 1;
                if operations == 4_096 {
                    self.write(batch)?;
                    batch = WriteBatch::default();
                    operations = 0;
                }
            }
        }
        for item in self
            .db
            .iterator_cf(self.cf(CF_AUTHZ_BINDINGS)?, IteratorMode::Start)
        {
            let (key, _) = item.map_err(storage_error)?;
            let scope = super::snapshot::decode_binding_scope(&key)
                .map_err(|error| AuthzStoreError::Storage(error.to_string()))?;
            for tuple in self.read_tuples(&scope)? {
                self.stage_leopard_tuple(&mut batch, &scope, &tuple, true)?;
                operations += 1;
                if operations == 4_096 {
                    self.write(batch)?;
                    batch = WriteBatch::default();
                    operations = 0;
                }
            }
        }
        batch.put_cf(forward_cf, BUILD_MARKER_KEY, BUILD_MARKER_VALUE);
        batch.put_cf(reverse_cf, BUILD_MARKER_KEY, BUILD_MARKER_VALUE);
        self.write(batch)
    }

    pub(super) fn stage_leopard_tuple(
        &self,
        batch: &mut WriteBatch,
        scope: &AuthzScope,
        tuple: &Tuple,
        present: bool,
    ) -> Result<(), AuthzStoreError> {
        let forward = leopard_forward_key(scope, tuple);
        let reverse = leopard_reverse_key(scope, tuple);
        if present {
            batch.put_cf(self.cf(CF_AUTHZ_LEOPARD_FORWARD)?, forward, []);
            batch.put_cf(self.cf(CF_AUTHZ_LEOPARD_REVERSE)?, reverse, []);
        } else {
            batch.delete_cf(self.cf(CF_AUTHZ_LEOPARD_FORWARD)?, forward);
            batch.delete_cf(self.cf(CF_AUTHZ_LEOPARD_REVERSE)?, reverse);
        }
        Ok(())
    }

    pub fn leopard_direct_subjects(
        &self,
        scope: &AuthzScope,
        userset: &UsersetRef,
    ) -> Result<Vec<TupleSubject>, AuthzStoreError> {
        let prefix = leopard_forward_prefix(scope, userset);
        let mut subjects = Vec::new();
        for item in self.db.iterator_cf(
            self.cf(CF_AUTHZ_LEOPARD_FORWARD)?,
            IteratorMode::From(&prefix, Direction::Forward),
        ) {
            let (key, _) = item.map_err(storage_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            subjects.push(
                decode_subject_suffix(&key[prefix.len()..]).map_err(AuthzStoreError::Storage)?,
            );
        }
        Ok(subjects)
    }

    pub fn leopard_containing_usersets(
        &self,
        scope: &AuthzScope,
        subject: &TupleSubject,
    ) -> Result<Vec<UsersetRef>, AuthzStoreError> {
        let prefix = leopard_reverse_prefix(scope, subject);
        let mut usersets = Vec::new();
        for item in self.db.iterator_cf(
            self.cf(CF_AUTHZ_LEOPARD_REVERSE)?,
            IteratorMode::From(&prefix, Direction::Forward),
        ) {
            let (key, _) = item.map_err(storage_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            usersets.push(
                decode_userset_suffix(&key[prefix.len()..]).map_err(AuthzStoreError::Storage)?,
            );
        }
        Ok(usersets)
    }
}

pub(super) fn leopard_forward_prefix(scope: &AuthzScope, userset: &UsersetRef) -> Vec<u8> {
    let mut key = scope_prefix(FORWARD, scope);
    put_userset(&mut key, userset);
    key
}

pub(super) fn leopard_forward_key(scope: &AuthzScope, tuple: &Tuple) -> Vec<u8> {
    let parent = UsersetRef {
        object: tuple.object.clone(),
        relation: tuple.relation.clone(),
    };
    let mut key = leopard_forward_prefix(scope, &parent);
    put_subject(&mut key, &tuple.subject);
    key
}

pub(super) fn leopard_reverse_prefix(scope: &AuthzScope, subject: &TupleSubject) -> Vec<u8> {
    let mut key = scope_prefix(REVERSE, scope);
    put_subject(&mut key, subject);
    key
}

pub(super) fn leopard_reverse_key(scope: &AuthzScope, tuple: &Tuple) -> Vec<u8> {
    let mut key = leopard_reverse_prefix(scope, &tuple.subject);
    put_userset(
        &mut key,
        &UsersetRef {
            object: tuple.object.clone(),
            relation: tuple.relation.clone(),
        },
    );
    key
}

fn scope_prefix(kind: u8, scope: &AuthzScope) -> Vec<u8> {
    let mut key = vec![KEY_FORMAT, kind];
    put_bytes(&mut key, scope.storage_tenant.as_str().as_bytes());
    put_bytes(&mut key, scope.realm.as_str().as_bytes());
    key
}

pub(super) fn leopard_scope_prefix(column_family: &str, scope: &AuthzScope) -> Vec<u8> {
    let kind = if column_family == crate::store::CF_AUTHZ_LEOPARD_FORWARD {
        FORWARD
    } else {
        REVERSE
    };
    scope_prefix(kind, scope)
}

fn put_userset(output: &mut Vec<u8>, userset: &UsersetRef) {
    put_object(output, &userset.object);
    put_bytes(output, userset.relation.as_bytes());
}

fn put_subject(output: &mut Vec<u8>, subject: &TupleSubject) {
    match subject {
        TupleSubject::Object(object) => {
            output.push(0);
            put_object(output, object);
        }
        TupleSubject::Userset(userset) => {
            output.push(1);
            put_userset(output, userset);
        }
    }
}

fn put_object(output: &mut Vec<u8>, object: &ObjectRef) {
    put_bytes(output, object.namespace.as_bytes());
    match &object.id {
        ObjectId::Opaque(id) => {
            output.push(0);
            put_bytes(output, id.as_bytes());
        }
        ObjectId::ExactPath(path) => {
            output.push(1);
            put_bytes(output, path.tenant.as_bytes());
            put_bytes(output, path.bucket.as_bytes());
            put_bytes(output, path.path.as_bytes());
        }
    }
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) {
    let length = u32::try_from(value.len()).expect("validated authorization component fits u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
}

fn decode_subject_suffix(encoded: &[u8]) -> Result<TupleSubject, String> {
    let mut input = encoded;
    let kind = take_byte(&mut input)?;
    let subject = match kind {
        0 => TupleSubject::Object(take_object(&mut input)?),
        1 => TupleSubject::Userset(take_userset(&mut input)?),
        _ => return Err("persisted Leopard subject kind is invalid".into()),
    };
    require_consumed(input)?;
    Ok(subject)
}

fn decode_userset_suffix(encoded: &[u8]) -> Result<UsersetRef, String> {
    let mut input = encoded;
    let userset = take_userset(&mut input)?;
    require_consumed(input)?;
    Ok(userset)
}

fn take_userset(input: &mut &[u8]) -> Result<UsersetRef, String> {
    let object = take_object(input)?;
    let relation = take_string(input)?;
    UsersetRef::new(object, relation).map_err(|error| error.to_string())
}

fn take_object(input: &mut &[u8]) -> Result<ObjectRef, String> {
    let namespace = take_string(input)?;
    match take_byte(input)? {
        0 => ObjectRef::opaque(namespace, take_string(input)?).map_err(|error| error.to_string()),
        1 => {
            let tenant = take_string(input)?;
            let bucket = take_string(input)?;
            let path = take_string(input)?;
            let path = ExactPath::new(tenant, bucket, path).map_err(|error| error.to_string())?;
            ObjectRef::exact_path(namespace, path).map_err(|error| error.to_string())
        }
        _ => Err("persisted Leopard object kind is invalid".into()),
    }
}

fn take_string(input: &mut &[u8]) -> Result<String, String> {
    let bytes = take_bytes(input)?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| "persisted Leopard component is not UTF-8".into())
}

fn take_bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let length = input
        .get(..4)
        .ok_or_else(|| "persisted Leopard component length is truncated".to_owned())?;
    let length = usize::try_from(u32::from_be_bytes(
        length.try_into().expect("four-byte slice"),
    ))
    .map_err(|_| "persisted Leopard component length exceeds usize".to_owned())?;
    *input = &input[4..];
    if input.len() < length {
        return Err("persisted Leopard component is truncated".into());
    }
    let (value, remaining) = input.split_at(length);
    *input = remaining;
    Ok(value)
}

fn take_byte(input: &mut &[u8]) -> Result<u8, String> {
    let value = input
        .first()
        .copied()
        .ok_or_else(|| "persisted Leopard key is truncated".to_owned())?;
    *input = &input[1..];
    Ok(value)
}

fn require_consumed(input: &[u8]) -> Result<(), String> {
    if input.is_empty() {
        Ok(())
    } else {
        Err("persisted Leopard key has trailing bytes".into())
    }
}

#[cfg(test)]
mod tests {
    use keldra_authz::{ObjectRef, RealmId, Tuple};

    use super::*;
    use crate::StorageTenantId;

    fn scope() -> AuthzScope {
        AuthzScope::new(
            StorageTenantId::parse("tenant").unwrap(),
            RealmId::custom("documents").unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn keys_group_forward_and_reverse_edges_without_hashing() {
        let document = ObjectRef::opaque("document", "report").unwrap();
        let alice = ObjectRef::opaque("user", "alice").unwrap();
        let bob = ObjectRef::opaque("user", "bob").unwrap();
        let alice_tuple = Tuple::new(document.clone(), "reader", alice.clone());
        let bob_tuple = Tuple::new(document.clone(), "reader", bob);
        let parent = UsersetRef::new(document, "reader").unwrap();

        assert!(
            leopard_forward_key(&scope(), &alice_tuple)
                .starts_with(&leopard_forward_prefix(&scope(), &parent))
        );
        assert!(
            leopard_forward_key(&scope(), &bob_tuple)
                .starts_with(&leopard_forward_prefix(&scope(), &parent))
        );
        assert!(
            leopard_reverse_key(&scope(), &alice_tuple).starts_with(&leopard_reverse_prefix(
                &scope(),
                &TupleSubject::Object(alice)
            ))
        );
        let forward = leopard_forward_key(&scope(), &alice_tuple);
        assert_eq!(
            decode_subject_suffix(&forward[leopard_forward_prefix(&scope(), &parent).len()..])
                .unwrap(),
            alice_tuple.subject.clone()
        );
        let reverse = leopard_reverse_key(&scope(), &alice_tuple);
        assert_eq!(
            decode_userset_suffix(
                &reverse[leopard_reverse_prefix(&scope(), &alice_tuple.subject).len()..]
            )
            .unwrap(),
            parent
        );
    }
}
