use super::*;
use crate::authz_journal::resolver::MaterializedSchemaRelation;

pub(super) async fn schema_descriptor_rows(
    storage: &Storage,
    tenant_id: i64,
    active_records: &[AuthzTupleRecord],
) -> Result<Vec<AuthzSchemaDescriptorRow>> {
    let namespace_parts = active_records
        .iter()
        .map(|record| namespace_realm_parts(&record.namespace))
        .collect::<BTreeSet<_>>();
    let mut rows = BTreeSet::new();
    for revision in schema_state::collect_latest_schema_revisions(storage, tenant_id).await? {
        for namespace in &revision.namespaces {
            rows.insert(AuthzSchemaDescriptorRow {
                tenant_id,
                realm_id: String::new(),
                namespace: namespace.namespace.clone(),
                schema_id: revision.schema_ref.schema_id.clone(),
                schema_revision: revision.schema_ref.schema_revision,
                schema_digest: revision.schema_ref.schema_digest.clone(),
                binding_generation: 0,
                authz_revision: revision.authz_revision,
            });
        }
    }
    for binding in schema_state::collect_schema_bindings(storage, tenant_id).await? {
        let Some(revision) = authz_realm_schema::read_schema_revision(
            storage,
            tenant_id,
            &binding.schema_ref.schema_id,
            Some(binding.schema_ref.schema_revision),
        )
        .await?
        else {
            continue;
        };
        for namespace in &revision.namespaces {
            rows.insert(AuthzSchemaDescriptorRow {
                tenant_id,
                realm_id: binding.realm_id.clone(),
                namespace: canonical_bound_namespace(&binding.realm_id, &namespace.namespace),
                schema_id: binding.schema_ref.schema_id.clone(),
                schema_revision: binding.schema_ref.schema_revision,
                schema_digest: binding.schema_ref.schema_digest.clone(),
                binding_generation: binding.binding_generation,
                authz_revision: binding.authz_revision,
            });
        }
    }
    for (realm_id, namespace) in namespace_parts {
        if let Some(binding) =
            authz_realm_schema::read_schema_binding(storage, tenant_id, &realm_id).await?
        {
            rows.insert(AuthzSchemaDescriptorRow {
                tenant_id,
                realm_id,
                namespace,
                schema_id: binding.schema_ref.schema_id,
                schema_revision: binding.schema_ref.schema_revision,
                schema_digest: binding.schema_ref.schema_digest,
                binding_generation: binding.binding_generation,
                authz_revision: binding.authz_revision,
            });
        } else {
            rows.insert(AuthzSchemaDescriptorRow {
                tenant_id,
                realm_id,
                namespace,
                schema_id: String::new(),
                schema_revision: 0,
                schema_digest: String::new(),
                binding_generation: 0,
                authz_revision: 0,
            });
        }
    }
    if rows.is_empty() {
        rows.insert(AuthzSchemaDescriptorRow {
            tenant_id,
            realm_id: DEFAULT_AUTHZ_REALM_ID.to_string(),
            namespace: "_empty".to_string(),
            schema_id: String::new(),
            schema_revision: 0,
            schema_digest: String::new(),
            binding_generation: 0,
            authz_revision: 0,
        });
    }
    Ok(rows.into_iter().collect())
}

pub(super) async fn bound_relation_rule_rows(
    storage: &Storage,
    tenant_id: i64,
    active_records: &[AuthzTupleRecord],
) -> Result<Vec<AuthzRelationRuleRow>> {
    let namespace_parts = active_records
        .iter()
        .map(|record| {
            let (realm_id, local_namespace) = namespace_realm_parts(&record.namespace);
            (realm_id, local_namespace, record.namespace.clone())
        })
        .collect::<BTreeSet<_>>();
    let mut rows = BTreeSet::new();
    for (realm_id, namespace, canonical_namespace) in namespace_parts {
        let Some(binding) =
            authz_realm_schema::read_schema_binding(storage, tenant_id, &realm_id).await?
        else {
            continue;
        };
        let Some(schema) = authz_realm_schema::read_bound_namespace_schema(
            storage, tenant_id, &realm_id, &namespace,
        )
        .await?
        else {
            continue;
        };
        for relation in schema.relations {
            let relation_name = relation.relation;
            if relation.rules.is_empty() {
                rows.insert(AuthzRelationRuleRow {
                    realm_id: realm_id.clone(),
                    namespace: canonical_namespace.clone(),
                    relation: relation_name.clone(),
                    rule_kind: "direct".to_string(),
                    inherited_relation: String::new(),
                    tuple_relation: String::new(),
                    target_relation: String::new(),
                    schema_generation: binding.schema_ref.schema_revision,
                });
            }
            for rule in relation.rules {
                rows.insert(AuthzRelationRuleRow {
                    realm_id: realm_id.clone(),
                    namespace: canonical_namespace.clone(),
                    relation: relation_name.clone(),
                    rule_kind: rule.kind,
                    inherited_relation: rule.relation,
                    tuple_relation: rule.tuple_relation,
                    target_relation: rule.target_relation,
                    schema_generation: binding.schema_ref.schema_revision,
                });
            }
        }
    }
    Ok(rows.into_iter().collect())
}

pub(super) async fn all_relation_rule_rows(
    storage: &Storage,
    tenant_id: i64,
    bound_rows: &[AuthzRelationRuleRow],
) -> Result<Vec<AuthzRelationRuleRow>> {
    let mut rows = bound_rows.iter().cloned().collect::<BTreeSet<_>>();
    for revision in schema_state::collect_latest_schema_revisions(storage, tenant_id).await? {
        for namespace in &revision.namespaces {
            insert_relation_rule_rows(
                &mut rows,
                "",
                &namespace.namespace,
                revision.schema_ref.schema_revision,
                &namespace.relations,
            );
        }
    }
    for binding in schema_state::collect_schema_bindings(storage, tenant_id).await? {
        let Some(revision) = authz_realm_schema::read_schema_revision(
            storage,
            tenant_id,
            &binding.schema_ref.schema_id,
            Some(binding.schema_ref.schema_revision),
        )
        .await?
        else {
            continue;
        };
        for namespace in &revision.namespaces {
            insert_relation_rule_rows(
                &mut rows,
                &binding.realm_id,
                &canonical_bound_namespace(&binding.realm_id, &namespace.namespace),
                binding.schema_ref.schema_revision,
                &namespace.relations,
            );
        }
    }
    Ok(rows.into_iter().collect())
}

fn insert_relation_rule_rows(
    rows: &mut BTreeSet<AuthzRelationRuleRow>,
    realm_id: &str,
    namespace: &str,
    schema_generation: u64,
    relations: &[crate::anvil_api::AuthzRelationSchema],
) {
    for relation in relations {
        if relation.rules.is_empty() {
            rows.insert(AuthzRelationRuleRow {
                realm_id: realm_id.to_string(),
                namespace: namespace.to_string(),
                relation: relation.relation.clone(),
                rule_kind: "direct".to_string(),
                inherited_relation: String::new(),
                tuple_relation: String::new(),
                target_relation: String::new(),
                schema_generation,
            });
        }
        for rule in &relation.rules {
            rows.insert(AuthzRelationRuleRow {
                realm_id: realm_id.to_string(),
                namespace: namespace.to_string(),
                relation: relation.relation.clone(),
                rule_kind: rule.kind.clone(),
                inherited_relation: rule.relation.clone(),
                tuple_relation: rule.tuple_relation.clone(),
                target_relation: rule.target_relation.clone(),
                schema_generation,
            });
        }
    }
}

pub(super) fn userset_edge_rows(
    active_records: &[AuthzTupleRecord],
    derived_usersets: &[AuthzDerivedUsersetEntry],
    generation: u64,
) -> Result<Vec<AuthzUsersetEdgeRow>> {
    let mut rows = BTreeSet::new();
    for record in active_records {
        if record.subject_kind == "userset" {
            rows.insert(AuthzUsersetEdgeRow {
                namespace: record.namespace.clone(),
                object_id: record.object_id.clone(),
                relation: record.relation.clone(),
                subject_kind: record.subject_kind.clone(),
                subject_id: record.subject_id.clone(),
                caveat_hash: record.caveat_hash.clone(),
                source: "tuple".to_string(),
                revision: u64::try_from(record.revision)
                    .context("authz tuple revision must be nonnegative")?,
                operation: "add".to_string(),
            });
        }
    }
    for entry in derived_usersets {
        rows.insert(AuthzUsersetEdgeRow {
            namespace: entry.namespace.clone(),
            object_id: entry.object_id.clone(),
            relation: entry.relation.clone(),
            subject_kind: entry.subject_kind.clone(),
            subject_id: entry.subject_id.clone(),
            caveat_hash: entry.caveat_hash.clone(),
            source: "derived_userset".to_string(),
            revision: generation,
            operation: "add".to_string(),
        });
    }
    Ok(rows.into_iter().collect())
}

pub(super) fn derived_userset_entries(
    active_records: &[AuthzTupleRecord],
    schema_descriptor_rows: &[AuthzSchemaDescriptorRow],
    relation_rule_rows: &[AuthzRelationRuleRow],
    current: &BTreeMap<authz_journal::TupleViewKey, AuthzTupleRecord>,
) -> Result<Vec<AuthzDerivedUsersetEntry>> {
    let schema_index = materialized_schema_rule_index(schema_descriptor_rows, relation_rule_rows);
    let mut entries = BTreeSet::new();
    for userset in materialized_userset_targets(active_records, relation_rule_rows) {
        for subject in collect_subjects_for_userset(current, &schema_index, &userset)? {
            entries.insert(AuthzDerivedUsersetEntry {
                namespace: userset.namespace.clone(),
                object_id: userset.object_id.clone(),
                relation: userset.relation.clone(),
                subject_kind: subject.kind,
                subject_id: subject.id,
                caveat_hash: subject.caveat_hash,
            });
        }
    }
    Ok(entries.into_iter().collect())
}

pub(super) fn list_object_rows(
    active_records: &[AuthzTupleRecord],
    derived_usersets: &[AuthzDerivedUsersetEntry],
    schema_descriptor_rows: &[AuthzSchemaDescriptorRow],
    relation_rule_rows: &[AuthzRelationRuleRow],
    current: &BTreeMap<authz_journal::TupleViewKey, AuthzTupleRecord>,
    generation: u64,
) -> Result<Vec<AuthzListObjectsRow>> {
    let mut rows = BTreeSet::new();
    for record in active_records {
        rows.insert(AuthzListObjectsRow {
            namespace: record.namespace.clone(),
            relation: record.relation.clone(),
            subject_kind: record.subject_kind.clone(),
            subject_id: record.subject_id.clone(),
            caveat_hash: record.caveat_hash.clone(),
            object_id: record.object_id.clone(),
            doc_ordinal: authz_doc_ordinal(&record.namespace, &record.object_id),
            revision: u64::try_from(record.revision)
                .context("authz tuple revision must be nonnegative")?,
            operation: "add".to_string(),
        });
    }
    for entry in derived_usersets {
        rows.insert(AuthzListObjectsRow {
            namespace: entry.namespace.clone(),
            relation: entry.relation.clone(),
            subject_kind: entry.subject_kind.clone(),
            subject_id: entry.subject_id.clone(),
            caveat_hash: entry.caveat_hash.clone(),
            object_id: entry.object_id.clone(),
            doc_ordinal: authz_doc_ordinal(&entry.namespace, &entry.object_id),
            revision: generation,
            operation: "add".to_string(),
        });
    }
    let schema_index = materialized_schema_rule_index(schema_descriptor_rows, relation_rule_rows);
    for userset in materialized_userset_targets(active_records, relation_rule_rows) {
        for subject in collect_subjects_for_userset(current, &schema_index, &userset)? {
            rows.insert(AuthzListObjectsRow {
                namespace: userset.namespace.clone(),
                relation: userset.relation.clone(),
                subject_kind: subject.kind,
                subject_id: subject.id,
                caveat_hash: subject.caveat_hash,
                object_id: userset.object_id.clone(),
                doc_ordinal: authz_doc_ordinal(&userset.namespace, &userset.object_id),
                revision: generation,
                operation: "add".to_string(),
            });
        }
    }
    Ok(rows.into_iter().collect())
}

pub(super) fn list_subject_rows(
    active_records: &[AuthzTupleRecord],
    derived_usersets: &[AuthzDerivedUsersetEntry],
    schema_descriptor_rows: &[AuthzSchemaDescriptorRow],
    relation_rule_rows: &[AuthzRelationRuleRow],
    current: &BTreeMap<authz_journal::TupleViewKey, AuthzTupleRecord>,
    generation: u64,
) -> Result<Vec<AuthzListSubjectsRow>> {
    let mut rows = BTreeSet::new();
    for record in active_records {
        rows.insert(AuthzListSubjectsRow {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
            subject_kind: record.subject_kind.clone(),
            subject_id: record.subject_id.clone(),
            caveat_hash: record.caveat_hash.clone(),
            doc_ordinal: authz_doc_ordinal(&record.namespace, &record.object_id),
            revision: u64::try_from(record.revision)
                .context("authz tuple revision must be nonnegative")?,
            operation: "add".to_string(),
        });
    }
    for entry in derived_usersets {
        rows.insert(AuthzListSubjectsRow {
            namespace: entry.namespace.clone(),
            object_id: entry.object_id.clone(),
            relation: entry.relation.clone(),
            subject_kind: entry.subject_kind.clone(),
            subject_id: entry.subject_id.clone(),
            caveat_hash: entry.caveat_hash.clone(),
            doc_ordinal: authz_doc_ordinal(&entry.namespace, &entry.object_id),
            revision: generation,
            operation: "add".to_string(),
        });
    }
    let schema_index = materialized_schema_rule_index(schema_descriptor_rows, relation_rule_rows);
    for userset in materialized_userset_targets(active_records, relation_rule_rows) {
        for subject in collect_subjects_for_userset(current, &schema_index, &userset)? {
            rows.insert(AuthzListSubjectsRow {
                namespace: userset.namespace.clone(),
                object_id: userset.object_id.clone(),
                relation: userset.relation.clone(),
                subject_kind: subject.kind,
                subject_id: subject.id,
                caveat_hash: subject.caveat_hash,
                doc_ordinal: authz_doc_ordinal(&userset.namespace, &userset.object_id),
                revision: generation,
                operation: "add".to_string(),
            });
        }
    }
    Ok(rows.into_iter().collect())
}

fn materialized_schema_rule_index(
    schema_descriptor_rows: &[AuthzSchemaDescriptorRow],
    relation_rule_rows: &[AuthzRelationRuleRow],
) -> SchemaRuleIndex {
    let bound_namespaces = schema_descriptor_rows
        .iter()
        .filter(|row| !row.realm_id.is_empty() && !row.schema_id.is_empty())
        .map(|row| row.namespace.clone())
        .collect::<BTreeSet<_>>();
    let mut relations =
        BTreeMap::<(String, String), (bool, BTreeSet<(String, String, String, String)>)>::new();
    for row in relation_rule_rows
        .iter()
        .filter(|row| !row.realm_id.is_empty())
    {
        let relation = relations
            .entry((row.namespace.clone(), row.relation.clone()))
            .or_default();
        if row.rule_kind == "direct" {
            relation.0 = true;
        } else {
            relation.1.insert((
                row.rule_kind.clone(),
                row.inherited_relation.clone(),
                row.tuple_relation.clone(),
                row.target_relation.clone(),
            ));
        }
    }
    let relations =
        relations
            .into_iter()
            .map(
                |((namespace, relation), (direct_relation, rules))| MaterializedSchemaRelation {
                    namespace,
                    relation,
                    direct_relation,
                    rules: rules
                        .into_iter()
                        .map(
                            |(kind, inherited_relation, tuple_relation, target_relation)| {
                                crate::anvil_api::AuthzRelationRule {
                                    kind,
                                    relation: inherited_relation,
                                    tuple_relation,
                                    target_relation,
                                }
                            },
                        )
                        .collect(),
                },
            );
    SchemaRuleIndex::from_materialized_relations(bound_namespaces, relations)
}

fn materialized_userset_targets(
    active_records: &[AuthzTupleRecord],
    relation_rule_rows: &[AuthzRelationRuleRow],
) -> BTreeSet<UsersetRef> {
    let object_namespaces = active_records
        .iter()
        .map(|record| (record.namespace.clone(), record.object_id.clone()))
        .collect::<BTreeSet<_>>();
    let direct_relations = active_records
        .iter()
        .map(|record| {
            (
                record.namespace.clone(),
                record.object_id.clone(),
                record.relation.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    let schema_relations = relation_rule_rows
        .iter()
        .flat_map(|rule| {
            object_namespaces
                .iter()
                .filter(move |(namespace, _)| namespace == &rule.namespace)
                .map(move |(namespace, object_id)| {
                    (namespace.clone(), object_id.clone(), rule.relation.clone())
                })
        })
        .collect::<BTreeSet<_>>();
    direct_relations
        .into_iter()
        .chain(schema_relations)
        .map(|(namespace, object_id, relation)| UsersetRef {
            namespace,
            object_id,
            relation,
        })
        .collect()
}

fn namespace_realm_parts(namespace: &str) -> (String, String) {
    split_realm_namespace(namespace)
        .map(|(realm_id, local_namespace)| (realm_id, local_namespace.to_string()))
        .unwrap_or_else(|| (DEFAULT_AUTHZ_REALM_ID.to_string(), namespace.to_string()))
}

fn canonical_bound_namespace(realm_id: &str, namespace: &str) -> String {
    if realm_id == DEFAULT_AUTHZ_REALM_ID {
        namespace.to_string()
    } else {
        encode_realm_namespace(realm_id, namespace)
    }
}
