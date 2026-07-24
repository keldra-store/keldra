use super::{
    projection,
    resolver::{SubjectRef, UsersetRef},
};
use crate::anvil_api::{AuthzNamespaceSchema, AuthzRelationRule};
use crate::authz_head;
use crate::authz_realm_schema;
use crate::authz_schema_contract;
use crate::authz_scope::{
    DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace, parse_userset_subject, split_realm_namespace,
};
use crate::persistence::AuthzTupleRecord;
use crate::storage::Storage;
use anyhow::{Result, anyhow, bail};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const MAX_AUTHZ_GRAPH_DEPTH: usize = 64;
const MAX_AUTHZ_GRAPH_NODES: usize = 4_096;
const MAX_AUTHZ_PROJECTION_ROWS_VISITED: usize = 16_384;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AuthzResolutionStats {
    pub projection_rows_visited: usize,
    pub graph_nodes_visited: usize,
    pub schema_point_reads: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthzResolutionOutcome {
    pub allowed: bool,
    pub stats: AuthzResolutionStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthzSubjectCollectionOutcome {
    pub subjects: BTreeSet<SubjectRef>,
    pub stats: AuthzResolutionStats,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RelationQueryKey {
    userset: UsersetRef,
    subject_kind: Option<String>,
}

#[derive(Debug, Clone)]
struct SchemaMember {
    direct_relation: bool,
    rules: Vec<AuthzRelationRule>,
}

#[derive(Debug, Clone, Default)]
struct BoundNamespace {
    members: BTreeMap<String, SchemaMember>,
}

struct CurrentResolver<'a> {
    storage: &'a Storage,
    tenant_id: i64,
    subject: SubjectRef,
    schemas: BTreeMap<String, Option<BoundNamespace>>,
    relation_rows: BTreeMap<RelationQueryKey, Vec<AuthzTupleRecord>>,
    stats: AuthzResolutionStats,
}

pub(crate) async fn resolve_at_current_revision(
    storage: &Storage,
    tenant_id: i64,
    userset: UsersetRef,
    subject: SubjectRef,
    expected_revision: i64,
) -> Result<AuthzResolutionOutcome> {
    require_revision(storage, tenant_id, expected_revision).await?;
    let mut resolver = CurrentResolver {
        storage,
        tenant_id,
        subject,
        schemas: BTreeMap::new(),
        relation_rows: BTreeMap::new(),
        stats: AuthzResolutionStats::default(),
    };
    let allowed = resolver.resolve(userset).await?;
    require_revision(storage, tenant_id, expected_revision).await?;
    Ok(AuthzResolutionOutcome {
        allowed,
        stats: resolver.stats,
    })
}

pub(crate) async fn collect_subjects_at_current_revision(
    storage: &Storage,
    tenant_id: i64,
    userset: UsersetRef,
    expected_revision: i64,
) -> Result<AuthzSubjectCollectionOutcome> {
    require_revision(storage, tenant_id, expected_revision).await?;
    let mut resolver = CurrentResolver {
        storage,
        tenant_id,
        subject: SubjectRef {
            kind: String::new(),
            id: String::new(),
            caveat_hash: String::new(),
        },
        schemas: BTreeMap::new(),
        relation_rows: BTreeMap::new(),
        stats: AuthzResolutionStats::default(),
    };
    let subjects = resolver.collect_subjects(userset).await?;
    require_revision(storage, tenant_id, expected_revision).await?;
    Ok(AuthzSubjectCollectionOutcome {
        subjects,
        stats: resolver.stats,
    })
}

impl CurrentResolver<'_> {
    async fn resolve(&mut self, root: UsersetRef) -> Result<bool> {
        let mut queue = VecDeque::from([(root.clone(), 0_usize)]);
        let mut scheduled = BTreeSet::from([root]);

        while let Some((userset, depth)) = queue.pop_front() {
            if depth > MAX_AUTHZ_GRAPH_DEPTH {
                bail!(
                    "AuthzGraphDepthExceeded: authorization graph exceeds depth {MAX_AUTHZ_GRAPH_DEPTH}"
                );
            }
            self.stats.graph_nodes_visited += 1;
            if self.stats.graph_nodes_visited > MAX_AUTHZ_GRAPH_NODES {
                bail!(
                    "AuthzGraphNodeLimitExceeded: authorization graph exceeds {MAX_AUTHZ_GRAPH_NODES} nodes"
                );
            }

            let member = self.schema_member(&userset).await?;
            if member.direct_relation {
                self.record_projection_visits(1)?;
                if projection::read_current_record(
                    self.storage,
                    self.tenant_id,
                    &userset.namespace,
                    &userset.object_id,
                    &userset.relation,
                    &self.subject.kind,
                    &self.subject.id,
                    &self.subject.caveat_hash,
                )
                .await?
                .is_some()
                {
                    return Ok(true);
                }

                let records = self.relation_rows(&userset, Some("userset")).await?;
                for record in records {
                    if record.caveat_hash.is_empty()
                        && let Some(next) = userset_from_userset_subject(
                            &record.subject_id,
                            &userset.namespace,
                            None,
                        )?
                    {
                        schedule_userset(&mut queue, &mut scheduled, next, depth + 1)?;
                    }
                }
            }

            for rule in member.rules {
                match rule.kind.as_str() {
                    "inherit" => {
                        schedule_userset(
                            &mut queue,
                            &mut scheduled,
                            UsersetRef {
                                namespace: userset.namespace.clone(),
                                object_id: userset.object_id.clone(),
                                relation: rule.relation,
                            },
                            depth + 1,
                        )?;
                    }
                    "computed" | "tuple_to_userset" => {
                        let edge_userset =
                            userset_with_relation(&userset, rule.tuple_relation.clone());
                        let records = self.relation_rows(&edge_userset, None).await?;
                        for record in records {
                            if !record.caveat_hash.is_empty() {
                                continue;
                            }
                            let Some(next) = userset_from_relation_edge(
                                &record,
                                &userset.namespace,
                                &rule.target_relation,
                            )?
                            else {
                                continue;
                            };
                            schedule_userset(&mut queue, &mut scheduled, next, depth + 1)?;
                        }
                    }
                    kind => return Err(anyhow!("unsupported authorization rule kind {kind}")),
                }
            }
        }
        Ok(false)
    }

    async fn collect_subjects(&mut self, root: UsersetRef) -> Result<BTreeSet<SubjectRef>> {
        let mut queue = VecDeque::from([(root.clone(), 0_usize)]);
        let mut scheduled = BTreeSet::from([root]);
        let mut subjects = BTreeSet::new();

        while let Some((userset, depth)) = queue.pop_front() {
            if depth > MAX_AUTHZ_GRAPH_DEPTH {
                bail!(
                    "AuthzGraphDepthExceeded: authorization graph exceeds depth {MAX_AUTHZ_GRAPH_DEPTH}"
                );
            }
            self.stats.graph_nodes_visited += 1;
            if self.stats.graph_nodes_visited > MAX_AUTHZ_GRAPH_NODES {
                bail!(
                    "AuthzGraphNodeLimitExceeded: authorization graph exceeds {MAX_AUTHZ_GRAPH_NODES} nodes"
                );
            }

            let member = self.schema_member(&userset).await?;
            if member.direct_relation {
                for record in self.relation_rows(&userset, None).await? {
                    if record.subject_kind == "userset" {
                        if record.caveat_hash.is_empty()
                            && let Some(next) = userset_from_userset_subject(
                                &record.subject_id,
                                &userset.namespace,
                                None,
                            )?
                        {
                            schedule_userset(&mut queue, &mut scheduled, next, depth + 1)?;
                        }
                    } else {
                        subjects.insert(SubjectRef {
                            kind: record.subject_kind,
                            id: record.subject_id,
                            caveat_hash: record.caveat_hash,
                        });
                    }
                }
            }

            for rule in member.rules {
                match rule.kind.as_str() {
                    "inherit" => {
                        schedule_userset(
                            &mut queue,
                            &mut scheduled,
                            UsersetRef {
                                namespace: userset.namespace.clone(),
                                object_id: userset.object_id.clone(),
                                relation: rule.relation,
                            },
                            depth + 1,
                        )?;
                    }
                    "computed" | "tuple_to_userset" => {
                        let edge_userset =
                            userset_with_relation(&userset, rule.tuple_relation.clone());
                        for record in self.relation_rows(&edge_userset, None).await? {
                            if !record.caveat_hash.is_empty() {
                                continue;
                            }
                            let Some(next) = userset_from_relation_edge(
                                &record,
                                &userset.namespace,
                                &rule.target_relation,
                            )?
                            else {
                                continue;
                            };
                            schedule_userset(&mut queue, &mut scheduled, next, depth + 1)?;
                        }
                    }
                    kind => return Err(anyhow!("unsupported authorization rule kind {kind}")),
                }
            }
        }
        Ok(subjects)
    }

    async fn schema_member(&mut self, userset: &UsersetRef) -> Result<SchemaMember> {
        if !self.schemas.contains_key(&userset.namespace) {
            let (realm_id, namespace) = namespace_realm_parts(&userset.namespace);
            self.stats.schema_point_reads += 1;
            let schema = authz_realm_schema::read_bound_namespace_schema(
                self.storage,
                self.tenant_id,
                &realm_id,
                &namespace,
            )
            .await?;
            self.schemas.insert(
                userset.namespace.clone(),
                schema.map(bound_namespace_from_schema),
            );
        }

        let Some(namespace) = self.schemas.get(&userset.namespace) else {
            unreachable!("authorization namespace cache was populated")
        };
        let Some(namespace) = namespace else {
            return Ok(SchemaMember {
                direct_relation: true,
                rules: Vec::new(),
            });
        };
        Ok(namespace
            .members
            .get(&userset.relation)
            .cloned()
            .unwrap_or(SchemaMember {
                direct_relation: false,
                rules: Vec::new(),
            }))
    }

    async fn relation_rows(
        &mut self,
        userset: &UsersetRef,
        subject_kind: Option<&str>,
    ) -> Result<Vec<AuthzTupleRecord>> {
        let key = RelationQueryKey {
            userset: userset.clone(),
            subject_kind: subject_kind.map(str::to_string),
        };
        if let Some(records) = self.relation_rows.get(&key) {
            return Ok(records.clone());
        }
        let rows = projection::read_current_relation_rows(
            self.storage,
            self.tenant_id,
            &userset.namespace,
            &userset.object_id,
            &userset.relation,
            subject_kind,
        )
        .await?;
        self.record_projection_visits(rows.candidates_visited)?;
        self.relation_rows.insert(key, rows.records.clone());
        Ok(rows.records)
    }

    fn record_projection_visits(&mut self, count: usize) -> Result<()> {
        self.stats.projection_rows_visited = self
            .stats
            .projection_rows_visited
            .checked_add(count)
            .ok_or_else(|| anyhow!("authorization projection visit count overflow"))?;
        if self.stats.projection_rows_visited > MAX_AUTHZ_PROJECTION_ROWS_VISITED {
            bail!(
                "AuthzGraphBreadthExceeded: authorization graph visits more than {MAX_AUTHZ_PROJECTION_ROWS_VISITED} tuple rows"
            );
        }
        Ok(())
    }
}

async fn require_revision(storage: &Storage, tenant_id: i64, expected_revision: i64) -> Result<()> {
    let actual = i64::try_from(
        authz_head::read(storage, tenant_id)
            .await?
            .head
            .committed_revision,
    )?;
    if actual != expected_revision {
        bail!(
            "AuthzRevisionUnavailable: current authorization revision is {actual}, requested {expected_revision}"
        );
    }
    Ok(())
}

fn bound_namespace_from_schema(schema: AuthzNamespaceSchema) -> BoundNamespace {
    BoundNamespace {
        members: schema
            .relations
            .into_iter()
            .map(|relation| {
                let direct_relation = authz_schema_contract::is_direct_relation(&relation);
                let relation_name = relation.relation.clone();
                (
                    relation_name,
                    SchemaMember {
                        direct_relation,
                        rules: relation.rules,
                    },
                )
            })
            .collect(),
    }
}

fn schedule_userset(
    queue: &mut VecDeque<(UsersetRef, usize)>,
    scheduled: &mut BTreeSet<UsersetRef>,
    userset: UsersetRef,
    depth: usize,
) -> Result<()> {
    if depth > MAX_AUTHZ_GRAPH_DEPTH {
        bail!("AuthzGraphDepthExceeded: authorization graph exceeds depth {MAX_AUTHZ_GRAPH_DEPTH}");
    }
    if scheduled.insert(userset.clone()) {
        if scheduled.len() > MAX_AUTHZ_GRAPH_NODES {
            bail!(
                "AuthzGraphNodeLimitExceeded: authorization graph exceeds {MAX_AUTHZ_GRAPH_NODES} nodes"
            );
        }
        queue.push_back((userset, depth));
    }
    Ok(())
}

fn userset_with_relation(userset: &UsersetRef, relation: String) -> UsersetRef {
    UsersetRef {
        namespace: userset.namespace.clone(),
        object_id: userset.object_id.clone(),
        relation,
    }
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
        bail!("invalid userset subject reference");
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
        .map(|(realm, local)| (realm, local.to_string()))
        .unwrap_or_else(|| (DEFAULT_AUTHZ_REALM_ID.to_string(), namespace.to_string()))
}
