use anyhow::{Context, Result, ensure};
use keldra_storage::v1::permission_rule::Rule;
use keldra_storage::v1::relation_definition::Kind;
use keldra_storage::v1::subject::Kind as SubjectKind;
use keldra_storage::v1::subject_selector::Selector;
use keldra_storage::v1::tuple_mutation::Operation;
use keldra_storage::v1::{
    AnyObjectSelector, AnyUsersetSelector, DirectRelation, InheritRule, NamespaceDefinition,
    ObjectRef, Permission, PermissionCheck, PermissionRule, RelationDefinition, RelationTuple,
    Subject, SubjectSelector, TupleMutation, Userset,
};

#[derive(Clone, Debug)]
pub(super) struct ExpectedCheck {
    pub check: PermissionCheck,
    pub allowed: bool,
}

#[derive(Clone, Debug)]
pub(super) struct Graph {
    pub mutations: Vec<TupleMutation>,
    pub checks: Vec<ExpectedCheck>,
    pub churn_remove: TupleMutation,
    pub churn_restore: TupleMutation,
    pub churned_user: ObjectRef,
    pub churned_document: ObjectRef,
    pub unaffected_user: ObjectRef,
    pub unaffected_document: ObjectRef,
    pub groups: usize,
    pub users: usize,
    pub direct_edges: usize,
}

pub(super) fn schema() -> Vec<NamespaceDefinition> {
    vec![
        NamespaceDefinition {
            name: "user".into(),
            relations: vec![RelationDefinition {
                name: "marker".into(),
                kind: Some(Kind::Direct(DirectRelation {
                    allowed_subjects: vec![SubjectSelector {
                        selector: Some(Selector::AnyObject(AnyObjectSelector {
                            namespace: "user".into(),
                        })),
                    }],
                })),
            }],
        },
        NamespaceDefinition {
            name: "group".into(),
            relations: vec![RelationDefinition {
                name: "member".into(),
                kind: Some(Kind::Direct(DirectRelation {
                    allowed_subjects: vec![
                        SubjectSelector {
                            selector: Some(Selector::AnyObject(AnyObjectSelector {
                                namespace: "user".into(),
                            })),
                        },
                        SubjectSelector {
                            selector: Some(Selector::AnyUserset(AnyUsersetSelector {
                                namespace: "group".into(),
                                relation: "member".into(),
                            })),
                        },
                    ],
                })),
            }],
        },
        NamespaceDefinition {
            name: "document".into(),
            relations: vec![
                RelationDefinition {
                    name: "viewer".into(),
                    kind: Some(Kind::Direct(DirectRelation {
                        allowed_subjects: vec![SubjectSelector {
                            selector: Some(Selector::AnyUserset(AnyUsersetSelector {
                                namespace: "group".into(),
                                relation: "member".into(),
                            })),
                        }],
                    })),
                },
                RelationDefinition {
                    name: "view".into(),
                    kind: Some(Kind::Permission(Permission {
                        rules: vec![PermissionRule {
                            rule: Some(Rule::Inherit(InheritRule {
                                relation: "viewer".into(),
                            })),
                        }],
                    })),
                },
            ],
        },
    ]
}

/// Builds disjoint, balanced userset trees. Every document points to one root;
/// every leaf points to one user. Disjoint roots make churn expectations exact
/// and prevent accidental alternative membership paths from hiding stale keys.
pub(super) fn build(roots: usize, depth: usize, fanout: usize) -> Result<Graph> {
    ensure!(roots > 0 && depth > 0 && fanout > 1, "invalid graph shape");
    let mut mutations = Vec::new();
    let mut checks = Vec::new();
    let mut first_membership = None;
    let mut first_user = None;
    let mut first_document = None;
    let mut second_user = None;
    let mut second_document = None;
    let mut group_count = 0_usize;
    let mut user_count = 0_usize;

    for root in 0..roots {
        let document = object("document", format!("document-{root}"));
        let root_group = group_id(root, 0, 0);
        mutations.push(add(tuple(
            document.clone(),
            "viewer",
            userset_subject(object("group", root_group), "member"),
        )));
        if root == 0 {
            first_document = Some(document.clone());
        } else if root == 1 {
            second_document = Some(document.clone());
        }

        let mut level_width = 1_usize;
        group_count += 1;
        for level in 0..depth {
            let child_width = level_width
                .checked_mul(fanout)
                .context("group graph width overflow")?;
            for parent_offset in 0..level_width {
                let parent = object("group", group_id(root, level, parent_offset));
                for child_offset in 0..fanout {
                    let ordinal = parent_offset * fanout + child_offset;
                    let child = object("group", group_id(root, level + 1, ordinal));
                    mutations.push(add(tuple(
                        parent.clone(),
                        "member",
                        userset_subject(child, "member"),
                    )));
                }
            }
            level_width = child_width;
            group_count = group_count
                .checked_add(child_width)
                .context("group count overflow")?;
        }

        for leaf in 0..level_width {
            let group = object("group", group_id(root, depth, leaf));
            let user = object("user", format!("user-{root}-{leaf}"));
            let membership = tuple(group, "member", object_subject(user.clone()));
            mutations.push(add(membership.clone()));
            checks.push(ExpectedCheck {
                check: permission_check(user.clone(), document.clone()),
                allowed: true,
            });
            user_count += 1;
            if root == 0 && leaf == 0 {
                first_membership = Some(membership);
                first_user = Some(user);
            } else if root == 1 && leaf == 0 {
                second_user = Some(user);
            }
        }
        let outsider = object("user", format!("outsider-{root}"));
        checks.push(ExpectedCheck {
            check: permission_check(outsider, document),
            allowed: false,
        });
    }

    let first_membership = first_membership.context("graph omitted first leaf membership")?;
    let direct_edges = mutations.len();
    Ok(Graph {
        mutations,
        checks,
        churn_remove: remove(first_membership.clone()),
        churn_restore: add(first_membership),
        churned_user: first_user.context("graph omitted first user")?,
        churned_document: first_document.context("graph omitted first document")?,
        unaffected_user: second_user.unwrap_or_else(|| object("user", "user-0-1")),
        unaffected_document: second_document.unwrap_or_else(|| object("document", "document-0")),
        groups: group_count,
        users: user_count,
        direct_edges,
    })
}

pub(super) fn permission_check(subject: ObjectRef, object: ObjectRef) -> PermissionCheck {
    PermissionCheck {
        subject: Some(object_subject(subject)),
        object: Some(object),
        relation: "view".into(),
    }
}

fn tuple(object: ObjectRef, relation: &str, subject: Subject) -> RelationTuple {
    RelationTuple {
        object: Some(object),
        relation: relation.into(),
        subject: Some(subject),
    }
}

fn add(tuple: RelationTuple) -> TupleMutation {
    TupleMutation {
        operation: Some(Operation::Add(tuple)),
    }
}

fn remove(tuple: RelationTuple) -> TupleMutation {
    TupleMutation {
        operation: Some(Operation::Remove(tuple)),
    }
}

fn object(namespace: &str, opaque_id: impl Into<String>) -> ObjectRef {
    ObjectRef {
        namespace: namespace.into(),
        id: Some(keldra_storage::v1::object_ref::Id::OpaqueId(
            opaque_id.into(),
        )),
    }
}

fn object_subject(object: ObjectRef) -> Subject {
    Subject {
        kind: Some(SubjectKind::Object(object)),
    }
}

fn userset_subject(object: ObjectRef, relation: &str) -> Subject {
    Subject {
        kind: Some(SubjectKind::Userset(Userset {
            object: Some(object),
            relation: relation.into(),
        })),
    }
}

fn group_id(root: usize, level: usize, ordinal: usize) -> String {
    format!("group-{root}-{level}-{ordinal}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_is_large_nested_disjoint_and_has_positive_and_negative_oracles() {
        let graph = build(2, 3, 4).unwrap();
        assert_eq!(graph.groups, 2 * (1 + 4 + 16 + 64));
        assert_eq!(graph.users, 128);
        assert_eq!(graph.checks.len(), 130);
        assert!(graph.checks.iter().any(|check| check.allowed));
        assert!(graph.checks.iter().any(|check| !check.allowed));
        assert_eq!(graph.direct_edges, graph.mutations.len());
    }

    #[test]
    fn churn_removes_and_restores_the_exact_same_canonical_tuple() {
        let graph = build(2, 2, 2).unwrap();
        let removed = match graph.churn_remove.operation.as_ref().unwrap() {
            Operation::Remove(tuple) => tuple,
            Operation::Add(_) => panic!("churn removal added a tuple"),
        };
        let restored = match graph.churn_restore.operation.as_ref().unwrap() {
            Operation::Add(tuple) => tuple,
            Operation::Remove(_) => panic!("churn restoration removed a tuple"),
        };
        assert_eq!(removed, restored);
    }
}
