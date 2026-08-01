//! Generated types for the intentionally breaking Anvil 0.5 API.

pub mod v1 {
    tonic::include_proto!("anvil.v1");
}

#[cfg(test)]
mod tests {
    use super::v1::{DeletedObject, NeverExisted, ObjectHead, PresentObject, object_head};

    #[test]
    fn exact_path_states_are_distinct() {
        let states = [
            ObjectHead {
                state: Some(object_head::State::Present(PresentObject {
                    version: 7,
                    blob: None,
                    content_type: String::new(),
                })),
            },
            ObjectHead {
                state: Some(object_head::State::Deleted(DeletedObject { version: 8 })),
            },
            ObjectHead {
                state: Some(object_head::State::NeverExisted(NeverExisted {})),
            },
        ];

        assert!(matches!(
            &states[0].state,
            Some(object_head::State::Present(_))
        ));
        assert!(matches!(
            &states[1].state,
            Some(object_head::State::Deleted(_))
        ));
        assert!(matches!(
            &states[2].state,
            Some(object_head::State::NeverExisted(_))
        ));
    }

    #[test]
    fn schema_keeps_removed_capabilities_out() {
        let schema = include_str!("../proto/anvil.proto").to_ascii_lowercase();
        for forbidden in ["partition", "transaction", "listprefix"] {
            assert!(!schema.contains(forbidden), "schema contains `{forbidden}`");
        }

        assert!(schema.contains("executor_nomination_log_index"));
        assert!(schema.contains("commit_log_index"));
        assert!(schema.contains("immutable_path_prefixes"));
        assert!(schema.contains("program_only_path_prefixes"));
        assert!(!schema.contains("rpc registerprogram"));
        assert!(!schema.contains("message registerprogram"));
        assert!(schema.contains("objectaddress program"));
        assert!(schema.contains("_anvil/programs/{name}@{version}"));

        for rpc in [
            "rpc putschema",
            "rpc bindschema",
            "rpc getbinding",
            "rpc getschema",
            "rpc mutatetuples",
            "rpc readtuples",
            "rpc checkpermission",
            "rpc checkpermissions",
        ] {
            assert!(schema.contains(rpc), "schema is missing `{rpc}`");
        }
        for forbidden in [
            "rpc createrealm",
            "rpc deleterealm",
            "rpc applyschema",
            "zookie",
            "caveat",
            "publication_metadata",
        ] {
            assert!(!schema.contains(forbidden), "schema contains `{forbidden}`");
        }
    }

    #[test]
    fn authorization_wire_types_preserve_scope_and_typed_unions() {
        use super::v1::{
            AnyUsersetSelector, AtLeastRevision, AuthzConsistency, AuthzScope, DirectRelation,
            InheritRule, NamespaceDefinition, ObjectRef, Permission, PermissionRule,
            PutSchemaRequest, RelationDefinition, SubjectSelector, Userset, authz_consistency,
            object_ref, permission_rule, relation_definition, subject, subject_selector,
        };

        let account = ObjectRef {
            namespace: "account".into(),
            id: Some(object_ref::Id::OpaqueId("acme".into())),
        };
        let members = Userset {
            object: Some(account),
            relation: "member".into(),
        };
        let schema = NamespaceDefinition {
            name: "ledger".into(),
            relations: vec![
                RelationDefinition {
                    name: "reader".into(),
                    kind: Some(relation_definition::Kind::Direct(DirectRelation {
                        allowed_subjects: vec![SubjectSelector {
                            selector: Some(subject_selector::Selector::AnyUserset(
                                AnyUsersetSelector {
                                    namespace: "account".into(),
                                    relation: "member".into(),
                                },
                            )),
                        }],
                    })),
                },
                RelationDefinition {
                    name: "read".into(),
                    kind: Some(relation_definition::Kind::Permission(Permission {
                        rules: vec![PermissionRule {
                            rule: Some(permission_rule::Rule::Inherit(InheritRule {
                                relation: "reader".into(),
                            })),
                        }],
                    })),
                },
            ],
        };
        let subject = super::v1::Subject {
            kind: Some(subject::Kind::Userset(members)),
        };
        let publication = PutSchemaRequest {
            schema_id: "worka".into(),
            namespaces: vec![schema],
        };
        let consistency = AuthzConsistency {
            requirement: Some(authz_consistency::Requirement::AtLeast(AtLeastRevision {
                revision: 42,
            })),
        };
        let system_scope = AuthzScope {
            storage_tenant: "anvil-internal".into(),
            realm: "_anvil/system".into(),
        };

        assert_eq!(publication.namespaces[0].relations.len(), 2);
        assert!(matches!(subject.kind, Some(subject::Kind::Userset(_))));
        assert!(matches!(
            consistency.requirement,
            Some(authz_consistency::Requirement::AtLeast(AtLeastRevision {
                revision: 42
            }))
        ));
        assert_eq!(system_scope.realm, "_anvil/system");
    }

    #[test]
    fn tuple_mutation_and_batch_checks_share_request_scope() {
        use super::v1::{
            AuthzScope, CheckPermissionsRequest, MutateTuplesRequest, ObjectRef, PermissionCheck,
            RelationTuple, Subject, TupleMutation, authz_consistency, object_ref, subject,
            tuple_mutation,
        };

        let scope = AuthzScope {
            storage_tenant: "acme".into(),
            realm: "default".into(),
        };
        let ledger = ObjectRef {
            namespace: "ledger".into(),
            id: Some(object_ref::Id::OpaqueId("main".into())),
        };
        let alice = Subject {
            kind: Some(subject::Kind::Object(ObjectRef {
                namespace: "user".into(),
                id: Some(object_ref::Id::OpaqueId("alice".into())),
            })),
        };
        let tuple = RelationTuple {
            object: Some(ledger.clone()),
            relation: "reader".into(),
            subject: Some(alice.clone()),
        };
        let mutation = MutateTuplesRequest {
            scope: Some(scope.clone()),
            operation_id: "grant-alice".into(),
            expected_revision: Some(41),
            mutations: vec![TupleMutation {
                operation: Some(tuple_mutation::Operation::Add(tuple)),
            }],
        };
        let checks = CheckPermissionsRequest {
            scope: Some(scope),
            checks: vec![PermissionCheck {
                subject: Some(alice),
                object: Some(ledger),
                relation: "read".into(),
            }],
            consistency: Some(super::v1::AuthzConsistency {
                requirement: Some(authz_consistency::Requirement::Exact(
                    super::v1::ExactRevision { revision: 42 },
                )),
            }),
        };

        assert_eq!(mutation.expected_revision, Some(41));
        assert_eq!(mutation.mutations.len(), 1);
        assert_eq!(checks.checks.len(), 1);
        assert!(matches!(
            checks
                .consistency
                .and_then(|consistency| consistency.requirement),
            Some(authz_consistency::Requirement::Exact(_))
        ));
    }
}
