//! Generated Rust types for the Anvil 0.9 gRPC API.

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
                    content_hash: vec![7; 32],
                    content_length: 99,
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
        for forbidden in [
            "rpc uploadblob",
            "rpc publishobject",
            "rpc putobject",
            "message blobref",
            "rpc listprefix",
            "rpc begintransaction",
            "rpc committransaction",
            "personaldb",
        ] {
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
        assert!(schema.contains("rpc startput(putheader) returns (puttoken)"));
        assert!(schema.contains("rpc put(stream putrequest) returns (puttoken)"));
        assert!(schema.contains("rpc putend(puttoken) returns (mutationreceipt)"));
        for rpc in [
            "rpc createindex(createindexrequest)",
            "rpc updateindex(updateindexrequest)",
            "rpc getindex(getindexrequest)",
            "rpc listindices(listindicesrequest)",
            "rpc deleteindex(deleteindexrequest)",
            "rpc queryindex(queryindexrequest)",
        ] {
            assert!(schema.contains(rpc), "schema is missing `{rpc}`");
        }
        assert!(schema.contains("index_kind_tensor"));
        assert!(schema.contains("tensorindexspec tensor"));
        assert!(schema.contains("tensorindexquery tensor"));

        for rpc in [
            "rpc exchangeclientcredentials",
            "rpc provisiontenant",
            "rpc createapplication",
            "rpc rotateapplicationcredential",
            "rpc disableapplicationcredential",
            "rpc createbucket",
            "rpc grantapplicationrole",
            "rpc revokeapplicationrole",
            "rpc putschema",
            "rpc bindschema",
            "rpc getbinding",
            "rpc getschema",
            "rpc mutatetuples",
            "rpc readtuples",
            "rpc checkpermission",
            "rpc checkpermissions",
            "rpc watchprefix",
            "rpc listobjects",
            "rpc deleteversion",
            "rpc listobjectversions",
            "rpc setbucketversioning",
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
            "insecure_no_auth",
            "api_token",
        ] {
            assert!(!schema.contains(forbidden), "schema contains `{forbidden}`");
        }
    }

    #[test]
    fn generated_index_client_is_publicly_exposed() {
        let _: Option<
            super::v1::index_service_client::IndexServiceClient<tonic::transport::Channel>,
        > = None;
    }

    #[test]
    fn typed_json_fields_have_explicit_type_cardinality_and_capabilities() {
        use super::v1::{
            IndexField, IndexFieldCapability, IndexFieldCardinality, KeywordIndexField, index_field,
        };

        let field = IndexField {
            name: "document_id".into(),
            json_pointer: "/id".into(),
            cardinality: IndexFieldCardinality::Single as i32,
            capabilities: vec![IndexFieldCapability::Exact as i32],
            field_type: Some(index_field::FieldType::Keyword(KeywordIndexField {})),
        };

        assert!(matches!(
            field.field_type,
            Some(index_field::FieldType::Keyword(_))
        ));
        assert_eq!(field.capabilities, [IndexFieldCapability::Exact as i32]);

        let schema = include_str!("../proto/anvil.proto").to_ascii_lowercase();
        assert!(schema.contains("rpc listindices(listindicesrequest)"));
        assert!(schema.contains("repeated indexdefinition indices = 1"));
        assert!(!schema.contains("listindexes"));
        assert!(!schema.contains("fields_json"));
        assert!(!schema.contains("bool multi_valued"));
    }

    #[test]
    fn generated_personaldb_client_is_publicly_exposed() {
        let _: Option<
            super::v1::personal_db_service_client::PersonalDbServiceClient<
                tonic::transport::Channel,
            >,
        > = None;

        let schema = include_str!("../proto/personaldb.proto").to_ascii_lowercase();
        for rpc in [
            "rpc creategroup(",
            "rpc describegroup(",
            "rpc listgroups(",
            "rpc grantgrouprole(",
            "rpc revokegrouprole(",
            "rpc appendentry(",
            "rpc materializeprojection(",
            "rpc catchup(",
            "rpc registersnapshot(",
            "rpc getsnapshot(",
        ] {
            assert!(schema.contains(rpc), "PersonalDB schema is missing `{rpc}`");
        }
    }

    #[test]
    fn object_surface_has_only_explicit_typed_mutations() {
        use super::v1::{
            BulkOperation, BulkPutIfVersionRequest, CreateBucketRequest, DeleteIfVersionRequest,
            DeleteRequest, DeleteVersionRequest, DeleteVersionResponse, Durability,
            ListObjectsRequest, ListObjectsResponse, ObjectAddress, ObjectVersioning, PutHeader,
            PutIfVersionOperation, PutRequest, PutToken, bulk_operation, put_header,
        };

        let address = Some(ObjectAddress {
            tenant: "acme".into(),
            bucket: "objects".into(),
            path: "one".into(),
        });
        let header = PutHeader {
            address: address.clone(),
            content_type: "application/json".into(),
            command_id: "command-1".into(),
            durability: Durability::Local as i32,
            operation: Some(put_header::Operation::PutIfVersion(PutIfVersionOperation {
                expected_version: 8,
            })),
        };
        let frame = PutRequest {
            token: Some(PutToken {
                value: b"opaque".to_vec(),
                expires_at: None,
            }),
            chunk: Vec::new(),
        };
        assert!(matches!(
            header.operation,
            Some(put_header::Operation::PutIfVersion(_))
        ));
        assert!(frame.chunk.is_empty());

        let operations = [
            bulk_operation::Operation::Put(Default::default()),
            bulk_operation::Operation::PutIfAbsent(Default::default()),
            bulk_operation::Operation::PutIfVersion(BulkPutIfVersionRequest::default()),
            bulk_operation::Operation::PutImmutable(Default::default()),
            bulk_operation::Operation::Delete(DeleteRequest {
                address: address.clone(),
                ..Default::default()
            }),
            bulk_operation::Operation::DeleteIfVersion(DeleteIfVersionRequest {
                address: address.clone(),
                expected_version: 8,
                ..Default::default()
            }),
        ];
        assert_eq!(
            operations
                .into_iter()
                .map(|operation| BulkOperation {
                    operation: Some(operation),
                })
                .count(),
            6
        );

        let current_head_delete = DeleteIfVersionRequest {
            address: address.clone(),
            expected_version: 8,
            ..Default::default()
        };
        let retained_version_delete = DeleteVersionRequest {
            address,
            version: 7,
            ..Default::default()
        };
        assert_eq!(current_head_delete.expected_version, 8);
        assert_eq!(retained_version_delete.version, 7);
        let replaced_current = DeleteVersionResponse {
            deleted: true,
            replacement_tombstone_version: Some(9),
        };
        assert_eq!(replaced_current.replacement_tombstone_version, Some(9));
        assert_eq!(
            CreateBucketRequest::default().versioning,
            ObjectVersioning::Unversioned as i32
        );

        let list_request = ListObjectsRequest {
            tenant: "acme".into(),
            bucket: "objects".into(),
            prefix: "reports/".into(),
            start_after: Some("reports/2025.json".into()),
            limit: 100,
        };
        let list_response = ListObjectsResponse {
            paths: vec!["reports/2026.json".into()],
            has_more: false,
        };
        assert_eq!(
            list_request.start_after.as_deref(),
            Some("reports/2025.json")
        );
        assert_eq!(list_response.paths, vec!["reports/2026.json".to_owned()]);
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
            schema_id: "acme".into(),
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
