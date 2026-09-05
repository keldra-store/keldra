//! Generated Rust types for the Keldra 0.17 gRPC API.

pub mod v1 {
    tonic::include_proto!("keldra.v1");
}

/// A Boolean predicate expression rejected before it is sent to Keldra.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PredicateExpressionError {
    EmptyConjunction,
    EmptyDisjunction,
}

impl std::fmt::Display for PredicateExpressionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyConjunction => {
                formatter.write_str("a predicate conjunction requires at least one child")
            }
            Self::EmptyDisjunction => {
                formatter.write_str("a predicate disjunction requires at least one child")
            }
        }
    }
}

impl std::error::Error for PredicateExpressionError {}

impl v1::IndexPredicateExpression {
    /// Construct one leaf expression.
    pub fn leaf(predicate: v1::IndexPredicate) -> Self {
        Self {
            expression: Some(v1::index_predicate_expression::Expression::Predicate(
                predicate,
            )),
        }
    }

    /// Construct a non-empty conjunction.
    pub fn all(
        expressions: impl IntoIterator<Item = Self>,
    ) -> Result<Self, PredicateExpressionError> {
        let expressions = expressions.into_iter().collect::<Vec<_>>();
        if expressions.is_empty() {
            return Err(PredicateExpressionError::EmptyConjunction);
        }
        Ok(Self {
            expression: Some(v1::index_predicate_expression::Expression::Conjunction(
                v1::IndexPredicateConjunction { expressions },
            )),
        })
    }

    /// Construct a non-empty disjunction.
    pub fn any(
        expressions: impl IntoIterator<Item = Self>,
    ) -> Result<Self, PredicateExpressionError> {
        let expressions = expressions.into_iter().collect::<Vec<_>>();
        if expressions.is_empty() {
            return Err(PredicateExpressionError::EmptyDisjunction);
        }
        Ok(Self {
            expression: Some(v1::index_predicate_expression::Expression::Disjunction(
                v1::IndexPredicateDisjunction { expressions },
            )),
        })
    }

    /// Negate this complete expression.
    pub fn negated(self) -> Self {
        Self {
            expression: Some(v1::index_predicate_expression::Expression::Negation(
                Box::new(v1::IndexPredicateNegation {
                    expression: Some(Box::new(self)),
                }),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::v1::{
        CloneObjectRequest, DeletedObject, Durability, LinkObjectRequest, NeverExisted,
        ObjectAddress, ObjectHead, PresentObject, PutIfVersionOperation, UnlinkObjectRequest,
        clone_object_request, object_head,
    };

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
    fn clone_object_wire_round_trip_preserves_both_identities_and_exact_cas() {
        let request = CloneObjectRequest {
            source: Some(ObjectAddress {
                tenant: "tenant".into(),
                bucket: "bucket".into(),
                path: "source".into(),
            }),
            source_version: 17,
            destination: Some(ObjectAddress {
                tenant: "tenant".into(),
                bucket: "bucket".into(),
                path: "destination".into(),
            }),
            command_id: "clone-17".into(),
            durability: Durability::Replicated as i32,
            operation: Some(clone_object_request::Operation::PutIfVersion(
                PutIfVersionOperation {
                    expected_version: 11,
                },
            )),
        };

        let decoded = CloneObjectRequest::decode(request.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn object_link_wire_contract_keeps_link_and_target_distinct() {
        let link = ObjectAddress {
            tenant: "tenant".into(),
            bucket: "bucket".into(),
            path: "alias".into(),
        };
        let request = LinkObjectRequest {
            link: Some(link.clone()),
            target: Some(ObjectAddress {
                tenant: "tenant".into(),
                bucket: "bucket".into(),
                path: "target".into(),
            }),
            command_id: "link-1".into(),
            durability: Durability::Replicated as i32,
        };
        assert_eq!(
            LinkObjectRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
            request
        );
        let unlink = UnlinkObjectRequest {
            link: Some(link),
            command_id: "unlink-1".into(),
            durability: Durability::Local as i32,
        };
        assert_eq!(
            UnlinkObjectRequest::decode(unlink.encode_to_vec().as_slice()).unwrap(),
            unlink
        );
    }

    #[test]
    fn schema_keeps_removed_capabilities_out() {
        let schema = include_str!("../proto/keldra.proto").to_ascii_lowercase();
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
        assert!(schema.contains("_keldra/programs/{name}@{version}"));
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
            "rpc cloneobject",
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

        let date = super::v1::DateIndexField {
            strftime_pattern: "%Y-%m-%d".into(),
        };
        assert!(matches!(
            index_field::FieldType::Date(date),
            index_field::FieldType::Date(value) if value.strftime_pattern == "%Y-%m-%d"
        ));

        let schema = include_str!("../proto/keldra.proto").to_ascii_lowercase();
        assert!(schema.contains("rpc listindices(listindicesrequest)"));
        assert!(schema.contains("repeated indexdefinition indices = 1"));
        assert!(!schema.contains("listindexes"));
        assert!(!schema.contains("fields_json"));
        assert!(!schema.contains("bool multi_valued"));
    }

    #[test]
    fn typed_json_queries_expose_fielded_text_facets_and_aggregates() {
        use super::v1::index_predicate_expression::Expression;
        use super::v1::{
            IndexAggregateOperation, IndexAggregateRequest, IndexAggregateResult, IndexFacetBucket,
            IndexFacetRequest, IndexFacetResult, IndexPredicate, IndexPredicateConjunction,
            IndexPredicateExpression, IndexPredicateOperator, IndexQueryHit, QueryIndexResponse,
            TypedJsonIndexQuery,
        };

        let query = TypedJsonIndexQuery {
            predicate: Some(IndexPredicateExpression {
                expression: Some(Expression::Conjunction(IndexPredicateConjunction {
                    expressions: [
                        IndexPredicateOperator::FullText,
                        IndexPredicateOperator::Phrase,
                    ]
                    .into_iter()
                    .map(|operator| IndexPredicateExpression {
                        expression: Some(Expression::Predicate(IndexPredicate {
                            field: "summary".into(),
                            operator: operator as i32,
                            values_json: vec![br#""memory safety""#.to_vec()],
                        })),
                    })
                    .collect(),
                })),
            }),
            order: Vec::new(),
            facets: vec![IndexFacetRequest {
                field: "ecosystem".into(),
                limit: 10,
            }],
            aggregates: vec![IndexAggregateRequest {
                field: "severity".into(),
                operation: IndexAggregateOperation::Average as i32,
            }],
        };
        let response = QueryIndexResponse {
            hits: vec![IndexQueryHit {
                address: None,
                object_version: 7,
                score: Some(0.75),
            }],
            next_page_token: Vec::new(),
            freshness: None,
            facet_results: vec![IndexFacetResult {
                field: "ecosystem".into(),
                buckets: vec![IndexFacetBucket {
                    value_json: br#""cargo""#.to_vec(),
                    count: 4,
                }],
            }],
            aggregate_results: vec![IndexAggregateResult {
                field: "severity".into(),
                operation: IndexAggregateOperation::Average as i32,
                value_json: Some(b"7.5".to_vec()),
                contributing_count: 4,
            }],
        };

        assert_eq!(query.facets[0].limit, 10);
        assert!(matches!(
            query.predicate.unwrap().expression,
            Some(Expression::Conjunction(_))
        ));
        assert_eq!(response.hits[0].object_version, 7);
        assert_eq!(response.aggregate_results[0].contributing_count, 4);

        let schema = include_str!("../proto/keldra.proto").to_ascii_lowercase();
        assert!(!schema.contains("repeated indexpredicate predicates"));
        assert!(schema.contains("indexpredicateexpression predicate = 5"));
        assert!(schema.contains("indexpredicateexpression predicate = 2"));
    }

    #[test]
    fn predicate_expression_helpers_reject_empty_boolean_operators() {
        use super::PredicateExpressionError;
        use super::v1::index_predicate_expression::Expression;
        use super::v1::{IndexPredicate, IndexPredicateExpression, IndexPredicateOperator};

        let leaf = IndexPredicateExpression::leaf(IndexPredicate {
            field: "status".into(),
            operator: IndexPredicateOperator::Exists as i32,
            values_json: Vec::new(),
        });
        assert!(matches!(
            IndexPredicateExpression::all([leaf.clone()])
                .unwrap()
                .expression,
            Some(Expression::Conjunction(_))
        ));
        assert!(matches!(
            IndexPredicateExpression::any([leaf.clone()])
                .unwrap()
                .expression,
            Some(Expression::Disjunction(_))
        ));
        assert!(matches!(
            leaf.negated().expression,
            Some(Expression::Negation(_))
        ));
        assert_eq!(
            IndexPredicateExpression::all(Vec::new()).unwrap_err(),
            PredicateExpressionError::EmptyConjunction
        );
        assert_eq!(
            IndexPredicateExpression::any(Vec::new()).unwrap_err(),
            PredicateExpressionError::EmptyDisjunction
        );
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
            storage_tenant: "keldra-internal".into(),
            realm: "_keldra/system".into(),
        };

        assert_eq!(publication.namespaces[0].relations.len(), 2);
        assert!(matches!(subject.kind, Some(subject::Kind::Userset(_))));
        assert!(matches!(
            consistency.requirement,
            Some(authz_consistency::Requirement::AtLeast(AtLeastRevision {
                revision: 42
            }))
        ));
        assert_eq!(system_scope.realm, "_keldra/system");
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
