//! Fail-closed conversion at the public authorization API boundary.
//!
//! Authentication is deliberately outside this module. Public scope parsing
//! accepts a trusted tenant established by the caller and only compares the
//! request with it; request fields never become identity.

#![allow(dead_code)]

use anvil_api::v1 as api;
use anvil_authz::{
    AllowedSubject, AuthorizationCheck, AuthorizationError, AuthorizationLimits, ExactPath,
    NamespaceDefinition, ObjectId, ObjectRef, RealmId, RelationDefinition, RelationKind,
    RewriteRule, Schema, Tuple, TupleSubject, UsersetRef,
};
use anvil_store::{
    AuthzConsistency, AuthzRevision, AuthzScope as ScopedRealm, AuthzStoreError, SchemaDigest,
    SchemaId, SchemaRef, StorageTenantId,
};
use tonic::Status;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DomainTupleMutation {
    Add(Tuple),
    Remove(Tuple),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DomainObjectFilter {
    Namespace(String),
    Exact(ObjectRef),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DomainTupleFilter {
    pub(crate) object: Option<DomainObjectFilter>,
    pub(crate) relation: Option<String>,
    pub(crate) subject: Option<TupleSubject>,
}

/// Parses a third-party scope after comparing its tenant with trusted caller
/// state. The protected system realm is intentionally representable on the
/// wire but is never admitted through this public conversion path.
pub(crate) fn public_scope_from_api(
    value: Option<api::AuthzScope>,
    trusted_storage_tenant: &str,
) -> Result<ScopedRealm, Status> {
    let value = required(value, "authorization scope")?;
    require_caller_tenant(&value, trusted_storage_tenant)?;
    let (storage_tenant, realm) = parse_scope_parts(value)?;
    if storage_tenant.is_system()
        || realm.is_system()
        || realm.as_str() == anvil_authz::PERSONALDB_REALM_ID
    {
        return Err(Status::permission_denied(
            "the protected authorization scope is not public",
        ));
    }
    ScopedRealm::new(storage_tenant, realm).map_err(store_input_status)
}

/// Parses an internally supplied scope. Unlike [`public_scope_from_api`], this
/// permits the one canonical protected system scope.
pub(crate) fn internal_scope_from_api(
    value: Option<api::AuthzScope>,
) -> Result<ScopedRealm, Status> {
    parse_scope(required(value, "authorization scope")?)
}

/// Compares request scope with identity established by authentication. This
/// helper does not derive or replace trusted identity from request data.
pub(crate) fn require_caller_tenant(
    scope: &api::AuthzScope,
    trusted_storage_tenant: &str,
) -> Result<(), Status> {
    if scope.storage_tenant != trusted_storage_tenant {
        return Err(Status::permission_denied(
            "authorization scope does not belong to the authenticated tenant",
        ));
    }
    Ok(())
}

pub(crate) fn scope_to_api(value: &ScopedRealm) -> api::AuthzScope {
    api::AuthzScope {
        storage_tenant: value.storage_tenant.to_string(),
        realm: value.realm.to_string(),
    }
}

pub(crate) fn object_from_api(value: Option<api::ObjectRef>) -> Result<ObjectRef, Status> {
    let value = required(value, "object reference")?;
    match required(value.id, "object ID")? {
        api::object_ref::Id::OpaqueId(id) => ObjectRef::opaque(value.namespace, id),
        api::object_ref::Id::ExactPath(path) => ObjectRef::exact_path(
            value.namespace,
            ExactPath::new(path.tenant, path.bucket, path.path).map_err(authz_status)?,
        ),
    }
    .map_err(authz_status)
}

pub(crate) fn object_to_api(value: &ObjectRef) -> api::ObjectRef {
    let id = match &value.id {
        ObjectId::Opaque(id) => api::object_ref::Id::OpaqueId(id.clone()),
        ObjectId::ExactPath(path) => api::object_ref::Id::ExactPath(api::ObjectAddress {
            tenant: path.tenant.clone(),
            bucket: path.bucket.clone(),
            path: path.path.clone(),
        }),
    };
    api::ObjectRef {
        namespace: value.namespace.clone(),
        id: Some(id),
    }
}

pub(crate) fn subject_from_api(value: Option<api::Subject>) -> Result<TupleSubject, Status> {
    match required(required(value, "subject")?.kind, "subject kind")? {
        api::subject::Kind::Object(object) => {
            object_from_api(Some(object)).map(TupleSubject::Object)
        }
        api::subject::Kind::Userset(userset) => {
            userset_from_api(userset).map(TupleSubject::Userset)
        }
    }
}

pub(crate) fn subject_to_api(value: &TupleSubject) -> api::Subject {
    let kind = match value {
        TupleSubject::Object(object) => api::subject::Kind::Object(object_to_api(object)),
        TupleSubject::Userset(userset) => api::subject::Kind::Userset(userset_to_api(userset)),
    };
    api::Subject { kind: Some(kind) }
}

pub(crate) fn userset_from_api(value: api::Userset) -> Result<UsersetRef, Status> {
    UsersetRef::new(object_from_api(value.object)?, value.relation).map_err(authz_status)
}

pub(crate) fn userset_to_api(value: &UsersetRef) -> api::Userset {
    api::Userset {
        object: Some(object_to_api(&value.object)),
        relation: value.relation.clone(),
    }
}

pub(crate) fn schema_from_api(
    namespaces: Vec<api::NamespaceDefinition>,
    limits: AuthorizationLimits,
) -> Result<Schema, Status> {
    let namespaces = namespaces
        .into_iter()
        .map(namespace_from_api)
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Schema::new(namespaces);
    schema.validate(limits).map_err(authz_status)?;
    Ok(schema)
}

pub(crate) fn schema_to_api(value: &Schema) -> Vec<api::NamespaceDefinition> {
    value.namespaces.iter().map(namespace_to_api).collect()
}

pub(crate) fn schema_ref_from_api(value: Option<api::SchemaRef>) -> Result<SchemaRef, Status> {
    let value = required(value, "schema reference")?;
    let schema_digest: [u8; 32] = value
        .schema_digest
        .try_into()
        .map_err(|_| Status::invalid_argument("schema digest must contain exactly 32 bytes"))?;
    let schema_ref = SchemaRef {
        schema_id: SchemaId::parse(value.schema_id).map_err(store_input_status)?,
        schema_revision: value.schema_revision,
        schema_digest: SchemaDigest(schema_digest),
    };
    if schema_ref.schema_revision == 0 {
        return Err(Status::invalid_argument("schema revision must be nonzero"));
    }
    Ok(schema_ref)
}

pub(crate) fn schema_ref_to_api(value: &SchemaRef) -> api::SchemaRef {
    api::SchemaRef {
        schema_id: value.schema_id.to_string(),
        schema_revision: value.schema_revision,
        schema_digest: value.schema_digest.0.to_vec(),
    }
}

pub(crate) fn tuple_from_api(value: api::RelationTuple) -> Result<Tuple, Status> {
    let object = object_from_api(value.object)?;
    let subject = subject_from_api(value.subject)?;
    UsersetRef::new(object.clone(), value.relation.clone()).map_err(authz_status)?;
    Ok(Tuple {
        object,
        relation: value.relation,
        subject,
    })
}

pub(crate) fn tuple_to_api(value: &Tuple) -> api::RelationTuple {
    api::RelationTuple {
        object: Some(object_to_api(&value.object)),
        relation: value.relation.clone(),
        subject: Some(subject_to_api(&value.subject)),
    }
}

pub(crate) fn tuple_mutation_from_api(
    value: api::TupleMutation,
) -> Result<DomainTupleMutation, Status> {
    match required(value.operation, "tuple mutation operation")? {
        api::tuple_mutation::Operation::Add(tuple) => {
            tuple_from_api(tuple).map(DomainTupleMutation::Add)
        }
        api::tuple_mutation::Operation::Remove(tuple) => {
            tuple_from_api(tuple).map(DomainTupleMutation::Remove)
        }
    }
}

pub(crate) fn tuple_mutation_to_api(value: &DomainTupleMutation) -> api::TupleMutation {
    let operation = match value {
        DomainTupleMutation::Add(tuple) => api::tuple_mutation::Operation::Add(tuple_to_api(tuple)),
        DomainTupleMutation::Remove(tuple) => {
            api::tuple_mutation::Operation::Remove(tuple_to_api(tuple))
        }
    };
    api::TupleMutation {
        operation: Some(operation),
    }
}

pub(crate) fn tuple_filter_from_api(
    value: Option<api::TupleFilter>,
) -> Result<DomainTupleFilter, Status> {
    let Some(value) = value else {
        return Ok(DomainTupleFilter::default());
    };
    let object = value
        .object
        .and_then(|filter| filter.selection)
        .map(|selection| match selection {
            api::object_filter::Selection::Namespace(namespace) => {
                // Object construction applies the canonical namespace limits
                // without creating a persisted resource.
                ObjectRef::opaque(&namespace, "_filter")
                    .map_err(authz_status)
                    .map(|_| DomainObjectFilter::Namespace(namespace))
            }
            api::object_filter::Selection::Exact(object) => {
                object_from_api(Some(object)).map(DomainObjectFilter::Exact)
            }
        })
        .transpose()?;
    let relation = value
        .relation
        .map(|relation| {
            UsersetRef::new(
                ObjectRef::opaque("_filter", "_filter").expect("static filter object is valid"),
                &relation,
            )
            .map_err(authz_status)?;
            Ok::<_, Status>(relation)
        })
        .transpose()?;
    let subject = value
        .subject
        .map(|subject| subject_from_api(Some(subject)))
        .transpose()?;
    Ok(DomainTupleFilter {
        object,
        relation,
        subject,
    })
}

pub(crate) fn consistency_from_api(
    value: Option<api::AuthzConsistency>,
) -> Result<AuthzConsistency, Status> {
    match value.and_then(|value| value.requirement) {
        None | Some(api::authz_consistency::Requirement::Latest(_)) => Ok(AuthzConsistency::Latest),
        Some(api::authz_consistency::Requirement::AtLeast(value)) => {
            Ok(AuthzConsistency::AtLeast(AuthzRevision(value.revision)))
        }
        Some(api::authz_consistency::Requirement::Exact(value)) => {
            Ok(AuthzConsistency::Exact(AuthzRevision(value.revision)))
        }
    }
}

pub(crate) fn check_from_api(value: api::PermissionCheck) -> Result<AuthorizationCheck, Status> {
    let subject = match subject_from_api(value.subject)? {
        TupleSubject::Object(subject) => subject,
        TupleSubject::Userset(_) => {
            return Err(Status::invalid_argument(
                "a permission check subject must be one typed object",
            ));
        }
    };
    let object = object_from_api(value.object)?;
    UsersetRef::new(object.clone(), value.relation.clone()).map_err(authz_status)?;
    Ok(AuthorizationCheck::new(subject, object, value.relation))
}

pub(crate) fn check_to_api(value: &AuthorizationCheck) -> api::PermissionCheck {
    api::PermissionCheck {
        subject: Some(subject_to_api(&TupleSubject::Object(value.subject.clone()))),
        object: Some(object_to_api(&value.object)),
        relation: value.relation.clone(),
    }
}

pub(crate) fn authz_status(error: AuthorizationError) -> Status {
    match error {
        AuthorizationError::EvaluationLimit { .. } => Status::resource_exhausted(error.to_string()),
        AuthorizationError::InvalidLimits(_) => {
            Status::internal("authorization limits are invalid")
        }
        AuthorizationError::InvalidRealm(_)
        | AuthorizationError::InvalidSchema(_)
        | AuthorizationError::InvalidTuple { .. }
        | AuthorizationError::InvalidCheck(_) => Status::invalid_argument(error.to_string()),
    }
}

fn parse_scope(value: api::AuthzScope) -> Result<ScopedRealm, Status> {
    let (storage_tenant, realm) = parse_scope_parts(value)?;
    ScopedRealm::new(storage_tenant, realm).map_err(store_input_status)
}

fn parse_scope_parts(value: api::AuthzScope) -> Result<(StorageTenantId, RealmId), Status> {
    let storage_tenant =
        StorageTenantId::parse(value.storage_tenant).map_err(store_input_status)?;
    let realm = RealmId::parse(value.realm).map_err(authz_status)?;
    Ok((storage_tenant, realm))
}

fn store_input_status(error: AuthzStoreError) -> Status {
    Status::invalid_argument(error.to_string())
}

fn namespace_from_api(value: api::NamespaceDefinition) -> Result<NamespaceDefinition, Status> {
    let relations = value
        .relations
        .into_iter()
        .map(relation_from_api)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NamespaceDefinition::new(value.name, relations))
}

fn namespace_to_api(value: &NamespaceDefinition) -> api::NamespaceDefinition {
    api::NamespaceDefinition {
        name: value.name.clone(),
        relations: value.relations.iter().map(relation_to_api).collect(),
    }
}

fn relation_from_api(value: api::RelationDefinition) -> Result<RelationDefinition, Status> {
    let kind = match required(value.kind, "relation kind")? {
        api::relation_definition::Kind::Direct(direct) => RelationKind::Direct {
            allowed_subjects: direct
                .allowed_subjects
                .into_iter()
                .map(selector_from_api)
                .collect::<Result<Vec<_>, _>>()?,
        },
        api::relation_definition::Kind::Permission(permission) => RelationKind::Permission {
            rules: permission
                .rules
                .into_iter()
                .map(rule_from_api)
                .collect::<Result<Vec<_>, _>>()?,
        },
    };
    Ok(RelationDefinition {
        name: value.name,
        kind,
    })
}

fn relation_to_api(value: &RelationDefinition) -> api::RelationDefinition {
    let kind = match &value.kind {
        RelationKind::Direct { allowed_subjects } => {
            api::relation_definition::Kind::Direct(api::DirectRelation {
                allowed_subjects: allowed_subjects.iter().map(selector_to_api).collect(),
            })
        }
        RelationKind::Permission { rules } => {
            api::relation_definition::Kind::Permission(api::Permission {
                rules: rules.iter().map(rule_to_api).collect(),
            })
        }
    };
    api::RelationDefinition {
        name: value.name.clone(),
        kind: Some(kind),
    }
}

fn selector_from_api(value: api::SubjectSelector) -> Result<AllowedSubject, Status> {
    match required(value.selector, "subject selector")? {
        api::subject_selector::Selector::AnyObject(selector) => {
            Ok(AllowedSubject::any_object(selector.namespace))
        }
        api::subject_selector::Selector::AnyUserset(selector) => Ok(AllowedSubject::any_userset(
            selector.namespace,
            selector.relation,
        )),
        api::subject_selector::Selector::Exact(subject) => {
            Ok(AllowedSubject::exact(subject_from_api(Some(subject))?))
        }
        api::subject_selector::Selector::SameResourceId(selector) => {
            Ok(AllowedSubject::same_resource_id(selector.namespace))
        }
        api::subject_selector::Selector::Public(_) => Ok(AllowedSubject::Public),
    }
}

fn selector_to_api(value: &AllowedSubject) -> api::SubjectSelector {
    let selector = match value {
        AllowedSubject::AnyObject { namespace } => {
            api::subject_selector::Selector::AnyObject(api::AnyObjectSelector {
                namespace: namespace.clone(),
            })
        }
        AllowedSubject::AnyUserset {
            namespace,
            relation,
        } => api::subject_selector::Selector::AnyUserset(api::AnyUsersetSelector {
            namespace: namespace.clone(),
            relation: relation.clone(),
        }),
        AllowedSubject::Exact { subject } => {
            api::subject_selector::Selector::Exact(subject_to_api(subject))
        }
        AllowedSubject::SameResourceId { namespace } => {
            api::subject_selector::Selector::SameResourceId(api::SameResourceIdSelector {
                namespace: namespace.clone(),
            })
        }
        AllowedSubject::Public => {
            api::subject_selector::Selector::Public(api::PublicSubjectSelector {})
        }
    };
    api::SubjectSelector {
        selector: Some(selector),
    }
}

fn rule_from_api(value: api::PermissionRule) -> Result<RewriteRule, Status> {
    match required(value.rule, "permission rule")? {
        api::permission_rule::Rule::Inherit(rule) => Ok(RewriteRule::Inherit {
            relation: rule.relation,
        }),
        api::permission_rule::Rule::TupleToUserset(rule) => Ok(RewriteRule::TupleToUserset {
            tuple_relation: rule.tuple_relation,
            target_relation: rule.target_relation,
        }),
    }
}

fn rule_to_api(value: &RewriteRule) -> api::PermissionRule {
    let rule = match value {
        RewriteRule::Inherit { relation } => {
            api::permission_rule::Rule::Inherit(api::InheritRule {
                relation: relation.clone(),
            })
        }
        RewriteRule::TupleToUserset {
            tuple_relation,
            target_relation,
        } => api::permission_rule::Rule::TupleToUserset(api::TupleToUsersetRule {
            tuple_relation: tuple_relation.clone(),
            target_relation: target_relation.clone(),
        }),
    };
    api::PermissionRule { rule: Some(rule) }
}

fn required<T>(value: Option<T>, label: &'static str) -> Result<T, Status> {
    value.ok_or_else(|| Status::invalid_argument(format!("{label} is required")))
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    fn opaque(namespace: &str, id: &str) -> api::ObjectRef {
        api::ObjectRef {
            namespace: namespace.into(),
            id: Some(api::object_ref::Id::OpaqueId(id.into())),
        }
    }

    fn object_subject(namespace: &str, id: &str) -> api::Subject {
        api::Subject {
            kind: Some(api::subject::Kind::Object(opaque(namespace, id))),
        }
    }

    #[test]
    fn public_scope_uses_trusted_tenant_and_internal_scope_keeps_system_support() {
        let custom = api::AuthzScope {
            storage_tenant: "acme".into(),
            realm: "ledger".into(),
        };
        let parsed = public_scope_from_api(Some(custom.clone()), "acme").unwrap();
        assert_eq!(parsed.storage_tenant.as_str(), "acme");
        assert_eq!(parsed.realm.as_str(), "ledger");
        assert_eq!(scope_to_api(&parsed), custom);

        let mismatch = public_scope_from_api(Some(custom), "other").unwrap_err();
        assert_eq!(mismatch.code(), Code::PermissionDenied);

        let personaldb = api::AuthzScope {
            storage_tenant: "acme".into(),
            realm: anvil_authz::PERSONALDB_REALM_ID.into(),
        };
        assert_eq!(
            public_scope_from_api(Some(personaldb), "acme")
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );

        let system = api::AuthzScope {
            storage_tenant: anvil_store::SYSTEM_STORAGE_TENANT_ID.into(),
            realm: anvil_authz::SYSTEM_REALM_ID.into(),
        };
        assert_eq!(
            public_scope_from_api(Some(system.clone()), anvil_store::SYSTEM_STORAGE_TENANT_ID)
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let internal = internal_scope_from_api(Some(system)).unwrap();
        assert!(internal.realm.is_system());

        let protected_tenant_custom_realm = api::AuthzScope {
            storage_tenant: anvil_store::SYSTEM_STORAGE_TENANT_ID.into(),
            realm: "custom".into(),
        };
        assert_eq!(
            public_scope_from_api(
                Some(protected_tenant_custom_realm.clone()),
                anvil_store::SYSTEM_STORAGE_TENANT_ID,
            )
            .unwrap_err()
            .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            internal_scope_from_api(Some(protected_tenant_custom_realm))
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );

        let custom_tenant_system_realm = api::AuthzScope {
            storage_tenant: "acme".into(),
            realm: anvil_authz::SYSTEM_REALM_ID.into(),
        };
        assert_eq!(
            internal_scope_from_api(Some(custom_tenant_system_realm))
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn typed_objects_and_usersets_round_trip_without_string_parsing() {
        let exact = api::ObjectRef {
            namespace: "object".into(),
            id: Some(api::object_ref::Id::ExactPath(api::ObjectAddress {
                tenant: "acme".into(),
                bucket: "objects".into(),
                path: "reports/one.json".into(),
            })),
        };
        let exact_domain = object_from_api(Some(exact.clone())).unwrap();
        assert_eq!(object_to_api(&exact_domain), exact);

        let userset = api::Subject {
            kind: Some(api::subject::Kind::Userset(api::Userset {
                object: Some(opaque("group", "auditors")),
                relation: "member".into(),
            })),
        };
        let userset_domain = subject_from_api(Some(userset.clone())).unwrap();
        assert_eq!(subject_to_api(&userset_domain), userset);

        let missing_id = object_from_api(Some(api::ObjectRef {
            namespace: "user".into(),
            id: None,
        }))
        .unwrap_err();
        assert_eq!(missing_id.code(), Code::InvalidArgument);
    }

    #[test]
    fn direct_selectors_and_permission_rules_round_trip_as_valid_schema() {
        let schema = vec![
            api::NamespaceDefinition {
                name: "user".into(),
                relations: vec![direct("marker", vec![any_object("user")])],
            },
            api::NamespaceDefinition {
                name: "group".into(),
                relations: vec![direct(
                    "member",
                    vec![any_object("user"), public_selector()],
                )],
            },
            api::NamespaceDefinition {
                name: "folder".into(),
                relations: vec![direct("viewer", vec![any_object("user")])],
            },
            api::NamespaceDefinition {
                name: "resource".into(),
                relations: vec![
                    direct(
                        "reader",
                        vec![
                            any_object("user"),
                            api::SubjectSelector {
                                selector: Some(api::subject_selector::Selector::AnyUserset(
                                    api::AnyUsersetSelector {
                                        namespace: "group".into(),
                                        relation: "member".into(),
                                    },
                                )),
                            },
                            api::SubjectSelector {
                                selector: Some(api::subject_selector::Selector::Exact(
                                    object_subject("user", "service-account"),
                                )),
                            },
                            api::SubjectSelector {
                                selector: Some(api::subject_selector::Selector::SameResourceId(
                                    api::SameResourceIdSelector {
                                        namespace: "user".into(),
                                    },
                                )),
                            },
                            public_selector(),
                        ],
                    ),
                    direct("parent", vec![any_object("folder")]),
                    api::RelationDefinition {
                        name: "read".into(),
                        kind: Some(api::relation_definition::Kind::Permission(
                            api::Permission {
                                rules: vec![
                                    api::PermissionRule {
                                        rule: Some(api::permission_rule::Rule::Inherit(
                                            api::InheritRule {
                                                relation: "reader".into(),
                                            },
                                        )),
                                    },
                                    api::PermissionRule {
                                        rule: Some(api::permission_rule::Rule::TupleToUserset(
                                            api::TupleToUsersetRule {
                                                tuple_relation: "parent".into(),
                                                target_relation: "viewer".into(),
                                            },
                                        )),
                                    },
                                ],
                            },
                        )),
                    },
                ],
            },
        ];

        let domain = schema_from_api(schema.clone(), AuthorizationLimits::default()).unwrap();
        assert_eq!(schema_to_api(&domain), schema);
    }

    #[test]
    fn malformed_schema_tuple_and_check_inputs_fail_closed() {
        let malformed_schema = vec![api::NamespaceDefinition {
            name: "resource".into(),
            relations: vec![api::RelationDefinition {
                name: "reader".into(),
                kind: None,
            }],
        }];
        assert_eq!(
            schema_from_api(malformed_schema, AuthorizationLimits::default())
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );

        let tuple = api::RelationTuple {
            object: Some(opaque("resource", "one")),
            relation: "reader".into(),
            subject: Some(object_subject("user", "alice")),
        };
        let domain_tuple = tuple_from_api(tuple.clone()).unwrap();
        assert_eq!(tuple_to_api(&domain_tuple), tuple);

        let userset_check = api::PermissionCheck {
            subject: Some(api::Subject {
                kind: Some(api::subject::Kind::Userset(api::Userset {
                    object: Some(opaque("group", "auditors")),
                    relation: "member".into(),
                })),
            }),
            object: Some(opaque("resource", "one")),
            relation: "read".into(),
        };
        assert_eq!(
            check_from_api(userset_check).unwrap_err().code(),
            Code::InvalidArgument
        );

        let check = api::PermissionCheck {
            subject: Some(object_subject("user", "alice")),
            object: Some(opaque("resource", "one")),
            relation: "read".into(),
        };
        let domain_check = check_from_api(check.clone()).unwrap();
        assert_eq!(check_to_api(&domain_check), check);
    }

    #[test]
    fn exact_consistency_is_preserved_at_the_transport_boundary() {
        let consistency = consistency_from_api(Some(api::AuthzConsistency {
            requirement: Some(api::authz_consistency::Requirement::Exact(
                api::ExactRevision { revision: 42 },
            )),
        }))
        .unwrap();
        assert_eq!(consistency, AuthzConsistency::Exact(AuthzRevision(42)));
    }

    #[test]
    fn domain_errors_map_to_bounded_grpc_outcomes() {
        assert_eq!(
            authz_status(AuthorizationError::InvalidCheck("bad check".into())).code(),
            Code::InvalidArgument
        );
        assert_eq!(
            authz_status(AuthorizationError::EvaluationLimit {
                limit: "step",
                maximum: 16,
            })
            .code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            authz_status(AuthorizationError::InvalidLimits("bad limits".into())).code(),
            Code::Internal
        );
    }

    fn direct(name: &str, allowed_subjects: Vec<api::SubjectSelector>) -> api::RelationDefinition {
        api::RelationDefinition {
            name: name.into(),
            kind: Some(api::relation_definition::Kind::Direct(
                api::DirectRelation { allowed_subjects },
            )),
        }
    }

    fn any_object(namespace: &str) -> api::SubjectSelector {
        api::SubjectSelector {
            selector: Some(api::subject_selector::Selector::AnyObject(
                api::AnyObjectSelector {
                    namespace: namespace.into(),
                },
            )),
        }
    }

    fn public_selector() -> api::SubjectSelector {
        api::SubjectSelector {
            selector: Some(api::subject_selector::Selector::Public(
                api::PublicSubjectSelector {},
            )),
        }
    }
}
