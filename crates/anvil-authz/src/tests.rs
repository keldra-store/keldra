use crate::*;

fn realm(value: &str) -> RealmId {
    RealmId::custom(value).unwrap()
}

fn opaque(namespace: &str, id: &str) -> ObjectRef {
    ObjectRef::opaque(namespace, id).unwrap()
}

fn path(path: &str) -> ObjectRef {
    ObjectRef::exact_path("object", ExactPath::new("acme", "objects", path).unwrap()).unwrap()
}

fn object_schema() -> Schema {
    Schema::new([
        NamespaceDefinition::new(
            "object",
            [
                RelationDefinition::direct("writer", [AllowedSubject::any_object("user")]),
                RelationDefinition::direct(
                    "reader",
                    [
                        AllowedSubject::any_object("user"),
                        AllowedSubject::any_userset("group", "member"),
                    ],
                ),
                RelationDefinition::direct("parent", [AllowedSubject::any_object("folder")]),
                RelationDefinition::permission(
                    "read",
                    [
                        RewriteRule::Inherit {
                            relation: "reader".into(),
                        },
                        RewriteRule::computed("parent", "viewer"),
                    ],
                ),
            ],
        ),
        NamespaceDefinition::new(
            "folder",
            [RelationDefinition::direct(
                "viewer",
                [AllowedSubject::any_object("user")],
            )],
        ),
        NamespaceDefinition::new(
            "group",
            [RelationDefinition::direct(
                "member",
                [AllowedSubject::any_object("user")],
            )],
        ),
    ])
}

#[test]
fn only_the_canonical_system_scope_is_reserved_from_custom_creation() {
    assert_eq!(RealmId::default_realm().as_str(), DEFAULT_REALM_ID);
    assert_eq!(RealmId::system().as_str(), SYSTEM_REALM_ID);
    assert!(RealmId::system().is_system());
    assert_eq!(RealmId::parse(SYSTEM_REALM_ID).unwrap(), RealmId::system());
    assert_eq!(
        RealmId::parse(DEFAULT_REALM_ID).unwrap(),
        RealmId::default()
    );
    assert!(RealmId::custom(SYSTEM_REALM_ID).is_err());
    assert_eq!(
        RealmId::custom(DEFAULT_REALM_ID).unwrap(),
        RealmId::default_realm()
    );
    for ordinary_name in ["_anvil", "_system", "system"] {
        assert_eq!(
            RealmId::custom(ordinary_name).unwrap().as_str(),
            ordinary_name
        );
    }
    assert!(RealmId::parse("team/other").is_err());
    assert!(RealmId::parse("team\nother").is_err());
}

#[test]
fn system_and_custom_realms_use_the_identical_evaluator() {
    let alice = opaque("user", "alice");
    let report = path("reports/a.json");
    let tuples = vec![Tuple::new(report.clone(), "reader", alice.clone())];
    let check = AuthorizationCheck::new(alice, report, "read");

    let system = Authorization::new(
        RealmId::system(),
        object_schema(),
        tuples.clone(),
        AuthorizationLimits::default(),
    )
    .unwrap();
    let custom = Authorization::new(
        realm("workspace"),
        object_schema(),
        tuples,
        AuthorizationLimits::default(),
    )
    .unwrap();

    assert_eq!(system.check(&check).unwrap(), custom.check(&check).unwrap());
    assert!(system.check(&check).unwrap());
    assert!(system.realm_id().is_system());
    assert_eq!(custom.realm_id().as_str(), "workspace");
}

#[test]
fn realm_tuple_graphs_are_structurally_isolated() {
    let document = path("reports/a.json");
    let alice = opaque("user", "alice");
    let bob = opaque("user", "bob");
    let alpha = Authorization::new(
        realm("alpha"),
        object_schema(),
        [Tuple::new(document.clone(), "reader", alice.clone())],
        AuthorizationLimits::default(),
    )
    .unwrap();
    let beta = Authorization::new(
        realm("beta"),
        object_schema(),
        [Tuple::new(document.clone(), "reader", bob.clone())],
        AuthorizationLimits::default(),
    )
    .unwrap();

    assert!(
        alpha
            .check(&AuthorizationCheck::new(
                alice.clone(),
                document.clone(),
                "read"
            ))
            .unwrap()
    );
    assert!(
        !alpha
            .check(&AuthorizationCheck::new(
                bob.clone(),
                document.clone(),
                "read"
            ))
            .unwrap()
    );
    assert!(
        beta.check(&AuthorizationCheck::new(bob, document.clone(), "read"))
            .unwrap()
    );
    assert!(
        !beta
            .check(&AuthorizationCheck::new(alice, document, "read"))
            .unwrap()
    );
}

#[test]
fn all_old_direct_subject_selectors_are_enforced() {
    let indexer = opaque("service", "indexer");
    let schema = Schema::new([NamespaceDefinition::new(
        "resource",
        [
            RelationDefinition::direct("any", [AllowedSubject::any_object("user")]),
            RelationDefinition::direct("exact", [AllowedSubject::exact(indexer.clone())]),
            RelationDefinition::direct(
                "self_access",
                [AllowedSubject::same_resource_id("account")],
            ),
            RelationDefinition::direct("public_reader", [AllowedSubject::Public]),
        ],
    )]);
    let any = opaque("resource", "any-1");
    let exact = opaque("resource", "exact-1");
    let self_access = opaque("resource", "account-1");
    let public = opaque("resource", "public-1");
    let alice = opaque("user", "alice");
    let self_subject = opaque("account", "account-1");
    let authorization = Authorization::new(
        realm("selectors"),
        schema.clone(),
        [
            Tuple::new(any.clone(), "any", alice.clone()),
            Tuple::new(exact.clone(), "exact", indexer.clone()),
            Tuple::new(self_access.clone(), "self_access", self_subject.clone()),
            Tuple::new(public.clone(), "public_reader", ObjectRef::public()),
        ],
        AuthorizationLimits::default(),
    )
    .unwrap();

    for check in [
        AuthorizationCheck::new(alice, any, "any"),
        AuthorizationCheck::new(indexer, exact, "exact"),
        AuthorizationCheck::new(self_subject, self_access, "self_access"),
        AuthorizationCheck::new(ObjectRef::public(), public, "public_reader"),
    ] {
        assert!(authorization.check(&check).unwrap());
    }

    let wrong_exact = Authorization::new(
        realm("wrong-exact"),
        schema.clone(),
        [Tuple::new(
            opaque("resource", "exact-1"),
            "exact",
            opaque("service", "other"),
        )],
        AuthorizationLimits::default(),
    )
    .unwrap_err();
    assert!(wrong_exact.to_string().contains("subject is not allowed"));

    let wrong_self = Authorization::new(
        realm("wrong-self"),
        schema,
        [Tuple::new(
            opaque("resource", "account-1"),
            "self_access",
            opaque("account", "account-2"),
        )],
        AuthorizationLimits::default(),
    )
    .unwrap_err();
    assert!(wrong_self.to_string().contains("subject is not allowed"));
}

#[test]
fn public_must_be_selected_explicitly() {
    let schema = Schema::new([NamespaceDefinition::new(
        "resource",
        [RelationDefinition::direct(
            "reader",
            [AllowedSubject::any_object(PUBLIC_SUBJECT_NAMESPACE)],
        )],
    )]);
    let error = Authorization::new(
        realm("public"),
        schema,
        [Tuple::new(
            opaque("resource", "one"),
            "reader",
            ObjectRef::public(),
        )],
        AuthorizationLimits::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("subject is not allowed"));

    let exact_public = Schema::new([NamespaceDefinition::new(
        "resource",
        [RelationDefinition::direct(
            "reader",
            [AllowedSubject::exact(ObjectRef::public())],
        )],
    )]);
    assert!(
        exact_public
            .validate(AuthorizationLimits::default())
            .unwrap_err()
            .to_string()
            .contains("must use the public selector")
    );
}

#[test]
fn explicit_and_typed_usersets_resolve_membership() {
    let alice = opaque("user", "alice");
    let report = path("reports/group.json");
    let group = opaque("group", "auditors");
    let group_members = UsersetRef::new(group.clone(), "member").unwrap();
    let authorization = Authorization::new(
        realm("usersets"),
        object_schema(),
        [
            Tuple::userset(report.clone(), "reader", group_members),
            Tuple::new(group, "member", alice.clone()),
        ],
        AuthorizationLimits::default(),
    )
    .unwrap();

    assert!(
        authorization
            .check(&AuthorizationCheck::new(alice, report, "read"))
            .unwrap()
    );
}

#[test]
fn an_exact_selector_can_name_one_typed_userset() {
    let alice = opaque("user", "alice");
    let report = opaque("document", "quarterly");
    let auditors = opaque("group", "auditors");
    let other_group = opaque("group", "other");
    let auditor_members = UsersetRef::new(auditors.clone(), "member").unwrap();
    let schema = Schema::new([
        NamespaceDefinition::new(
            "document",
            [RelationDefinition::direct(
                "reader",
                [AllowedSubject::exact(auditor_members.clone())],
            )],
        ),
        NamespaceDefinition::new(
            "group",
            [RelationDefinition::direct(
                "member",
                [AllowedSubject::any_object("user")],
            )],
        ),
    ]);
    let authorization = Authorization::new(
        realm("exact-userset"),
        schema.clone(),
        [
            Tuple::userset(report.clone(), "reader", auditor_members),
            Tuple::new(auditors, "member", alice.clone()),
        ],
        AuthorizationLimits::default(),
    )
    .unwrap();
    assert!(
        authorization
            .check(&AuthorizationCheck::new(alice, report.clone(), "reader"))
            .unwrap()
    );

    let other_members = UsersetRef::new(other_group, "member").unwrap();
    assert!(
        Authorization::new(
            realm("wrong-exact-userset"),
            schema,
            [Tuple::userset(report, "reader", other_members)],
            AuthorizationLimits::default(),
        )
        .unwrap_err()
        .to_string()
        .contains("subject is not allowed")
    );
}

#[test]
fn inherit_and_computed_rules_are_a_union() {
    let alice = opaque("user", "alice");
    let bob = opaque("user", "bob");
    let direct = path("reports/direct.json");
    let computed = path("reports/computed.json");
    let folder = opaque("folder", "finance");
    let authorization = Authorization::new(
        realm("union"),
        object_schema(),
        [
            Tuple::new(direct.clone(), "reader", bob.clone()),
            Tuple::new(computed.clone(), "parent", folder.clone()),
            Tuple::new(folder, "viewer", alice.clone()),
        ],
        AuthorizationLimits::default(),
    )
    .unwrap();

    assert!(
        authorization
            .check(&AuthorizationCheck::new(bob, direct, "read"))
            .unwrap()
    );
    assert!(
        authorization
            .check(&AuthorizationCheck::new(alice, computed, "read"))
            .unwrap()
    );
}

#[test]
fn reserved_program_definition_uses_the_same_exact_path_grant() {
    let alice = opaque("user", "alice");
    let bob = opaque("user", "bob");
    let program = path("_anvil/programs/import_osv@1");
    let authorization = Authorization::new(
        RealmId::system(),
        object_schema(),
        [Tuple::new(program.clone(), "writer", alice.clone())],
        AuthorizationLimits::default(),
    )
    .unwrap();

    assert!(
        authorization
            .check(&AuthorizationCheck::new(alice, program.clone(), "writer"))
            .unwrap()
    );
    assert!(
        !authorization
            .check(&AuthorizationCheck::new(bob, program, "writer"))
            .unwrap()
    );
}

#[test]
fn tuples_cannot_target_permissions_or_wrong_subject_types() {
    let report = path("reports/a.json");
    let permission = Authorization::new(
        realm("permission"),
        object_schema(),
        [Tuple::new(report.clone(), "read", opaque("user", "alice"))],
        AuthorizationLimits::default(),
    )
    .unwrap_err();
    assert!(
        permission
            .to_string()
            .contains("cannot be written as a tuple")
    );

    let wrong_type = Authorization::new(
        realm("wrong-type"),
        object_schema(),
        [Tuple::new(report, "writer", opaque("service", "importer"))],
        AuthorizationLimits::default(),
    )
    .unwrap_err();
    assert!(wrong_type.to_string().contains("subject is not allowed"));
}

#[test]
fn schema_rejects_unresolved_and_unsafe_tuple_to_userset_sources() {
    let missing_target = Schema::new([NamespaceDefinition::new(
        "document",
        [
            RelationDefinition::direct("parent", [AllowedSubject::any_object("folder")]),
            RelationDefinition::permission("view", [RewriteRule::computed("parent", "viewer")]),
        ],
    )]);
    assert!(
        missing_target
            .validate(AuthorizationLimits::default())
            .unwrap_err()
            .to_string()
            .contains("tuple-to-userset target")
    );

    let public_source = Schema::new([NamespaceDefinition::new(
        "document",
        [
            RelationDefinition::direct("parent", [AllowedSubject::Public]),
            RelationDefinition::permission("view", [RewriteRule::computed("parent", "viewer")]),
        ],
    )]);
    assert!(
        public_source
            .validate(AuthorizationLimits::default())
            .unwrap_err()
            .to_string()
            .contains("cannot allow the public subject")
    );
}

#[test]
fn duplicate_tuples_and_schema_members_are_rejected() {
    let report = path("reports/a.json");
    let tuple = Tuple::new(report, "writer", opaque("user", "alice"));
    let duplicate_tuple = Authorization::new(
        realm("duplicates"),
        object_schema(),
        [tuple.clone(), tuple],
        AuthorizationLimits::default(),
    )
    .unwrap_err();
    assert!(duplicate_tuple.to_string().contains("duplicate tuple"));

    let duplicate_schema = Schema::new([NamespaceDefinition::new(
        "object",
        [
            RelationDefinition::direct("writer", [AllowedSubject::any_object("user")]),
            RelationDefinition::direct("writer", [AllowedSubject::any_object("user")]),
        ],
    )]);
    assert!(
        duplicate_schema
            .validate(AuthorizationLimits::default())
            .unwrap_err()
            .to_string()
            .contains("duplicate relation")
    );
}

#[test]
fn cycles_deny_and_depth_is_hard_bounded() {
    let cycle = Schema::new([NamespaceDefinition::new(
        "document",
        [
            RelationDefinition::permission(
                "a",
                [RewriteRule::Inherit {
                    relation: "b".into(),
                }],
            ),
            RelationDefinition::permission(
                "b",
                [RewriteRule::Inherit {
                    relation: "a".into(),
                }],
            ),
        ],
    )]);
    let document = opaque("document", "doc-1");
    let alice = opaque("user", "alice");
    let authorization =
        Authorization::new(realm("cycle"), cycle, [], AuthorizationLimits::default()).unwrap();
    assert!(
        !authorization
            .check(&AuthorizationCheck::new(
                alice.clone(),
                document.clone(),
                "a"
            ))
            .unwrap()
    );

    let chain = Schema::new([NamespaceDefinition::new(
        "document",
        [
            RelationDefinition::permission(
                "a",
                [RewriteRule::Inherit {
                    relation: "b".into(),
                }],
            ),
            RelationDefinition::permission(
                "b",
                [RewriteRule::Inherit {
                    relation: "c".into(),
                }],
            ),
            RelationDefinition::direct("c", [AllowedSubject::any_object("user")]),
        ],
    )]);
    let limits = AuthorizationLimits {
        max_depth: 2,
        ..AuthorizationLimits::default()
    };
    let authorization = Authorization::new(realm("depth"), chain, [], limits).unwrap();
    assert!(matches!(
        authorization.check(&AuthorizationCheck::new(alice, document, "a")),
        Err(AuthorizationError::EvaluationLimit { limit: "depth", .. })
    ));
}

#[test]
fn tuple_schema_and_evaluation_limits_are_hard_bounds() {
    let target = path("reports/wide.json");
    let tuples = (0..8)
        .map(|index| {
            Tuple::new(
                target.clone(),
                "reader",
                opaque("user", &format!("user-{index}")),
            )
        })
        .collect::<Vec<_>>();
    let step_limits = AuthorizationLimits {
        max_steps: 4,
        ..AuthorizationLimits::default()
    };
    let authorization =
        Authorization::new(realm("steps"), object_schema(), tuples.clone(), step_limits).unwrap();
    assert!(matches!(
        authorization.check(&AuthorizationCheck::new(
            opaque("user", "missing"),
            target,
            "read",
        )),
        Err(AuthorizationError::EvaluationLimit { limit: "step", .. })
    ));

    let tuple_limits = AuthorizationLimits {
        max_tuples: 1,
        ..AuthorizationLimits::default()
    };
    assert!(
        Authorization::new(realm("tuples"), object_schema(), tuples, tuple_limits)
            .unwrap_err()
            .to_string()
            .contains("tuple set")
    );

    let oversized_namespace = "n".repeat(257);
    assert!(
        Schema::new([NamespaceDefinition::new(
            oversized_namespace,
            [RelationDefinition::direct(
                "reader",
                [AllowedSubject::any_object("user")],
            )],
        )])
        .validate(AuthorizationLimits::default())
        .is_err()
    );
}

#[test]
fn exact_paths_are_canonical_and_never_gain_implicit_prefix_rights() {
    assert!(ExactPath::new("acme", "objects", "/absolute").is_err());
    assert!(ExactPath::new("acme", "objects", "double//segment").is_err());
    assert!(ExactPath::new("acme", "objects", "one/../two").is_err());

    let alice = opaque("user", "alice");
    let parent = path("reports");
    let child = path("reports/a.json");
    let authorization = Authorization::new(
        realm("paths"),
        object_schema(),
        [Tuple::new(parent, "reader", alice.clone())],
        AuthorizationLimits::default(),
    )
    .unwrap();
    assert!(
        !authorization
            .check(&AuthorizationCheck::new(alice, child, "read"))
            .unwrap()
    );
}

#[test]
fn persisted_models_round_trip_without_realm_namespace_encoding() {
    let realm_id = RealmId::system();
    let tuple = Tuple::userset(
        path("reports/group.json"),
        "reader",
        UsersetRef::new(opaque("group", "auditors"), "member").unwrap(),
    );
    let schema = object_schema();

    assert_eq!(
        serde_json::from_str::<RealmId>(&serde_json::to_string(&realm_id).unwrap()).unwrap(),
        realm_id
    );
    assert_eq!(
        serde_json::from_str::<Tuple>(&serde_json::to_string(&tuple).unwrap()).unwrap(),
        tuple
    );
    let schema_json = serde_json::to_string(&schema).unwrap();
    assert!(!schema_json.contains("realm__"));
    assert_eq!(
        serde_json::from_str::<Schema>(&schema_json).unwrap(),
        schema
    );
    assert!(serde_json::from_str::<RealmId>("\"other/realm\"").is_err());
}
