//! Public multi-node qualification for custom-realm index authorization.

use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::io;
use std::time::Duration;

use keldra_storage::v1::index_field::FieldType as IndexFieldType;
use keldra_storage::v1::index_query::Query as QueryValue;
use keldra_storage::v1::index_result_authorization::Policy as AuthorizationPolicy;
use keldra_storage::v1::index_service_client::IndexServiceClient;
use keldra_storage::v1::index_specification::Specification as SpecificationValue;
use keldra_storage::v1::put_header::Operation as PutOperationValue;
use keldra_storage::v1::subject::Kind as SubjectKind;
use keldra_storage::v1::subject_selector::Selector;
use keldra_storage::v1::tuple_mutation::Operation as TupleOperation;
use keldra_storage::v1::{
    AnyObjectSelector, AuthzScope, BindSchemaRequest, BooleanIndexField, CreateBucketRequest,
    CreateIndexRequest, DirectRelation, Durability, IndexAuthorizationTarget, IndexField,
    IndexFieldCapability, IndexFieldCardinality, IndexPredicate, IndexPredicateExpression,
    IndexPredicateOperator, IndexQuery, IndexResultAuthorization, IndexSpecification,
    MutateTuplesRequest, NamespaceDefinition, ObjectAddress, ObjectRef, ObjectVersioning,
    PutHeader, PutOperation, PutSchemaRequest, QueryIndexRequest, RealmIndexResultAuthorization,
    RelationDefinition, RelationTuple, Subject, SubjectSelector, TupleMutation,
    TypedJsonIndexQuery, TypedJsonIndexSpec,
};
use keldra_storage::{
    BearerToken, RawClient, administration_client, authz_client, connect_channel,
    exchange_client_credentials, object_client, put_chunks,
};
use tokio::time::{Instant, sleep};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type IndexClient = IndexServiceClient<InterceptedService<Channel, BearerToken>>;

const WAIT_LIMIT: Duration = Duration::from_secs(90);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const INDEX_NAME: &str = "realm-visible-paths";
const REALM: &str = "index-visibility";
const RESOURCE_NAMESPACE: &str = "document";
const RELATION: &str = "viewer";
const ALICE: &str = "alice";
const BOB: &str = "bob";
const CHARLIE: &str = "charlie";
const ALICE_ONLY: &str = "docs/alice.json";
const BOB_ONLY: &str = "docs/bob.json";
const SHARED: &str = "docs/shared.json";
const HIDDEN: &str = "docs/hidden.json";

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let endpoints = required("KELDRA_REALM_INDEX_QUALIFICATION_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !matches!(endpoints.len(), 2 | 3) {
        return Err(invalid(
            "realm index qualification requires exactly two or three endpoints",
        ));
    }
    let tenant = required("KELDRA_REALM_INDEX_QUALIFICATION_TENANT")?;
    let bucket = required("KELDRA_REALM_INDEX_QUALIFICATION_BUCKET")?;
    let client_id = required("KELDRA_REALM_INDEX_QUALIFICATION_CLIENT_ID")?;
    let client_secret = required("KELDRA_REALM_INDEX_QUALIFICATION_CLIENT_SECRET")?;

    let mut channels = Vec::with_capacity(endpoints.len());
    for endpoint in &endpoints {
        channels.push(connect_channel(endpoint).await?);
    }
    let token = exchange_client_credentials(channels[0].clone(), client_id, client_secret)
        .await?
        .access_token;
    let mut authz = channels
        .iter()
        .cloned()
        .map(|channel| authz_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;
    let mut indexes = channels
        .iter()
        .cloned()
        .map(|channel| index_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;
    let mut objects = channels
        .iter()
        .cloned()
        .map(|channel| object_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;
    let mut administrator = administration_client(channels[0].clone(), &token)?;

    if env::var("KELDRA_REALM_INDEX_QUALIFICATION_VERIFY_ONLY").is_ok_and(|value| value == "1") {
        wait_for_every_endpoint(&mut indexes, &bucket, CHARLIE, &BTreeSet::from([HIDDEN]), 1)
            .await?;
        wait_for_every_endpoint(
            &mut indexes,
            &bucket,
            ALICE,
            &BTreeSet::from([ALICE_ONLY, SHARED]),
            1,
        )
        .await?;
        wait_for_every_endpoint(
            &mut indexes,
            &bucket,
            BOB,
            &BTreeSet::from([BOB_ONLY, SHARED]),
            1,
        )
        .await?;
        println!(
            "existing custom-realm index remained correct through {} reachable nodes",
            endpoints.len()
        );
        return Ok(());
    }

    let created_bucket = administrator
        .create_bucket(CreateBucketRequest {
            bucket: bucket.clone(),
            versioning: ObjectVersioning::Unversioned as i32,
        })
        .await?
        .into_inner();
    if created_bucket.bucket != bucket {
        return Err(invalid("bucket creation returned another bucket"));
    }

    let schema = authz[0]
        .put_schema(PutSchemaRequest {
            schema_id: format!("realm-index-{}-node", endpoints.len()),
            namespaces: authorization_schema(),
        })
        .await?
        .into_inner()
        .schema_ref
        .ok_or_else(|| invalid("schema publication omitted its reference"))?;
    // Deliberately perform the first-ever binding through another node. This
    // is the public regression boundary for distributed fresh-realm creation.
    let bound = authz[1]
        .bind_schema(BindSchemaRequest {
            scope: Some(scope(&tenant)),
            schema_ref: Some(schema),
            expected_binding_generation: None,
        })
        .await?
        .into_inner();
    let binding = bound
        .binding
        .ok_or_else(|| invalid("fresh realm binding omitted its binding"))?;
    if binding.generation == 0 || bound.revision == 0 {
        return Err(invalid("fresh realm binding returned invalid identity"));
    }

    let initial_grants = vec![
        grant(&tenant, &bucket, ALICE_ONLY, ALICE),
        grant(&tenant, &bucket, SHARED, ALICE),
        grant(&tenant, &bucket, BOB_ONLY, BOB),
        grant(&tenant, &bucket, SHARED, BOB),
        grant(&tenant, &bucket, HIDDEN, CHARLIE),
    ];
    let granted = authz[endpoints.len() - 1]
        .mutate_tuples(MutateTuplesRequest {
            scope: Some(scope(&tenant)),
            operation_id: format!("realm-index-initial-{}-node", endpoints.len()),
            expected_revision: Some(bound.revision),
            mutations: initial_grants,
        })
        .await?
        .into_inner();
    if granted.revision <= bound.revision {
        return Err(invalid("initial grants did not advance authorization"));
    }

    let definition = indexes[0]
        .create_index(CreateIndexRequest {
            bucket: bucket.clone(),
            name: INDEX_NAME.into(),
            path_prefix: "docs/".into(),
            content_type: "application/json".into(),
            specification: Some(IndexSpecification {
                specification: Some(SpecificationValue::TypedJson(TypedJsonIndexSpec {
                    fields: vec![IndexField {
                        name: "qualified".into(),
                        json_pointer: "/qualified".into(),
                        cardinality: IndexFieldCardinality::Single as i32,
                        capabilities: vec![IndexFieldCapability::Exact as i32],
                        field_type: Some(IndexFieldType::Boolean(BooleanIndexField {})),
                    }],
                    physical_order: Vec::new(),
                })),
            }),
            command_id: format!("realm-index-create-{}-node", endpoints.len()),
            result_authorization: Some(realm_result_authorization()),
        })
        .await?
        .into_inner();
    if definition.index_id == 0 || definition.version == 0 {
        return Err(invalid("realm-authorized index returned invalid identity"));
    }

    let object_client_count = objects.len();
    for (position, path) in [ALICE_ONLY, BOB_ONLY, SHARED, HIDDEN].iter().enumerate() {
        put_document(
            &mut objects[position % object_client_count],
            &tenant,
            &bucket,
            path,
            &format!("realm-index-write-{}-{position}", endpoints.len()),
        )
        .await?;
    }

    let alice_expected = BTreeSet::from([ALICE_ONLY, SHARED]);
    let bob_expected = BTreeSet::from([BOB_ONLY, SHARED]);
    // Prove the candidate called "hidden" has reached the committed index;
    // its later absence for Alice and Bob must therefore be authorization.
    wait_for_every_endpoint(
        &mut indexes,
        &bucket,
        CHARLIE,
        &BTreeSet::from([HIDDEN]),
        granted.revision,
    )
    .await?;
    wait_for_every_endpoint(
        &mut indexes,
        &bucket,
        ALICE,
        &alice_expected,
        granted.revision,
    )
    .await?;
    wait_for_every_endpoint(&mut indexes, &bucket, BOB, &bob_expected, granted.revision).await?;
    require_subject_and_token_fail_closed(&mut indexes[0], &bucket).await?;

    let revoked = authz[0]
        .mutate_tuples(MutateTuplesRequest {
            scope: Some(scope(&tenant)),
            operation_id: format!("realm-index-revoke-{}-node", endpoints.len()),
            expected_revision: Some(granted.revision),
            mutations: vec![revoke(&tenant, &bucket, ALICE_ONLY, ALICE)],
        })
        .await?
        .into_inner();
    wait_for_every_endpoint(
        &mut indexes,
        &bucket,
        ALICE,
        &BTreeSet::from([SHARED]),
        revoked.revision,
    )
    .await?;

    let restored = authz[1]
        .mutate_tuples(MutateTuplesRequest {
            scope: Some(scope(&tenant)),
            operation_id: format!("realm-index-restore-{}-node", endpoints.len()),
            expected_revision: Some(revoked.revision),
            mutations: vec![grant(&tenant, &bucket, ALICE_ONLY, ALICE)],
        })
        .await?
        .into_inner();
    wait_for_every_endpoint(
        &mut indexes,
        &bucket,
        ALICE,
        &alice_expected,
        restored.revision,
    )
    .await?;

    println!(
        "fresh custom-realm binding and realm-filtered index qualification passed on {} nodes",
        endpoints.len()
    );
    Ok(())
}

fn authorization_schema() -> Vec<NamespaceDefinition> {
    vec![
        NamespaceDefinition {
            name: "user".into(),
            relations: vec![RelationDefinition {
                name: "marker".into(),
                kind: Some(keldra_storage::v1::relation_definition::Kind::Direct(
                    DirectRelation {
                        allowed_subjects: vec![SubjectSelector {
                            selector: Some(Selector::AnyObject(AnyObjectSelector {
                                namespace: "user".into(),
                            })),
                        }],
                    },
                )),
            }],
        },
        NamespaceDefinition {
            name: RESOURCE_NAMESPACE.into(),
            relations: vec![RelationDefinition {
                name: RELATION.into(),
                kind: Some(keldra_storage::v1::relation_definition::Kind::Direct(
                    DirectRelation {
                        allowed_subjects: vec![SubjectSelector {
                            selector: Some(Selector::AnyObject(AnyObjectSelector {
                                namespace: "user".into(),
                            })),
                        }],
                    },
                )),
            }],
        },
    ]
}

fn realm_result_authorization() -> IndexResultAuthorization {
    IndexResultAuthorization {
        policy: Some(AuthorizationPolicy::Realm(RealmIndexResultAuthorization {
            realm: REALM.into(),
            resource_namespace: RESOURCE_NAMESPACE.into(),
            relation: RELATION.into(),
            target: IndexAuthorizationTarget::ResultPath as i32,
        })),
    }
}

fn scope(tenant: &str) -> AuthzScope {
    AuthzScope {
        storage_tenant: tenant.into(),
        realm: REALM.into(),
    }
}

fn grant(tenant: &str, bucket: &str, path: &str, user: &str) -> TupleMutation {
    TupleMutation {
        operation: Some(TupleOperation::Add(relation_tuple(
            tenant, bucket, path, user,
        ))),
    }
}

fn revoke(tenant: &str, bucket: &str, path: &str, user: &str) -> TupleMutation {
    TupleMutation {
        operation: Some(TupleOperation::Remove(relation_tuple(
            tenant, bucket, path, user,
        ))),
    }
}

fn relation_tuple(tenant: &str, bucket: &str, path: &str, user: &str) -> RelationTuple {
    RelationTuple {
        object: Some(ObjectRef {
            namespace: RESOURCE_NAMESPACE.into(),
            id: Some(keldra_storage::v1::object_ref::Id::ExactPath(
                ObjectAddress {
                    tenant: tenant.into(),
                    bucket: bucket.into(),
                    path: path.into(),
                },
            )),
        }),
        relation: RELATION.into(),
        subject: Some(user_subject(user)),
    }
}

fn user_subject(user: &str) -> Subject {
    Subject {
        kind: Some(SubjectKind::Object(ObjectRef {
            namespace: "user".into(),
            id: Some(keldra_storage::v1::object_ref::Id::OpaqueId(user.into())),
        })),
    }
}

async fn put_document(
    client: &mut RawClient,
    tenant: &str,
    bucket: &str,
    path: &str,
    command_id: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    let receipt = loop {
        let result = put_chunks(
            client,
            PutHeader {
                address: Some(ObjectAddress {
                    tenant: tenant.into(),
                    bucket: bucket.into(),
                    path: path.into(),
                }),
                content_type: "application/json".into(),
                command_id: command_id.into(),
                durability: Durability::Replicated as i32,
                operation: Some(PutOperationValue::Put(PutOperation {})),
            },
            [br#"{"qualified":true}"#.to_vec()],
        )
        .await;
        match result {
            Ok(receipt) => break receipt,
            Err(status) if retryable(&status) && Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(status) => {
                return Err(invalid(format!(
                    "realm index source write for {path} failed with {:?}: {}",
                    status.code(),
                    status.message()
                )));
            }
        }
    };
    if receipt.version == 0 || receipt.deleted {
        return Err(invalid(format!(
            "realm index source write for {path} returned invalid receipt"
        )));
    }
    Ok(())
}

async fn wait_for_every_endpoint(
    clients: &mut [IndexClient],
    bucket: &str,
    user: &str,
    expected: &BTreeSet<&str>,
    minimum_authorization_revision: u64,
) -> TestResult<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        let mut all_match = true;
        let mut observations = Vec::with_capacity(clients.len());
        for (ordinal, client) in clients.iter_mut().enumerate() {
            match client
                .query_index(query_request(bucket, user, 100, Vec::new()))
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    let actual = response
                        .hits
                        .iter()
                        .filter_map(|hit| hit.address.as_ref().map(|address| address.path.as_str()))
                        .collect::<BTreeSet<_>>();
                    let authorization = response
                        .freshness
                        .as_ref()
                        .and_then(|freshness| freshness.result_authorization.as_ref());
                    let matches = &actual == expected
                        && response
                            .freshness
                            .as_ref()
                            .is_some_and(|freshness| freshness.initial_build_complete)
                        && authorization.is_some_and(|evidence| {
                            evidence.realm == REALM
                                && evidence.authorization_revision >= minimum_authorization_revision
                                && evidence.binding_generation != 0
                                && evidence.schema_ref.as_ref().is_some_and(|schema| {
                                    schema.schema_revision != 0 && !schema.schema_digest.is_empty()
                                })
                        });
                    observations.push(format!(
                        "endpoint {}: hits={actual:?}, freshness={:?}",
                        ordinal + 1,
                        response.freshness
                    ));
                    if !matches {
                        all_match = false;
                    }
                }
                Err(status) if retryable(&status) => {
                    all_match = false;
                    observations.push(format!(
                        "endpoint {}: {:?}: {}",
                        ordinal + 1,
                        status.code(),
                        status.message()
                    ));
                }
                Err(status) => return Err(status.into()),
            }
        }
        if all_match {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "realm-filtered query for {user} did not converge to {expected:?} on every endpoint; last observations: {}",
                observations.join("; ")
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn require_subject_and_token_fail_closed(
    client: &mut IndexClient,
    bucket: &str,
) -> TestResult<()> {
    let missing = QueryIndexRequest {
        authorization_subject: None,
        ..query_request(bucket, ALICE, 100, Vec::new())
    };
    require_status(
        client
            .query_index(missing)
            .await
            .expect_err("realm-authorized query accepted no subject"),
        Code::InvalidArgument,
        "query without authorization subject",
    )?;

    let first = client
        .query_index(query_request(bucket, ALICE, 1, Vec::new()))
        .await?
        .into_inner();
    if first.next_page_token.is_empty() {
        return Err(invalid(
            "realm-filtered pagination did not produce a continuation token",
        ));
    }
    require_status(
        client
            .query_index(query_request(bucket, BOB, 1, first.next_page_token))
            .await
            .expect_err("another subject replayed a realm-bound page token"),
        Code::InvalidArgument,
        "page token replay by another subject",
    )
}

fn query_request(bucket: &str, user: &str, limit: u32, page_token: Vec<u8>) -> QueryIndexRequest {
    QueryIndexRequest {
        bucket: bucket.into(),
        index_name: INDEX_NAME.into(),
        query: Some(IndexQuery {
            query: Some(QueryValue::TypedJson(TypedJsonIndexQuery {
                predicate: Some(IndexPredicateExpression::leaf(IndexPredicate {
                    field: "qualified".into(),
                    operator: IndexPredicateOperator::Equal as i32,
                    values_json: vec![b"true".to_vec()],
                })),
                order: Vec::new(),
                facets: Vec::new(),
                aggregates: Vec::new(),
            })),
        }),
        limit,
        page_token,
        tenant: String::new(),
        required_freshness: None,
        authorization_subject: Some(user_subject(user)),
    }
}

fn index_client(
    channel: Channel,
    token: &str,
) -> Result<IndexClient, tonic::metadata::errors::InvalidMetadataValue> {
    Ok(IndexServiceClient::with_interceptor(
        channel,
        BearerToken::new(token)?,
    ))
}

fn retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::NotFound | Code::FailedPrecondition
    ) || (status.code() == Code::Cancelled && status.message() == "Timeout expired")
}

fn require_status(status: Status, expected: Code, context: &str) -> TestResult<()> {
    if status.code() == expected {
        Ok(())
    } else {
        Err(invalid(format!(
            "{context} returned {:?}, expected {expected:?}",
            status.code()
        )))
    }
}

fn required(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| invalid(format!("{name} must be set")))
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
