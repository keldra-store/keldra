use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use serde_json::{Value, json};
use tokio::time::{Duration, timeout};

use super::*;

const PROGRAM_PATH_HASH: [u8; 32] = [0x11; 32];

#[derive(Debug, Clone)]
struct TestReader {
    reads: Arc<AtomicUsize>,
    snapshot: Arc<Mutex<ProgramSnapshot>>,
    requested: Arc<Mutex<Vec<ObjectPath>>>,
}

impl TestReader {
    fn new(snapshot: ProgramSnapshot) -> Self {
        Self {
            reads: Arc::new(AtomicUsize::new(0)),
            snapshot: Arc::new(Mutex::new(snapshot)),
            requested: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl StateReader for TestReader {
    async fn read_snapshot(
        &self,
        document_paths: &[ObjectPath],
    ) -> Result<ProgramSnapshot, String> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        *self.requested.lock().unwrap() = document_paths.to_vec();
        Ok(self.snapshot.lock().unwrap().clone())
    }
}

fn reference(slot: &str) -> DocumentRef {
    DocumentRef::one(slot)
}

fn pointer(slot: &str, pointer: &str) -> JsonPointerRef {
    JsonPointerRef::new(reference(slot), pointer)
}

fn document_value(slot: &str, pointer: &str, view: DocumentView) -> DocumentValueRef {
    DocumentValueRef {
        value: self::pointer(slot, pointer),
        view,
    }
}

fn definition() -> ProgramDefinition {
    ProgramDefinition {
        schema_version: DEFINITION_SCHEMA_VERSION,
        documents: vec![
            DocumentSpec {
                name: "account".into(),
                path: PathTemplate::new("{tenant}", "objects", "account"),
                cardinality: Cardinality::One,
                access: DocumentAccess::ReadWrite,
                allow_initial_json: false,
            },
            DocumentSpec {
                name: "summary".into(),
                path: PathTemplate::new("{tenant}", "objects", "summary"),
                cardinality: Cardinality::One,
                access: DocumentAccess::ReadWrite,
                allow_initial_json: false,
            },
            DocumentSpec {
                name: "ledger".into(),
                path: PathTemplate::new("{tenant}", "objects", "ledger/{entry}"),
                cardinality: Cardinality::Repeated { max: 2 },
                access: DocumentAccess::ReadWrite,
                allow_initial_json: true,
            },
        ],
        assertions: vec![
            Assertion::Exists {
                document: reference("account"),
            },
            Assertion::IntegerCompare {
                actual: pointer("account", "/balance"),
                comparison: Comparison::Ge,
                expected: InputValue::Input {
                    name: "minimum_balance".into(),
                },
                numeric_type: IntegerType::I64 {
                    min: None,
                    max: None,
                },
            },
        ],
        operations: vec![
            Operation::CheckedIntegerAdd {
                target: pointer("account", "/event_count"),
                delta: InputValue::Literal { value: json!(1) },
                numeric_type: IntegerType::U64 {
                    min: Some(0),
                    max: None,
                },
            },
            Operation::CheckedIntegerAdd {
                target: pointer("account", "/quantity"),
                delta: InputValue::Input {
                    name: "quantity_delta".into(),
                },
                numeric_type: IntegerType::U64 {
                    min: Some(0),
                    max: None,
                },
            },
            Operation::CheckedIntegerAdd {
                target: pointer("account", "/balance"),
                delta: InputValue::Input {
                    name: "balance_delta".into(),
                },
                numeric_type: IntegerType::I64 {
                    min: Some(-1_000_000),
                    max: Some(1_000_000),
                },
            },
            Operation::CopyValue {
                source: document_value("account", "/balance", DocumentView::Before),
                target: pointer("ledger", "/balance_before"),
            },
            Operation::CopyValue {
                source: document_value("account", "/balance", DocumentView::Current),
                target: pointer("ledger", "/balance_after"),
            },
            Operation::SetValue {
                target: pointer("summary", "/last_balance"),
                value: ValueSource::Document {
                    source: document_value("account", "/balance", DocumentView::Current),
                },
            },
            Operation::RemoveValue {
                target: pointer("summary", "/obsolete"),
            },
        ],
        returns: vec![
            ReturnDefinition {
                name: "balance".into(),
                value: document_value("account", "/balance", DocumentView::Current),
            },
            ReturnDefinition {
                name: "original_balance".into(),
                value: document_value("account", "/balance", DocumentView::Before),
            },
        ],
        caps: ProgramCaps {
            max_paths: 5,
            max_writes: 4,
            max_operations: 16,
            max_input_bytes: 1024 * 1024,
            max_document_bytes: 1024 * 1024,
        },
    }
}

fn invocation() -> ProgramInvocation {
    ProgramInvocation {
        program_path_hash: PROGRAM_PATH_HASH,
        command_id: "cmd-001".into(),
        input_fingerprint: "a".repeat(64),
        arguments: Default::default(),
        inputs: [
            ("minimum_balance".into(), json!(0)),
            ("quantity_delta".into(), json!(-3)),
            ("balance_delta".into(), json!(-25)),
        ]
        .into_iter()
        .collect(),
        blobs: Default::default(),
        bindings: [
            (
                "account".into(),
                vec![PathBinding {
                    path: path("account"),
                    template_values: Default::default(),
                    expected_head: ExpectedHead::Version {
                        version: "a1".into(),
                    },
                    initial_json: None,
                }],
            ),
            (
                "summary".into(),
                vec![PathBinding {
                    path: path("summary"),
                    template_values: Default::default(),
                    expected_head: ExpectedHead::Any,
                    initial_json: None,
                }],
            ),
            (
                "ledger".into(),
                vec![PathBinding {
                    path: path("ledger/event-7"),
                    template_values: [("entry".into(), "event-7".into())].into_iter().collect(),
                    expected_head: ExpectedHead::Absent,
                    initial_json: Some(json!({
                        "balance_before": null,
                        "balance_after": null,
                    })),
                }],
            ),
        ]
        .into_iter()
        .collect(),
    }
}

fn path(key: &str) -> ObjectPath {
    ObjectPath::new("acme", "objects", key).unwrap()
}

fn context() -> InvocationContext {
    InvocationContext::new("acme").unwrap()
}

fn snapshot() -> ProgramSnapshot {
    ProgramSnapshot {
        documents: [
            (
                path("account"),
                VersionedDocument {
                    version: "a1".into(),
                    value: Some(StoredValue::Json(json!({
                        "event_count": 7,
                        "quantity": 10,
                        "balance": 100,
                    }))),
                    content_type: Some("application/json".into()),
                },
            ),
            (
                path("summary"),
                VersionedDocument {
                    version: "s4".into(),
                    value: Some(StoredValue::Json(json!({
                        "last_balance": 0,
                        "obsolete": true,
                    }))),
                    content_type: Some("application/json".into()),
                },
            ),
        ]
        .into_iter()
        .collect(),
    }
}

fn write_json<'a>(bundle: &'a AtomicWriteBundle, key: &str) -> &'a Value {
    let write = bundle
        .writes
        .iter()
        .find(|write| write.path.path == key)
        .unwrap();
    match write.value.as_ref().unwrap() {
        StoredValue::Json(value) => value,
        StoredValue::Opaque(_) => panic!("expected JSON write"),
    }
}

#[test]
fn expanded_paths_are_available_for_authorization_before_reads_or_locks() {
    let reader = TestReader::new(snapshot());
    let reads = reader.reads.clone();
    let engine = AtomicProgramEngine::new(definition(), reader).unwrap();

    let paths = engine.expanded_paths(&context(), &invocation()).unwrap();

    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(paths.len(), 3);
    assert!(paths.iter().all(|candidate| candidate.intent.get));
    assert!(paths.iter().all(|candidate| candidate.intent.put));
    assert!(paths.iter().all(|candidate| !candidate.intent.delete));
}

#[test]
fn expanded_intent_distinguishes_put_delete_and_unused_write_permission() {
    let mut definition = definition();
    definition.documents.push(DocumentSpec {
        name: "deleted".into(),
        path: PathTemplate::new("{tenant}", "objects", "deleted"),
        cardinality: Cardinality::One,
        access: DocumentAccess::ReadWrite,
        allow_initial_json: false,
    });
    definition.documents.push(DocumentSpec {
        name: "unused".into(),
        path: PathTemplate::new("{tenant}", "objects", "unused"),
        cardinality: Cardinality::One,
        access: DocumentAccess::ReadWrite,
        allow_initial_json: false,
    });
    definition.operations.push(Operation::RemoveValue {
        target: pointer("deleted", ""),
    });
    definition.caps.max_paths = 7;
    definition.caps.max_writes = 6;

    let mut invocation = invocation();
    invocation.bindings.insert(
        "deleted".into(),
        vec![PathBinding {
            path: path("deleted"),
            template_values: Default::default(),
            expected_head: ExpectedHead::Any,
            initial_json: None,
        }],
    );
    invocation.bindings.insert(
        "unused".into(),
        vec![PathBinding {
            path: path("unused"),
            template_values: Default::default(),
            expected_head: ExpectedHead::Any,
            initial_json: None,
        }],
    );

    let engine = AtomicProgramEngine::new(definition, TestReader::new(snapshot())).unwrap();
    let paths = engine.expanded_paths(&context(), &invocation).unwrap();
    let deleted = paths
        .iter()
        .find(|candidate| candidate.path.path == "deleted")
        .unwrap();
    assert_eq!(
        deleted.intent,
        ProgramPathIntent {
            get: true,
            put: false,
            delete: true,
        }
    );
    let unused = paths
        .iter()
        .find(|candidate| candidate.path.path == "unused")
        .unwrap();
    assert_eq!(
        unused.intent,
        ProgramPathIntent {
            get: true,
            put: false,
            delete: false,
        }
    );
}

#[tokio::test]
async fn evaluates_typed_updates_copy_views_and_outputs() {
    let reader = TestReader::new(snapshot());
    let reader_handle = reader.clone();
    let engine = AtomicProgramEngine::new(definition(), reader).unwrap();

    let bundle = engine
        .prepare(&context(), &invocation())
        .await
        .unwrap()
        .release();

    assert_eq!(reader_handle.reads.load(Ordering::SeqCst), 1);
    assert_eq!(bundle.head_preconditions.len(), 3);
    assert_eq!(bundle.writes.len(), 3);

    assert_eq!(
        write_json(&bundle, "account"),
        &json!({"event_count": 8, "quantity": 7, "balance": 75})
    );
    assert_eq!(
        write_json(&bundle, "ledger/event-7"),
        &json!({"balance_before": 100, "balance_after": 75})
    );
    assert_eq!(write_json(&bundle, "summary"), &json!({"last_balance": 75}));
    assert_eq!(bundle.outputs["balance"], json!(75));
    assert_eq!(bundle.outputs["original_balance"], json!(100));
    assert_eq!(bundle.receipt.outputs, bundle.outputs);

    let requested = reader_handle.requested.lock().unwrap().clone();
    assert!(requested.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn accepts_any_bounded_path_matching_the_registered_template() {
    let reader = TestReader::new(snapshot());
    let reader_handle = reader.clone();
    let engine = AtomicProgramEngine::new(definition(), reader).unwrap();
    let mut invocation = invocation();
    let ledger = &mut invocation.bindings.get_mut("ledger").unwrap()[0];
    ledger.path = path("ledger/remote");
    ledger
        .template_values
        .insert("entry".into(), "remote".into());

    let lease = engine.prepare(&context(), &invocation).await.unwrap();
    assert_eq!(lease.bundle().writes.len(), 3);
    assert_eq!(reader_handle.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn tenant_comes_from_authenticated_context_before_locking_or_reading() {
    let reader = TestReader::new(snapshot());
    let reader_handle = reader.clone();
    let engine = AtomicProgramEngine::new(definition(), reader).unwrap();

    let wrong_tenant = InvocationContext::new("other-tenant").unwrap();
    let error = engine
        .prepare(&wrong_tenant, &invocation())
        .await
        .unwrap_err();
    assert!(matches!(error, EngineError::InvalidInvocation(_)));
    assert_eq!(reader_handle.reads.load(Ordering::SeqCst), 0);

    let mut injected = invocation();
    injected.arguments.insert("tenant".into(), "acme".into());
    let error = engine.prepare(&context(), &injected).await.unwrap_err();
    assert!(matches!(error, EngineError::InvalidInvocation(_)));
    assert_eq!(reader_handle.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn locks_every_resolved_document_path_during_preparation() {
    let locks = LocalLockManager::default();
    let engine = AtomicProgramEngine::with_lock_manager(
        definition(),
        TestReader::new(snapshot()),
        locks.clone(),
    )
    .unwrap();

    let lease = engine.prepare(&context(), &invocation()).await.unwrap();
    let expected = [path("account"), path("ledger/event-7"), path("summary")];

    for path in expected {
        let competing_locks = locks.clone();
        let mut task = tokio::spawn(async move { competing_locks.acquire(&[path]).await });
        assert!(timeout(Duration::from_millis(20), &mut task).await.is_err());
        task.abort();
    }

    drop(lease);
}

#[tokio::test]
async fn recreating_a_tombstone_preserves_its_version_as_the_cas_boundary() {
    let mut state = snapshot();
    state.documents.insert(
        path("ledger/event-7"),
        VersionedDocument {
            version: "deleted-6".into(),
            value: None,
            content_type: None,
        },
    );
    let engine = AtomicProgramEngine::new(definition(), TestReader::new(state)).unwrap();

    let bundle = engine
        .prepare(&context(), &invocation())
        .await
        .unwrap()
        .release();
    let ledger = bundle
        .writes
        .iter()
        .find(|write| write.path.path == "ledger/event-7")
        .unwrap();
    assert_eq!(
        ledger.expected,
        ObservedHead::Version {
            version: "deleted-6".into()
        }
    );
}

#[tokio::test]
async fn rejects_unbounded_or_mismatched_bindings_before_reading() {
    let reader = TestReader::new(snapshot());
    let reader_handle = reader.clone();
    let engine = AtomicProgramEngine::new(definition(), reader).unwrap();
    let mut too_many = invocation();
    let first = too_many.bindings["ledger"][0].clone();
    too_many
        .bindings
        .get_mut("ledger")
        .unwrap()
        .extend([first.clone(), first]);

    let error = engine.prepare(&context(), &too_many).await.unwrap_err();
    assert!(matches!(error, EngineError::InvalidInvocation(_)));
    assert_eq!(reader_handle.reads.load(Ordering::SeqCst), 0);

    let mut mismatch = invocation();
    mismatch.bindings.get_mut("account").unwrap()[0].path = path("wrong/path");
    let error = engine.prepare(&context(), &mismatch).await.unwrap_err();
    assert!(matches!(error, EngineError::InvalidInvocation(_)));
    assert_eq!(reader_handle.reads.load(Ordering::SeqCst), 0);
}

#[test]
fn removed_emission_schema_is_rejected_instead_of_silently_ignored() {
    let mut with_emissions = serde_json::to_value(definition()).unwrap();
    with_emissions
        .as_object_mut()
        .unwrap()
        .insert("emissions".into(), json!([]));
    let error = serde_json::from_value::<ProgramDefinition>(with_emissions).unwrap_err();
    assert!(error.to_string().contains("unknown field `emissions`"));

    let mut with_emission_cap = serde_json::to_value(definition()).unwrap();
    with_emission_cap["caps"]
        .as_object_mut()
        .unwrap()
        .insert("max_emissions".into(), json!(1));
    let error = serde_json::from_value::<ProgramDefinition>(with_emission_cap).unwrap_err();
    assert!(error.to_string().contains("unknown field `max_emissions`"));
}

#[tokio::test]
async fn rejects_float_arithmetic_and_unsigned_underflow() {
    let engine = AtomicProgramEngine::new(definition(), TestReader::new(snapshot())).unwrap();
    let mut float = invocation();
    float.inputs.insert("balance_delta".into(), json!(-0.5));
    assert!(matches!(
        engine.prepare(&context(), &float).await.unwrap_err(),
        EngineError::Operation { index: 2, .. }
    ));

    let engine = AtomicProgramEngine::new(definition(), TestReader::new(snapshot())).unwrap();
    let mut underflow = invocation();
    underflow.inputs.insert("quantity_delta".into(), json!(-11));
    assert!(matches!(
        engine.prepare(&context(), &underflow).await.unwrap_err(),
        EngineError::Operation { index: 1, .. }
    ));
}

#[tokio::test]
async fn local_locks_are_canonical_and_serialize_reverse_order_requests() {
    let locks = LocalLockManager::default();
    let a = path("a");
    let b = path("b");
    let first = locks.acquire(&[b.clone(), a.clone()]).await;
    assert_eq!(first.paths(), &[a.clone(), b.clone()]);

    let competing_locks = locks.clone();
    let competing_a = a.clone();
    let competing_b = b.clone();
    let mut task =
        tokio::spawn(async move { competing_locks.acquire(&[competing_a, competing_b]).await });
    assert!(timeout(Duration::from_millis(20), &mut task).await.is_err());
    drop(first);
    let second = timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.paths(), &[a, b]);
}

#[tokio::test]
async fn cancelling_a_partial_lock_wait_releases_every_acquired_lock() {
    let locks = LocalLockManager::default();
    let a = path("a");
    let b = path("b");
    let held_b = locks.acquire(std::slice::from_ref(&b)).await;

    let waiting_locks = locks.clone();
    let waiting_a = a.clone();
    let waiting_b = b.clone();
    let waiting = tokio::spawn(async move { waiting_locks.acquire(&[waiting_a, waiting_b]).await });
    tokio::task::yield_now().await;
    assert!(
        timeout(
            Duration::from_millis(20),
            locks.acquire(std::slice::from_ref(&a))
        )
        .await
        .is_err(),
        "the waiter must acquire the first canonical lock before blocking on the second"
    );
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());

    let acquired_a = timeout(
        Duration::from_secs(1),
        locks.acquire(std::slice::from_ref(&a)),
    )
    .await
    .expect("cancelling the waiter must release its already-acquired first lock");
    drop(acquired_a);
    drop(held_b);

    timeout(Duration::from_secs(1), locks.acquire(&[a, b]))
        .await
        .expect("cancelling the waiter must not leave either lock held");
}

#[test]
fn definition_caps_are_registration_time_contracts() {
    let mut invalid = definition();
    invalid.caps.max_paths = 3;
    assert!(matches!(
        invalid.validate().unwrap_err(),
        EngineError::InvalidDefinition(_)
    ));

    let mut invalid = definition();
    invalid.operations.push(Operation::SetValue {
        target: pointer("missing", ""),
        value: ValueSource::Literal { value: Value::Null },
    });
    assert!(matches!(
        invalid.validate().unwrap_err(),
        EngineError::InvalidDefinition(_)
    ));
}

#[test]
fn server_constructed_input_fingerprints_are_canonical() {
    let first = ProgramInput {
        arguments: BTreeMap::from([("account".into(), "a-1".into())]),
        inputs: BTreeMap::from([("delta".into(), json!(50))]),
        ..Default::default()
    };
    let second = first.clone();
    let left = ProgramInvocation::from_input(PROGRAM_PATH_HASH, "command-a", first).unwrap();
    let right = ProgramInvocation::from_input(PROGRAM_PATH_HASH, "command-b", second).unwrap();
    assert_eq!(left.input_fingerprint, right.input_fingerprint);

    let changed = ProgramInvocation::from_input(
        PROGRAM_PATH_HASH,
        "command-a",
        ProgramInput {
            arguments: BTreeMap::from([("account".into(), "a-1".into())]),
            inputs: BTreeMap::from([("delta".into(), json!(-300))]),
            ..Default::default()
        },
    )
    .unwrap();
    assert_ne!(left.input_fingerprint, changed.input_fingerprint);

    let other_program_object = ProgramInvocation::from_input(
        [0x22; 32],
        "command-a",
        ProgramInput {
            arguments: BTreeMap::from([("account".into(), "a-1".into())]),
            inputs: BTreeMap::from([("delta".into(), json!(50))]),
            ..Default::default()
        },
    )
    .unwrap();
    assert_ne!(
        left.input_fingerprint,
        other_program_object.input_fingerprint
    );
}

#[test]
fn object_paths_enforce_the_storage_kernels_canonical_address_rules() {
    assert!(
        ObjectPath::new(
            "t".repeat(MAX_OBJECT_TENANT_BYTES),
            "b".repeat(MAX_OBJECT_BUCKET_BYTES),
            "p".repeat(MAX_OBJECT_PATH_BYTES),
        )
        .is_ok()
    );

    assert!(ObjectPath::new("t".repeat(MAX_OBJECT_TENANT_BYTES + 1), "bucket", "path",).is_err());
    assert!(ObjectPath::new("tenant", "b".repeat(MAX_OBJECT_BUCKET_BYTES + 1), "path",).is_err());
    assert!(ObjectPath::new("tenant", "bucket", "p".repeat(MAX_OBJECT_PATH_BYTES + 1),).is_err());

    for invalid in ["/a", "a/", "a//b", "a/./b", "a/../b", "a\nb"] {
        assert!(ObjectPath::new("tenant", "bucket", invalid).is_err());
    }
    assert!(ObjectPath::new("tenant/other", "bucket", "path").is_err());
    assert!(ObjectPath::new("tenant", "bad\tbucket", "path").is_err());

    // Braces are ordinary canonical component characters once a template has
    // already been parsed; only separators and controls are forbidden.
    assert!(ObjectPath::new("tenant{one}", "bucket{two}", "path/{three}").is_ok());
}

#[test]
fn invocation_context_uses_the_same_tenant_component_rules() {
    assert!(InvocationContext::new("t".repeat(MAX_OBJECT_TENANT_BYTES)).is_ok());
    assert!(InvocationContext::new("t".repeat(MAX_OBJECT_TENANT_BYTES + 1)).is_err());
    assert!(InvocationContext::new("tenant/other").is_err());
    assert!(InvocationContext::new("tenant\nother").is_err());
    assert!(InvocationContext::new("tenant{one}").is_ok());
}

#[test]
fn definition_json_rejects_misspelled_top_level_and_nested_fields() {
    let mut top_level = serde_json::to_value(definition()).unwrap();
    top_level
        .as_object_mut()
        .unwrap()
        .insert("schema_vesion".into(), json!(1));
    let error = serde_json::from_value::<ProgramDefinition>(top_level).unwrap_err();
    assert!(error.to_string().contains("unknown field"));
    assert!(error.to_string().contains("schema_vesion"));

    let mut nested = serde_json::to_value(definition()).unwrap();
    nested["operations"][0]
        .as_object_mut()
        .unwrap()
        .insert("targte".into(), json!({}));
    let error = serde_json::from_value::<ProgramDefinition>(nested).unwrap_err();
    assert!(error.to_string().contains("unknown field"));
    assert!(error.to_string().contains("targte"));
}

#[test]
fn invocation_input_json_rejects_misspelled_top_level_and_nested_fields() {
    let input = ProgramInput {
        bindings: BTreeMap::from([(
            "account".into(),
            vec![PathBinding {
                path: path("account"),
                template_values: BTreeMap::new(),
                expected_head: ExpectedHead::Version {
                    version: "a1".into(),
                },
                initial_json: None,
            }],
        )]),
        ..Default::default()
    };

    let mut top_level = serde_json::to_value(&input).unwrap();
    top_level
        .as_object_mut()
        .unwrap()
        .insert("argumnts".into(), json!({}));
    let error = serde_json::from_value::<ProgramInput>(top_level).unwrap_err();
    assert!(error.to_string().contains("unknown field"));
    assert!(error.to_string().contains("argumnts"));

    let mut nested = serde_json::to_value(input).unwrap();
    nested["bindings"]["account"][0]["expected_head"]
        .as_object_mut()
        .unwrap()
        .insert("versoin".into(), json!("a1"));
    let error = serde_json::from_value::<ProgramInput>(nested).unwrap_err();
    assert!(error.to_string().contains("unknown field"));
    assert!(error.to_string().contains("versoin"));
}
