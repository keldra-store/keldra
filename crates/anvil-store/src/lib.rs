//! Anvil's storage kernel.
//!
//! The kernel deliberately has no transaction lifecycle, snapshot protocol or
//! knowledge of payload contents. It stores immutable object versions and
//! atomically moves current heads subject to small CAS preconditions.

mod authz;
mod blob;
mod blob_gc;
mod bootstrap;
mod clock;
mod credential_secret;
mod definition_state;
mod derived_consumer;
mod erasure;
mod journal_route;
mod key;
mod logical_record;
mod model;
mod program;
mod reference_delta;
mod store;
mod watch;

pub use authz::{
    AUTHZ_REALM_MUTATION_FORMAT, AUTHZ_REALM_MUTATION_STAMP_FORMAT, AUTHZ_REALM_SNAPSHOT_FORMAT,
    AUTHZ_REALM_TRANSFER_MANIFEST_FORMAT, AtomicRealmBinding, AuthzBatchCheck, AuthzConsistency,
    AuthzRealmAggregate, AuthzRealmChange, AuthzRealmCursor, AuthzRealmKeyPage, AuthzRealmMutation,
    AuthzRealmMutationContext, AuthzRealmMutationStamp, AuthzRealmSchema,
    AuthzRealmSnapshotApplied, AuthzRealmSnapshotError, AuthzRealmTransferManifest,
    AuthzRepository, AuthzRevision, AuthzSchemaCatalogue, AuthzSchemaCatalogueApplied,
    AuthzSchemaCatalogueCandidate, AuthzSchemaPublicationMutation, AuthzSchemaPublicationStamp,
    AuthzSchemaRevision, AuthzScope, AuthzStoreError, AuthzStoreLimits, BindSchemaRequest,
    BoundRealm, CoordinatedAuthzRealmMutation, CoordinatedAuthzRealmResult,
    CoordinatedAuthzSchemaPublication, DEFAULT_AUTHZ_RECEIPT_MAX_BYTES,
    DEFAULT_AUTHZ_RECEIPT_MAX_ENTRIES, DEFAULT_AUTHZ_RECEIPT_RETENTION_SECONDS,
    MAX_AUTHZ_REALM_EXPORT_BYTES, MAX_AUTHZ_REALM_EXPORT_RECORDS, ProtectedRealmOwnership,
    PublishSchemaRequest, PublishedSchema, RealmBinding, RealmSnapshot,
    ReplicaAuthzRealmMutationApplied, ReplicaAuthzSchemaPublicationApplied,
    SYSTEM_STORAGE_TENANT_ID, SchemaDigest, SchemaId, SchemaRef, StorageTenantId,
    TupleBatchReceipt, TupleBatchRequest, TupleMutation, TupleMutationKind,
};
pub use blob::{AWAITING_PUBLISH, BlobReader, BlobRef, BlobReferenceState, BlobStore, BlobUpload};
pub use blob_gc::{BlobGcBudget, BlobGcCursor, BlobGcTick};
pub use bootstrap::{
    ApplicationCredential, ApplicationCredentialRequest, ApplicationRoleTarget,
    BucketApplicationRole, CreateBucketReceipt, CreateBucketRequest, CredentialMutationReceipt,
    CredentialRepository, CredentialRepositoryError, PreparedApplicationCredential,
    PreparedBucketCreation, PreparedTenantProvisioning, ProvisionTenantReceipt,
    ProvisionTenantRequest, SYSTEM_BOOTSTRAP_VERSION, SYSTEM_SCHEMA_ID, SetApplicationRoleReceipt,
    SetApplicationRoleRequest, SetBucketPublicReadRequest, SystemApplicationRole,
    SystemBootstrapError, SystemBootstrapRequest, SystemBootstrapState, TenantApplicationRole,
    system_schema,
};
pub use clock::VersionClock;
pub use credential_secret::CredentialSecretEnvelope;
pub use definition_state::{
    DefinitionAssignment, DefinitionAssignmentCursor, DefinitionAssignmentMutation,
    DefinitionAssignmentPage, DefinitionCheckpoint, DefinitionConsumerKind, DefinitionKind,
    DefinitionLocator, DefinitionLocatorCursor, DefinitionLocatorPage, DefinitionMutationIntent,
    DefinitionOperation, DefinitionStateError, DefinitionTransition,
    MAX_DEFINITION_STATE_SCAN_RECORDS,
};
pub use derived_consumer::{
    DerivedConsumerCheckpoint, DerivedConsumerError, DerivedConsumerKind, DerivedConsumerStatus,
    MAX_DERIVED_CONSUMER_NODES, SourceJournalRuntimeMetrics,
};
pub use erasure::{
    DEFAULT_ERASURE_DATA_SHARDS, DEFAULT_ERASURE_PARITY_SHARDS, DEFAULT_ERASURE_STRIPE_UNIT_BYTES,
    ErasureCodec, ErasureError, ErasureProfile, FRAGMENT_FORMAT_VERSION,
};
pub use journal_route::{JournalRoute, RoutedJournalError, RoutedLocalChangePage};
pub use key::ObjectKey;
pub use logical_record::{
    BaselineHash, LOGICAL_RECORD_FORMAT, LogicalApplicationRecord, LogicalBucketRecord,
    LogicalCredentialRecord, LogicalRecordApplied, LogicalRecordCandidate, LogicalRecordCursor,
    LogicalRecordError, LogicalRecordExport, LogicalRecordExportPage, LogicalRecordId,
    LogicalRecordMutation, LogicalRecordMutationContext, LogicalRecordPredecessor,
    LogicalRecordSnapshotApplied, LogicalRecordValue, LogicalTenantRecord, LogicalTenantSchema,
    MAX_LOGICAL_RECORD_EXPORT_BYTES, MAX_LOGICAL_RECORD_EXPORT_RECORDS,
};
pub use model::{
    BatchOperation, BatchOutcome, BucketPolicy, CoordinatedObjectMutation,
    CoordinatedRetainedVersionDelete, DeleteRequest, DeleteRetainedVersionOutcome, Durability,
    Head, MAX_BUCKET_POLICY_PREFIX_BYTES, MAX_BUCKET_POLICY_PREFIXES, MAX_CONTENT_TYPE_BYTES,
    MAX_OBJECT_MUTATION_REFERENCE_DELTAS, MUTATION_STAMP_FORMAT, MutationError, MutationReceipt,
    MutationStamp, OBJECT_MUTATION_FORMAT, Object, ObjectMutation, ObjectMutationContext,
    ObjectMutationGovernance, ObjectVersioning, PlacementLogId, Precondition, PublishRequest,
    PutMode, PutRequest, RETAINED_VERSION_DELETE_FORMAT, ReplicaObjectMutationApplied,
    ReplicaRetainedVersionDeleteApplied, RetainedVersionDeleteMutation, SMALL_BLOB_MAX_BYTES,
    Version, VersionId,
};
pub use program::{
    AppliedProgramCommit, CommittedProgramResult, CoordinatedProgramPathFinalization,
    PreparedBundleHash, PreparedBundleRef, PreparedProgramBundle, PreparedProgramRecord,
    PreparedProgramWrite, ProgramCommit, ProgramDurabilityClassHash, ProgramDurabilityEvidence,
    ProgramDurabilityEvidenceHash, ProgramDurabilityScope, ProgramExecutionLease, ProgramHash,
    ProgramPathMutation, ProgramPathStage, ProgramStoreError, PublishedProgramVersion,
    ReplicaProgramPathApplied, StoreProgramEngine, VerifiedProgramDefinition,
    path_stage_from_prepared,
};
pub use reference_delta::{
    DestinationReferenceArtifact, DestinationReferenceDelta, ReferenceDelta, ReferenceDeltaApplied,
    ReferenceDeltaBatch, ReferenceDeltaError,
};
pub use store::{
    BatchGetSelection, CompleteCopySealOutcome, CurrentHeadCursor, CurrentObjectSnapshot,
    CurrentObjectSnapshotFrame, CurrentObjectSnapshotPage, CurrentObjectSnapshotScan,
    DEFAULT_AWAITING_PUBLISH_TTL_SECONDS, DEFAULT_MUTATION_RECEIPT_MAX_BYTES,
    DEFAULT_MUTATION_RECEIPT_MAX_ENTRIES, DEFAULT_MUTATION_RECEIPT_RETENTION_SECONDS,
    ListObjectsPage, LocalPayloadPresence, MAX_CURRENT_HEAD_SNAPSHOT_BYTES,
    MAX_CURRENT_HEAD_SNAPSHOT_RECORDS, MAX_LIST_OBJECT_VERSIONS, MAX_LIST_OBJECTS,
    MAX_OBJECT_RECORD_EXPORT_BYTES, MAX_OBJECT_RECORD_EXPORT_RECORDS,
    MAX_PAYLOAD_HANDOFF_EXPORT_RECORDS, MAX_REFERENCE_PROOF_EXPORT_BYTES,
    MAX_REFERENCE_PROOF_EXPORT_RECORDS, MAX_REFERENCE_PROOF_PRUNE_BYTES,
    MAX_REFERENCE_PROOF_PRUNE_RECORDS, MetadataRuntimeMetrics, MutationReceiptRetention,
    ObjectPathSnapshot, ObjectRecordCursor, ObjectRecordExport, ObjectRecordExportPage,
    ObjectSnapshotApplied, ObjectSnapshotError, OpenedObject, PayloadArtifactCursor,
    PayloadArtifactIdentity, PayloadArtifactSnapshot, PayloadArtifactSnapshotPage,
    PayloadArtifactState, PayloadStoreError, ReferenceProofCursor, ReferenceProofExportError,
    ReferenceProofPage, ReferenceProofPruneError, ReferenceProofPruneResult, RetainedHeadState,
    RetainedObjectCursor, RetainedObjectSnapshot, RetainedObjectSnapshotFrame,
    RetainedObjectSnapshotPage, RetainedObjectSnapshotScan, RetainedVersionCursor, ShardIdentity,
    ShardReader, ShardSealOutcome, ShardStoreError, Store, StoreOptions,
};
pub use watch::{
    AccountingHeadTransition, AggregateChanged, AggregateKind, ContentLifecycleChanged,
    DEFAULT_WATCH_MAX_BYTES, DEFAULT_WATCH_MAX_ENTRIES, InvalidationStateHint, LocalChange,
    LocalChangePage, LocalInvalidation, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, ObjectHeadChange,
    ObjectHeadChangeKind, OversizeLocalChange, ReferenceProof, ReferenceProofMutation,
    RetainedVersionDeletedChange, SourceId, WatchCursor, WatchError, WatchJournalStatus, WatchPage,
    WatchRetention, WatchScope, WatchStart,
};
