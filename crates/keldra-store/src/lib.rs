//! Keldra's storage kernel.
//!
//! The kernel deliberately has no transaction lifecycle, snapshot protocol or
//! knowledge of payload contents. It stores immutable object versions and
//! atomically moves current heads subject to small CAS preconditions.

/// Reserved ordinary-object prefix containing current index definitions.
///
/// The storage kernel owns exact prefix recognition for its bounded
/// bucket-scoped iterator. Definition-name validation remains a server
/// concern.
pub const INDEX_DEFINITION_PREFIX: &str = "_keldra/indices/v4/definitions/";

mod authz;
mod blob;
mod blob_gc;
mod bootstrap;
mod clock;
mod credential_secret;
mod definition_state;
mod derived_consumer;
mod erasure;
mod index_orphan_scrub_due;
mod index_retention_due;
mod journal_route;
mod key;
mod logical_record;
mod model;
mod object_link;
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
    DefinitionAssignmentPage, DefinitionCheckpoint, DefinitionConsumerKind, DefinitionDeletion,
    DefinitionKind, DefinitionLocator, DefinitionLocatorCursor, DefinitionLocatorPage,
    DefinitionMutationIntent, DefinitionOperation, DefinitionStateError, DefinitionTransition,
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
pub use index_orphan_scrub_due::{
    IndexOrphanScrubDue, IndexOrphanScrubDueError, MAX_INDEX_ORPHAN_CURSOR_BYTES,
};
pub use index_retention_due::{
    DeletedDefinitionCleanup, IndexCommitRetentionDue, IndexRetentionDueError,
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
    BatchOperation, BatchOutcome, BucketPolicy, CloneRequest, CoordinatedObjectMutation,
    CoordinatedRetainedVersionDelete, DeleteRequest, DeleteRetainedVersionOutcome, Durability,
    Head, LEGACY_OBJECT_MUTATION_FORMAT, MAX_BUCKET_POLICY_PREFIX_BYTES,
    MAX_BUCKET_POLICY_PREFIXES, MAX_CONTENT_TYPE_BYTES, MAX_OBJECT_MUTATION_REFERENCE_DELTAS,
    MUTATION_STAMP_FORMAT, MutationError, MutationReceipt, MutationStamp,
    OBJECT_ALIAS_REGISTRY_FORMAT, OBJECT_ALIAS_REGISTRY_TRANSITION_FORMAT, OBJECT_MUTATION_FORMAT,
    Object, ObjectAliasRegistry, ObjectAliasRegistryTransition, ObjectAliasSnapshot,
    ObjectMutation, ObjectMutationContext, ObjectMutationGovernance, ObjectVersioning,
    PlacementLogId, Precondition, PublishRequest, PutMode, PutRequest,
    RETAINED_VERSION_DELETE_FORMAT, ReplicaObjectMutationApplied,
    ReplicaRetainedVersionDeleteApplied, RetainedVersionDeleteMutation, SMALL_BLOB_MAX_BYTES,
    Version, VersionId,
};
pub use object_link::{
    MAX_INBOUND_OBJECT_LINKS, OBJECT_LINK_CONTENT_TYPE, ObjectLinkDescriptor, ObjectLinkError,
    ResolvedObjectLink, is_object_link_content_type, object_link_command_fingerprint,
    resolve_descriptor,
};
pub use program::{
    AppliedProgramCommit, BuiltInAliasObservation, BuiltInAliasRegistryAccess,
    BuiltInObjectTransactionPlan, BuiltInReadProof, BuiltInTransactionAssertion,
    BuiltInVersionWrite, BuiltInWritePayload, CommittedProgramResult,
    CoordinatedProgramPathFinalization, ExistingReferenceWrite,
    PROGRAM_PARTICIPANT_MANIFEST_FORMAT, PROGRAM_PATH_RESERVATION_FORMAT, PreparedBundleHash,
    PreparedBundleRef, PreparedProgramBundle, PreparedProgramRecord, PreparedProgramWrite,
    ProgramAliasBinding, ProgramAliasRegistryCondition, ProgramAliasRegistryMutation,
    ProgramAliasRegistryStage, ProgramBundleAuthority, ProgramCommit, ProgramDurabilityClassHash,
    ProgramDurabilityEvidence, ProgramDurabilityEvidenceHash, ProgramDurabilityScope,
    ProgramExecutionLease, ProgramGovernanceParticipant, ProgramGovernanceReservation, ProgramHash,
    ProgramObjectParticipant, ProgramParticipantIntent, ProgramParticipantManifest,
    ProgramPathCondition, ProgramPathMutation, ProgramPathReservation, ProgramPathStage,
    ProgramReservation, ProgramReservationState, ProgramStoreError, PublishedProgramVersion,
    ReplicaProgramPathApplied, SealedAtomicBatchPublication, StoreProgramEngine,
    VerifiedProgramDefinition, alias_registry_stages_from_prepared, path_stage_from_prepared,
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
    AccountingHeadTransition, AggregateChanged, AggregateKind, AtomicBatchMutation,
    AtomicBatchPublished, AtomicBatchRoute, ContentAccountingTransition, ContentLifecycleChanged,
    DEFAULT_WATCH_MAX_BYTES, DEFAULT_WATCH_MAX_ENTRIES, InvalidationStateHint, LocalChange,
    LocalChangePage, LocalInvalidation, MAX_ATOMIC_BATCH_MUTATIONS,
    MAX_ATOMIC_BATCH_PUBLISHED_BYTES, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, ObjectHeadChange,
    ObjectHeadChangeKind, OversizeLocalChange, ReferenceProof, ReferenceProofMutation,
    RetainedVersionDeletedChange, SourceId, WatchCursor, WatchError, WatchJournalStatus, WatchPage,
    WatchRetention, WatchScope, WatchStart,
};
