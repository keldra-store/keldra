//! Bounded process-local handoff for index definitions assigned to this node.
//!
//! Durable `ASSIGNED` records and ordinary definition objects remain the
//! authorities. This queue only coalesces work between the paged assignment
//! walker and the bounded builder scheduler. Losing it merely causes the next
//! assignment walk to offer the definition again.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use keldra_index::IndexKind;
use keldra_index::v4::{RecipeFingerprints, Schema};
use tonic::Status;

use crate::index_service::{StoredIndexDefinition, definition_path};

use super::v4_schema::compile_schema;

const MAX_PENDING_CATALOG_CHANGES: usize = 1_024;
const PHYSICAL_PROJECTION_DOMAIN: &[u8] = b"keldra.index.physical-projection/v1";
const PROJECTION_FAMILY_DOMAIN: &[u8] = b"keldra.index.projection-family/v1";

/// Stable physical identity for one complete canonical source/schema recipe.
///
/// The full schema fingerprint remains stored and validated by every segment
/// and manifest. These compact values are routing/path keys only: a truncated
/// collision fails closed on that full fingerprint instead of sharing bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PhysicalProjectionIdentity {
    pub(crate) index_id: u64,
    pub(crate) definition_version: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct PhysicalRecipeIdentity {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) fingerprint: [u8; 32],
}

/// Exact format-v5 ownership identity for one tenant/bucket source scope.
/// Field subsets sharing the same membership universe append to this family;
/// different authorities or membership semantics never share it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ProjectionFamilyIdentity {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) family_id: [u8; 32],
}

#[derive(Clone, Debug)]
pub(crate) struct CatalogDefinition {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) object_version: u64,
    pub(crate) stored: StoredIndexDefinition,
    /// Deterministic runtime contract compiled from the authoritative ordinary
    /// definition object. This is process-local and can always be reconstructed.
    pub(crate) schema: Schema,
    pub(crate) schema_fingerprint: [u8; 32],
    pub(crate) recipe_fingerprints: RecipeFingerprints,
}

impl CatalogDefinition {
    pub(crate) fn new(
        tenant_id: u64,
        bucket_id: u64,
        object_version: u64,
        stored: StoredIndexDefinition,
    ) -> Result<Self, Status> {
        let specification = stored.specification()?;
        let schema = compile_schema(
            &stored.path_prefix,
            stored.content_type.as_deref(),
            &specification,
        )
        .map_err(schema_status)?;
        let schema_fingerprint = schema.fingerprint().map_err(schema_status)?;
        let recipe_fingerprints = schema.recipe_fingerprints().map_err(schema_status)?;
        let definition = Self {
            tenant_id,
            bucket_id,
            object_version,
            stored,
            schema,
            schema_fingerprint,
            recipe_fingerprints,
        };
        definition.validate()?;
        Ok(definition)
    }

    pub(crate) fn identity(&self) -> CatalogIdentity {
        CatalogIdentity {
            tenant_id: self.tenant_id,
            bucket_id: self.bucket_id,
            index_id: self.stored.index_id,
        }
    }

    pub(crate) fn physical_identity(&self) -> PhysicalProjectionIdentity {
        if self.schema.kind == IndexKind::TypedJson {
            let family = self.projection_family_identity().family_id;
            let mut index = [0_u8; 8];
            let mut version = [0_u8; 8];
            index.copy_from_slice(&family[..8]);
            version.copy_from_slice(&family[8..16]);
            return PhysicalProjectionIdentity {
                index_id: nonzero_identity(index),
                definition_version: nonzero_identity(version),
            };
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(PHYSICAL_PROJECTION_DOMAIN);
        hasher.update(&self.tenant_id.to_be_bytes());
        hasher.update(&self.bucket_id.to_be_bytes());
        hasher.update(&self.schema_fingerprint);
        let digest = hasher.finalize();
        let mut index = [0_u8; 8];
        let mut version = [0_u8; 8];
        index.copy_from_slice(&digest.as_bytes()[..8]);
        version.copy_from_slice(&digest.as_bytes()[8..16]);
        PhysicalProjectionIdentity {
            index_id: nonzero_identity(index),
            definition_version: nonzero_identity(version),
        }
    }

    pub(crate) fn membership_recipe_identity(&self) -> PhysicalRecipeIdentity {
        self.scoped_recipe(self.recipe_fingerprints.membership)
    }

    pub(crate) fn projection_family_identity(&self) -> ProjectionFamilyIdentity {
        projection_family_identity(
            self.tenant_id,
            self.bucket_id,
            self.recipe_fingerprints.membership,
        )
    }

    pub(crate) fn replace_runtime_schema(&mut self, schema: Schema) -> Result<(), Status> {
        if schema.kind != self.schema.kind
            || schema.path_prefix != self.schema.path_prefix
            || schema.content_type_scope != self.schema.content_type_scope
        {
            return Err(Status::data_loss(
                "projection family schema changed its source universe",
            ));
        }
        self.schema_fingerprint = schema.fingerprint().map_err(schema_status)?;
        self.recipe_fingerprints = schema.recipe_fingerprints().map_err(schema_status)?;
        self.schema = schema;
        Ok(())
    }

    pub(crate) fn family_identity_for_schema(
        tenant_id: u64,
        bucket_id: u64,
        schema: &Schema,
    ) -> Result<ProjectionFamilyIdentity, Status> {
        let recipes = schema.recipe_fingerprints().map_err(schema_status)?;
        Ok(projection_family_identity(
            tenant_id,
            bucket_id,
            recipes.membership,
        ))
    }

    pub(crate) fn field_recipe_identities(&self) -> Vec<PhysicalRecipeIdentity> {
        self.recipe_fingerprints
            .fields
            .iter()
            .copied()
            .map(|fingerprint| self.scoped_recipe(fingerprint))
            .collect()
    }

    fn scoped_recipe(&self, fingerprint: [u8; 32]) -> PhysicalRecipeIdentity {
        PhysicalRecipeIdentity {
            tenant_id: self.tenant_id,
            bucket_id: self.bucket_id,
            fingerprint,
        }
    }

    pub(crate) fn physical_stored(&self) -> StoredIndexDefinition {
        self.stored.with_index_id(self.physical_identity().index_id)
    }

    pub(crate) fn physical_index_id(&self) -> u64 {
        self.physical_identity().index_id
    }

    pub(crate) fn physical_definition_version(&self) -> u64 {
        self.physical_identity().definition_version
    }

    pub(crate) fn validate(&self) -> Result<(), Status> {
        if self.tenant_id == 0 || self.bucket_id == 0 || self.object_version == 0 {
            return Err(Status::data_loss(
                "assigned index definition has a zero stable identity",
            ));
        }
        // `definition_path` is the sole canonical path/name validator. The
        // assignment's exact path is checked before this value enters the
        // catalog; this handoff intentionally stores only the validated name.
        definition_path(&self.stored.name)?;
        let specification = self.stored.specification()?;
        let expected_schema = compile_schema(
            &self.stored.path_prefix,
            self.stored.content_type.as_deref(),
            &specification,
        )
        .map_err(schema_status)?;
        if self.schema != expected_schema
            || self.schema_fingerprint != self.schema.fingerprint().map_err(schema_status)?
            || self.recipe_fingerprints
                != self.schema.recipe_fingerprints().map_err(schema_status)?
        {
            return Err(Status::data_loss(
                "assigned index schema does not match its ordinary definition object",
            ));
        }
        Ok(())
    }
}

fn nonzero_identity(bytes: [u8; 8]) -> u64 {
    let value = u64::from_be_bytes(bytes);
    if value == 0 { 1 } else { value }
}

fn projection_family_identity(
    tenant_id: u64,
    bucket_id: u64,
    membership: [u8; 32],
) -> ProjectionFamilyIdentity {
    let mut hasher = blake3::Hasher::new();
    hasher.update(PROJECTION_FAMILY_DOMAIN);
    hasher.update(&tenant_id.to_be_bytes());
    hasher.update(&bucket_id.to_be_bytes());
    hasher.update(&membership);
    ProjectionFamilyIdentity {
        tenant_id,
        bucket_id,
        family_id: *hasher.finalize().as_bytes(),
    }
}

fn schema_status(error: keldra_index::IndexError) -> Status {
    Status::data_loss(format!(
        "stored index definition cannot compile to format-v4 schema: {error}"
    ))
}

#[derive(Clone, Debug)]
pub(crate) enum CatalogChange {
    Upsert(CatalogDefinition),
    Delete {
        identity: CatalogIdentity,
        object_version: u64,
    },
    Remove(CatalogIdentity),
}

impl CatalogChange {
    pub(crate) fn identity(&self) -> CatalogIdentity {
        match self {
            Self::Upsert(definition) => definition.identity(),
            Self::Delete { identity, .. } => *identity,
            Self::Remove(identity) => *identity,
        }
    }

    fn object_version(&self) -> Option<u64> {
        match self {
            Self::Upsert(definition) => Some(definition.object_version),
            Self::Delete { object_version, .. } => Some(*object_version),
            Self::Remove(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CatalogIdentity {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) index_id: u64,
}

#[derive(Clone)]
pub(crate) struct IndexCatalog {
    inner: Arc<Mutex<CatalogState>>,
    changes: tokio::sync::broadcast::Sender<CatalogIdentity>,
    capacity_changed: Arc<tokio::sync::Notify>,
}

struct CatalogState {
    pending: BTreeMap<CatalogIdentity, CatalogChange>,
    capacity: usize,
}

impl Default for IndexCatalog {
    fn default() -> Self {
        Self::with_capacity(MAX_PENDING_CATALOG_CHANGES)
    }
}

impl IndexCatalog {
    fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "index catalog capacity must be positive");
        let (changes, _) = tokio::sync::broadcast::channel(capacity);
        Self {
            inner: Arc::new(Mutex::new(CatalogState {
                pending: BTreeMap::new(),
                capacity,
            })),
            changes,
            capacity_changed: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub(crate) fn upsert(&self, definition: CatalogDefinition) -> Result<(), Status> {
        definition.validate()?;
        self.enqueue(CatalogChange::Upsert(definition))
    }

    /// Losslessly hand one affected definition to the bounded builder queue.
    ///
    /// The source-journal demultiplexer cannot acknowledge its aggregate
    /// checkpoint until this disposable wake has been admitted. Capacity
    /// pressure therefore delays journal progress instead of dropping the only
    /// prompt wake for an idle builder. Durable assignments remain the recovery
    /// authority if the process stops while waiting.
    pub(crate) async fn upsert_wait(&self, definition: CatalogDefinition) -> Result<(), Status> {
        definition.validate()?;
        self.enqueue_wait(CatalogChange::Upsert(definition)).await
    }

    pub(crate) async fn delete_wait(
        &self,
        identity: CatalogIdentity,
        object_version: u64,
    ) -> Result<(), Status> {
        if object_version == 0 {
            return Err(Status::data_loss(
                "deleted index definition has a zero object version",
            ));
        }
        self.enqueue_wait(CatalogChange::Delete {
            identity,
            object_version,
        })
        .await
    }

    async fn enqueue_wait(&self, change: CatalogChange) -> Result<(), Status> {
        let identity = change.identity();
        loop {
            let notified = self.capacity_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.enqueue(change.clone()) {
                Ok(()) => return Ok(()),
                Err(error) if error.code() == tonic::Code::ResourceExhausted => {
                    tracing::debug!(
                        index.id = identity.index_id,
                        "affected index wake waits for bounded catalog capacity"
                    );
                    notified.await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn remove(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<(), Status> {
        self.enqueue(CatalogChange::Remove(CatalogIdentity {
            tenant_id,
            bucket_id,
            index_id,
        }))
    }

    fn enqueue(&self, change: CatalogChange) -> Result<(), Status> {
        let identity = change.identity();
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("assigned index handoff lock is poisoned"))?;
        if !state.pending.contains_key(&identity) && state.pending.len() >= state.capacity {
            return Err(Status::resource_exhausted(
                "assigned index handoff is at its bounded capacity",
            ));
        }
        if state
            .pending
            .get(&identity)
            .is_some_and(|current| keep_current_change(current, &change))
        {
            return Ok(());
        }
        state.pending.insert(identity, change);
        drop(state);
        let _ = self.changes.send(identity);
        Ok(())
    }

    pub(crate) fn take(
        &self,
        identity: CatalogIdentity,
        mut admit_upsert: impl FnMut(&CatalogChange) -> bool,
    ) -> Result<Option<CatalogChange>, Status> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("assigned index handoff lock is poisoned"))?;
        if state.pending.get(&identity).is_some_and(|change| {
            matches!(change, CatalogChange::Upsert(_)) && !admit_upsert(change)
        }) {
            return Ok(None);
        }
        let removed = state.pending.remove(&identity);
        drop(state);
        if removed.is_some() {
            self.capacity_changed.notify_waiters();
        }
        Ok(removed)
    }

    pub(crate) fn take_page(
        &self,
        limit: usize,
        mut admit_upsert: impl FnMut(&CatalogChange) -> bool,
    ) -> Result<Vec<CatalogChange>, Status> {
        if limit == 0 {
            return Err(Status::invalid_argument(
                "assigned index handoff page must be positive",
            ));
        }
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("assigned index handoff lock is poisoned"))?;
        let identities = state
            .pending
            .iter()
            .filter_map(|(identity, change)| match change {
                CatalogChange::Delete { .. } | CatalogChange::Remove(_) => Some(*identity),
                CatalogChange::Upsert(_) if admit_upsert(change) => Some(*identity),
                CatalogChange::Upsert(_) => None,
            })
            .take(limit)
            .collect::<Vec<_>>();
        let changes = identities
            .into_iter()
            .filter_map(|identity| state.pending.remove(&identity))
            .collect::<Vec<_>>();
        drop(state);
        if !changes.is_empty() {
            self.capacity_changed.notify_waiters();
        }
        Ok(changes)
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::broadcast::Receiver<CatalogIdentity> {
        self.changes.subscribe()
    }

    #[cfg(test)]
    fn pending_len(&self) -> Result<usize, Status> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| Status::internal("assigned index handoff lock is poisoned"))?
            .pending
            .len())
    }
}

fn keep_current_change(current: &CatalogChange, incoming: &CatalogChange) -> bool {
    match (current, incoming) {
        (CatalogChange::Delete { .. }, CatalogChange::Remove(_)) => true,
        (CatalogChange::Delete { .. }, CatalogChange::Upsert(_))
        | (CatalogChange::Upsert(_), CatalogChange::Delete { .. })
        | (CatalogChange::Delete { .. }, CatalogChange::Delete { .. }) => {
            current.object_version() >= incoming.object_version()
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use keldra_api::v1::{CreateIndexRequest, IndexSpecification, PathIndexSpec};

    use super::*;

    fn definition(tenant_id: u64, bucket_id: u64, index_id: u64) -> CatalogDefinition {
        CatalogDefinition::new(
            tenant_id,
            bucket_id,
            1,
            StoredIndexDefinition::create(
                "tenant".into(),
                CreateIndexRequest {
                    bucket: "bucket".into(),
                    name: format!("index-{index_id}"),
                    path_prefix: String::new(),
                    content_type: String::new(),
                    specification: Some(IndexSpecification {
                        specification: Some(
                            keldra_api::v1::index_specification::Specification::Path(
                                PathIndexSpec {},
                            ),
                        ),
                    }),
                    command_id: format!("create-{index_id}"),
                },
                index_id,
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn changes_are_coalesced_and_consumed() {
        let catalog = IndexCatalog::with_capacity(2);
        let first = definition(1, 2, 9);
        let mut replacement = first.clone();
        replacement.object_version = 2;
        catalog.upsert(first).unwrap();
        catalog.upsert(replacement.clone()).unwrap();
        assert_eq!(catalog.pending_len().unwrap(), 1);
        let change = catalog
            .take(replacement.identity(), |_| true)
            .unwrap()
            .unwrap();
        assert!(matches!(change, CatalogChange::Upsert(value) if value.object_version == 2));
        assert_eq!(catalog.pending_len().unwrap(), 0);
    }

    #[test]
    fn bounded_queue_rejects_extra_upserts_but_coalesces_same_identity_removal() {
        let catalog = IndexCatalog::with_capacity(1);
        let first = definition(1, 2, 9);
        let second = definition(3, 4, 10);
        catalog.upsert(first.clone()).unwrap();
        assert_eq!(
            catalog.upsert(second).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        catalog
            .remove(first.tenant_id, first.bucket_id, first.stored.index_id)
            .unwrap();
        assert!(matches!(
            catalog.take(first.identity(), |_| true).unwrap(),
            Some(CatalogChange::Remove(_))
        ));
    }

    #[test]
    fn definition_delete_is_not_downgraded_to_assignment_removal() {
        let catalog = IndexCatalog::with_capacity(1);
        let definition = definition(1, 2, 9);
        let identity = definition.identity();
        catalog.upsert(definition).unwrap();
        catalog
            .enqueue(CatalogChange::Delete {
                identity,
                object_version: 2,
            })
            .unwrap();
        catalog.remove(1, 2, 9).unwrap();
        assert!(matches!(
            catalog.take(identity, |_| true).unwrap(),
            Some(CatalogChange::Delete {
                object_version: 2,
                ..
            })
        ));
    }

    #[test]
    fn recreation_replaces_one_pending_tombstone_only_when_newer() {
        let catalog = IndexCatalog::with_capacity(1);
        let mut stale = definition(1, 2, 9);
        stale.object_version = 2;
        let identity = stale.identity();
        catalog
            .enqueue(CatalogChange::Delete {
                identity,
                object_version: 3,
            })
            .unwrap();
        catalog.upsert(stale).unwrap();
        assert!(matches!(
            catalog.take(identity, |_| true).unwrap(),
            Some(CatalogChange::Delete {
                object_version: 3,
                ..
            })
        ));

        catalog
            .enqueue(CatalogChange::Delete {
                identity,
                object_version: 3,
            })
            .unwrap();
        let mut recreated = definition(1, 2, 9);
        recreated.object_version = 4;
        catalog.upsert(recreated).unwrap();
        assert!(matches!(
            catalog.take(identity, |_| true).unwrap(),
            Some(CatalogChange::Upsert(CatalogDefinition {
                object_version: 4,
                ..
            }))
        ));
    }

    #[test]
    fn upserts_remain_pending_while_builder_leases_are_full() {
        let catalog = IndexCatalog::with_capacity(1);
        catalog.upsert(definition(1, 2, 9)).unwrap();
        assert!(catalog.take_page(1, |_| false).unwrap().is_empty());
        assert_eq!(catalog.pending_len().unwrap(), 1);
        assert_eq!(catalog.take_page(1, |_| true).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn affected_definition_waits_for_capacity_instead_of_losing_its_wake() {
        let catalog = IndexCatalog::with_capacity(1);
        let first = definition(1, 2, 9);
        let second = definition(3, 4, 10);
        let second_identity = second.identity();
        catalog.upsert(first.clone()).unwrap();

        let waiting_catalog = catalog.clone();
        let waiting = tokio::spawn(async move { waiting_catalog.upsert_wait(second).await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        assert!(catalog.take(first.identity(), |_| true).unwrap().is_some());
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("affected wake did not resume after catalog capacity was released")
            .unwrap()
            .unwrap();
        assert!(catalog.take(second_identity, |_| true).unwrap().is_some());
    }

    #[test]
    fn ordinary_definition_compiles_one_bound_v4_schema_and_fingerprint() {
        let definition = definition(1, 2, 9);
        assert_eq!(definition.schema.path_prefix, "");
        assert_eq!(definition.schema.fields[0].name, "path");
        assert_eq!(
            definition.schema_fingerprint,
            definition.schema.fingerprint().unwrap()
        );
        definition.validate().unwrap();
    }

    #[test]
    fn ordinary_definition_semantic_update_compiles_a_new_fingerprint() {
        let original = definition(1, 2, 9);
        let mut updated = original.stored.clone();
        updated.path_prefix = "tenant/42/".into();
        let updated = CatalogDefinition::new(1, 2, 2, updated).unwrap();

        assert_ne!(original.schema_fingerprint, updated.schema_fingerprint);
        assert_eq!(updated.schema.path_prefix, "tenant/42/");
    }

    #[test]
    fn equivalent_logical_definitions_share_one_physical_projection_identity() {
        let first = definition(1, 2, 9);
        let second = definition(1, 2, 10);
        assert_ne!(first.identity(), second.identity());
        assert_eq!(first.schema_fingerprint, second.schema_fingerprint);
        assert_eq!(first.physical_identity(), second.physical_identity());
        assert_eq!(
            first.projection_family_identity(),
            second.projection_family_identity()
        );
        assert_eq!(
            first.membership_recipe_identity(),
            second.membership_recipe_identity()
        );
        assert_eq!(
            first.field_recipe_identities(),
            second.field_recipe_identities()
        );

        let different_bucket = definition(1, 3, 11);
        assert_ne!(
            first.physical_identity(),
            different_bucket.physical_identity()
        );
        assert_ne!(
            first.projection_family_identity(),
            different_bucket.projection_family_identity()
        );

        let mut different_scope = second.stored.clone();
        different_scope.path_prefix = "other/".into();
        let different_scope = CatalogDefinition::new(1, 2, 2, different_scope).unwrap();
        assert_ne!(
            first.physical_identity(),
            different_scope.physical_identity()
        );
        assert_ne!(
            first.projection_family_identity(),
            different_scope.projection_family_identity()
        );
    }

    #[test]
    fn physical_recipe_identity_never_crosses_tenant_or_bucket_authority() {
        let first = definition(1, 2, 9);
        let other_tenant = definition(3, 2, 10);
        let other_bucket = definition(1, 4, 11);
        assert_ne!(
            first.membership_recipe_identity(),
            other_tenant.membership_recipe_identity()
        );
        assert_ne!(
            first.membership_recipe_identity(),
            other_bucket.membership_recipe_identity()
        );
        assert_ne!(
            first.field_recipe_identities(),
            other_tenant.field_recipe_identities()
        );
    }

    #[test]
    fn catalog_scale_collapses_two_hundred_fifty_thousand_equivalent_definitions() {
        let base = definition(1, 2, 1);
        let logical = (1..=250_000_u64)
            .map(|index_id| {
                (
                    CatalogIdentity {
                        tenant_id: 1,
                        bucket_id: 2,
                        index_id,
                    },
                    base.physical_identity(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let physical = logical
            .values()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(logical.len(), 250_000);
        assert_eq!(physical.len(), 1);
    }

    #[test]
    fn catalog_rejects_schema_or_fingerprint_detached_from_the_definition() {
        let definition = definition(1, 2, 9);
        let mut wrong_fingerprint = definition.clone();
        wrong_fingerprint.schema_fingerprint[0] ^= 1;
        assert_eq!(
            wrong_fingerprint.validate().unwrap_err().code(),
            tonic::Code::DataLoss
        );

        let mut wrong_schema = definition;
        wrong_schema.schema.path_prefix = "other/".into();
        wrong_schema.schema_fingerprint = wrong_schema.schema.fingerprint().unwrap();
        assert_eq!(
            wrong_schema.validate().unwrap_err().code(),
            tonic::Code::DataLoss
        );
    }
}
