//! Process-local active index catalog compiled from ordinary definitions.
//!
//! The catalog is a disposable, version-monotonic projection. Definition
//! changes mutate active state directly and broadcast only a best-effort wake;
//! there is no bounded builder handoff or per-definition assignment queue.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use keldra_index::typed_json::{FieldId, FieldSchema, RecipeFingerprints, TypedJsonSchema};
use keldra_index::v1::{
    IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryPermit, IndexingMemoryStage,
};
use tonic::Status;

use crate::index_service::{StoredIndexDefinition, definition_path};

use super::json_projection::CompiledScalarProjectionPlan;
use super::typed_json_schema::compile_typed_json_schema;

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

/// Exact format-v1 physical-family identity for one tenant/bucket source scope.
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
    pub(crate) schema: TypedJsonSchema,
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
        if tenant_id == 0 || bucket_id == 0 || object_version == 0 {
            return Err(Status::data_loss(
                "assigned index definition has a zero stable identity",
            ));
        }
        definition_path(&stored.name)?;
        let specification = stored.specification()?;
        let schema = compile_typed_json_schema(
            &stored.path_prefix,
            stored.content_type.as_deref(),
            &specification,
        )
        .map_err(schema_status)?;
        let schema_fingerprint = schema.fingerprint().map_err(schema_status)?;
        let recipe_fingerprints = schema.recipe_fingerprints().map_err(schema_status)?;
        Ok(Self {
            tenant_id,
            bucket_id,
            object_version,
            stored,
            schema,
            schema_fingerprint,
            recipe_fingerprints,
        })
    }

    pub(crate) fn identity(&self) -> CatalogIdentity {
        CatalogIdentity {
            tenant_id: self.tenant_id,
            bucket_id: self.bucket_id,
            index_id: self.stored.index_id,
        }
    }

    pub(crate) fn physical_identity(&self) -> PhysicalProjectionIdentity {
        let family = self.projection_family_identity().family_id;
        let mut index = [0_u8; 8];
        let mut version = [0_u8; 8];
        index.copy_from_slice(&family[..8]);
        version.copy_from_slice(&family[8..16]);
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

    pub(crate) fn replace_runtime_schema(&mut self, schema: TypedJsonSchema) -> Result<(), Status> {
        if schema.path_prefix != self.schema.path_prefix
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
        schema: &TypedJsonSchema,
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

    #[cfg(test)]
    fn validate(&self) -> Result<(), Status> {
        if self.tenant_id == 0 || self.bucket_id == 0 || self.object_version == 0 {
            return Err(Status::data_loss(
                "assigned index definition has a zero stable identity",
            ));
        }
        definition_path(&self.stored.name)?;
        let specification = self.stored.specification()?;
        let expected_schema = compile_typed_json_schema(
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
        "stored TypedJson definition cannot compile to its physical recipe catalog: {error}"
    ))
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CatalogIdentity {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) index_id: u64,
}

/// Compact query binding retained once per logical definition. Source routes
/// and the full compiled schema are interned in `PhysicalCatalogRecipe`.
#[derive(Clone, Debug)]
pub(crate) struct LogicalCatalogBinding {
    pub(crate) identity: CatalogIdentity,
    pub(crate) object_version: u64,
    pub(crate) family: ProjectionFamilyIdentity,
    pub(crate) query_contract: [u8; 32],
    // Reconstructed from the ordinary definition; never a second authority.
    pub(crate) explicit_rebuild_at_unix_millis: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalQueryContract {
    pub(crate) identity: [u8; 32],
    pub(crate) public_fields: Arc<ResidentQueryFields>,
    references: usize,
}

#[derive(Debug)]
pub(crate) struct ResidentQueryFields {
    values: Vec<(String, [u8; 32])>,
    _permit: IndexingMemoryPermit,
}

impl std::ops::Deref for ResidentQueryFields {
    type Target = Vec<(String, [u8; 32])>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

#[derive(Debug)]
pub(crate) struct PhysicalCatalogRecipe {
    pub(crate) family: ProjectionFamilyIdentity,
    pub(crate) storage_tenant: String,
    pub(crate) bucket: String,
    pub(crate) membership_recipe: [u8; 32],
    pub(crate) path_prefix: String,
    pub(crate) content_type: Option<String>,
    template: Arc<TypedJsonSchema>,
    pub(crate) fields: BTreeMap<[u8; 32], Arc<FieldSchema>>,
    pub(crate) projection_plan: Arc<CompiledScalarProjectionPlan>,
    pub(crate) physical_generation: [u8; 32],
    resident_lease: Option<Arc<RecipeResidentLease>>,
}

#[derive(Debug)]
struct RecipeResidentTracker {
    bytes: AtomicUsize,
    credits: IndexingMemoryCredits,
}

#[derive(Debug)]
pub(crate) struct RecipeResidentLease {
    tracker: Arc<RecipeResidentTracker>,
    bytes: usize,
    _permit: IndexingMemoryPermit,
}

impl Drop for RecipeResidentLease {
    fn drop(&mut self) {
        self.tracker.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct CatalogRecipeState {
    /// Only accepted explicit intents, derived from live ordinary definitions.
    /// Family-local state keeps ordinary alias loading independent of bucket size.
    rebuild_intents: BTreeMap<u64, u64>,
    physical: Arc<PhysicalCatalogRecipe>,
    references: usize,
    field_references: BTreeMap<[u8; 32], usize>,
}

impl PhysicalCatalogRecipe {
    pub(crate) fn projection_schema(&self) -> Result<TypedJsonSchema, Status> {
        let mut schema = (*self.template).clone();
        schema.fields = self
            .fields
            .iter()
            .enumerate()
            .map(|(ordinal, (recipe, field))| {
                let mut field = (**field).clone();
                field.id = FieldId::new(u32::try_from(ordinal).map_err(|_| {
                    Status::resource_exhausted("physical field catalog exceeds field ID capacity")
                })?);
                field.name = format!("__keldra_recipe_{}", hex::encode(recipe));
                Ok(field)
            })
            .collect::<Result<Vec<_>, Status>>()?;
        schema.physical_order.clear();
        schema.canonicalize_physical_fields().map_err(schema_status)
    }

    /// Rebuild one logical query schema from the interned physical fields and
    /// compact public-name contract. The authoritative definition was already
    /// compiled when this catalog generation committed, so query execution
    /// does not parse and fingerprint the same definition again.
    pub(crate) fn query_schema(
        &self,
        contract: &LogicalQueryContract,
    ) -> Result<TypedJsonSchema, Status> {
        let mut schema = (*self.template).clone();
        schema.fields = contract
            .public_fields
            .iter()
            .enumerate()
            .map(|(ordinal, (name, recipe))| {
                let mut field = self
                    .fields
                    .get(recipe)
                    .ok_or_else(|| {
                        Status::data_loss("logical query field is absent from its physical family")
                    })?
                    .as_ref()
                    .clone();
                field.id = FieldId::new(u32::try_from(ordinal).map_err(|_| {
                    Status::resource_exhausted("logical field catalog exceeds field ID capacity")
                })?);
                field.name.clone_from(name);
                Ok(field)
            })
            .collect::<Result<Vec<_>, Status>>()?;
        schema.physical_order.clear();
        Ok(schema)
    }
}

#[derive(Clone)]
pub(crate) struct IndexCatalog {
    inner: Arc<Mutex<CatalogState>>,
    changes: tokio::sync::broadcast::Sender<CatalogNotice>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CatalogNotice {
    pub(crate) identity: CatalogIdentity,
    pub(crate) physical_changed: bool,
}

/// Authoritative mutable catalog state. Mutations use `CatalogMutationBackup`
/// to retain only affected entries until admission and publication succeed.
struct CatalogState {
    resident_permit: IndexingMemoryPermit,
    bindings: BTreeMap<CatalogIdentity, LogicalCatalogBinding>,
    recipes: BTreeMap<ProjectionFamilyIdentity, CatalogRecipeState>,
    query_contracts: BTreeMap<[u8; 32], LogicalQueryContract>,
    generation: u64,
    physical_generation: [u8; 32],
    published_physical: Arc<PhysicalCatalogSnapshot>,
    resident_bytes: usize,
    maximum_bytes: usize,
    recipe_resident: Arc<RecipeResidentTracker>,
}

/// Compact undo state for one logical catalog mutation.
///
/// A mutation can touch only its logical binding, its previous and next
/// physical families, and its previous and next query contracts. Retaining
/// those entries makes all fallible preparation transactional without cloning
/// the complete logical-binding map for every write.
struct CatalogMutationBackup {
    identity: CatalogIdentity,
    binding: Option<LogicalCatalogBinding>,
    recipes: Vec<(ProjectionFamilyIdentity, Option<CatalogRecipeState>)>,
    query_contracts: Vec<([u8; 32], Option<LogicalQueryContract>)>,
    generation: u64,
    physical_generation: [u8; 32],
    published_physical: Arc<PhysicalCatalogSnapshot>,
    resident_bytes: usize,
}

impl CatalogMutationBackup {
    fn capture(
        state: &CatalogState,
        identity: CatalogIdentity,
        next: Option<&LogicalCatalogBinding>,
    ) -> Self {
        let binding = state.bindings.get(&identity).cloned();
        let mut families = BTreeSet::new();
        let mut contracts = BTreeSet::new();
        if let Some(binding) = binding.as_ref() {
            families.insert(binding.family);
            contracts.insert(binding.query_contract);
        }
        if let Some(binding) = next {
            families.insert(binding.family);
            contracts.insert(binding.query_contract);
        }
        Self {
            identity,
            binding,
            recipes: families
                .into_iter()
                .map(|family| (family, state.recipes.get(&family).cloned()))
                .collect(),
            query_contracts: contracts
                .into_iter()
                .map(|contract| (contract, state.query_contracts.get(&contract).cloned()))
                .collect(),
            generation: state.generation,
            physical_generation: state.physical_generation,
            published_physical: Arc::clone(&state.published_physical),
            resident_bytes: state.resident_bytes,
        }
    }

    fn restore(self, state: &mut CatalogState) {
        match self.binding {
            Some(binding) => {
                state.bindings.insert(self.identity, binding);
            }
            None => {
                state.bindings.remove(&self.identity);
            }
        }
        for (family, recipe) in self.recipes {
            match recipe {
                Some(recipe) => {
                    state.recipes.insert(family, recipe);
                }
                None => {
                    state.recipes.remove(&family);
                }
            }
        }
        for (identity, contract) in self.query_contracts {
            match contract {
                Some(contract) => {
                    state.query_contracts.insert(identity, contract);
                }
                None => {
                    state.query_contracts.remove(&identity);
                }
            }
        }
        state.generation = self.generation;
        state.physical_generation = self.physical_generation;
        state.published_physical = self.published_physical;
        state.resident_bytes = self.resident_bytes;
    }
}

/// Immutable worker view published only after a committed physical-catalog
/// change. Readers clone this `Arc`, never the complete recipe catalogue.
#[derive(Clone, Debug)]
pub(crate) struct PhysicalCatalogSnapshot {
    pub(crate) generation: u64,
    pub(crate) identity: [u8; 32],
    pub(crate) recipes: Arc<[Arc<PhysicalCatalogRecipe>]>,
    pub(crate) resident_lease: Option<Arc<RecipeResidentLease>>,
}

impl Default for IndexCatalog {
    fn default() -> Self {
        Self::with_memory_bytes(128 * 1024 * 1024)
            .expect("default active index catalog memory is valid")
    }
}

impl IndexCatalog {
    pub(crate) fn with_memory_bytes(maximum_bytes: u64) -> Result<Self, Status> {
        let maximum_bytes = usize::try_from(maximum_bytes).map_err(|_| {
            Status::invalid_argument("active index catalog memory exceeds this platform")
        })?;
        if maximum_bytes == 0 {
            return Err(Status::invalid_argument(
                "active index catalog memory must be positive",
            ));
        }
        let limits = IndexingMemoryLimits {
            hot_payload_bytes: maximum_bytes,
            worker_scratch_bytes: maximum_bytes,
            prepared_rows_bytes: maximum_bytes,
            replay_input_bytes: maximum_bytes,
            projection_accumulator_bytes: maximum_bytes,
            seal_scratch_bytes: maximum_bytes,
            ordering_catalog_bytes: maximum_bytes,
        };
        let credits = IndexingMemoryCredits::new(maximum_bytes, limits)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Self::with_credits(credits, maximum_bytes)
    }

    pub(crate) fn with_credits(
        credits: IndexingMemoryCredits,
        maximum_bytes: usize,
    ) -> Result<Self, Status> {
        let permit = credits
            .acquire(IndexingMemoryStage::OrderingCatalog, 0)
            .map_err(|_| Status::resource_exhausted("active index catalog memory unavailable"))?;
        let (changes, _) = tokio::sync::broadcast::channel(1_024);
        let empty_physical =
            physical_catalog_generation(std::iter::empty::<&PhysicalCatalogRecipe>());
        let recipe_resident = Arc::new(RecipeResidentTracker {
            bytes: AtomicUsize::new(0),
            credits,
        });
        let published_physical = track_snapshot(&recipe_resident, 1, empty_physical, Vec::new())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(CatalogState {
                resident_permit: permit,
                bindings: BTreeMap::new(),
                recipes: BTreeMap::new(),
                query_contracts: BTreeMap::new(),
                generation: 1,
                physical_generation: empty_physical,
                published_physical,
                resident_bytes: 0,
                maximum_bytes,
                recipe_resident,
            })),
            changes,
        })
    }
    pub(crate) fn upsert(&self, definition: CatalogDefinition) -> Result<(), Status> {
        let identity = definition.identity();
        let binding = compact_binding(&definition);
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        let current_version = state
            .bindings
            .get(&identity)
            .map(|binding| binding.object_version)
            .unwrap_or(0);
        if current_version >= definition.object_version {
            return Ok(());
        }
        let external_recipes = externally_retained_recipes(&state);
        let backup = CatalogMutationBackup::capture(&state, identity, Some(&binding));
        let mut physical_changed = false;
        let result = (|| {
            let mut previous_family = None;
            if let Some(previous) = state.bindings.remove(&identity) {
                previous_family = Some(previous.family);
                state.resident_bytes = state
                    .resident_bytes
                    .saturating_sub(binding_resident_bytes(&previous)?);
                physical_changed |= remove_recipe_reference(&mut state, &previous)?;
                remove_query_contract_reference(&mut state, previous.query_contract);
            }
            physical_changed |= add_recipe_reference(&mut state, &definition)?;
            add_query_contract_reference(&mut state, &definition, binding.query_contract)?;
            state.resident_bytes = state
                .resident_bytes
                .checked_add(binding_resident_bytes(&binding)?)
                .ok_or_else(|| {
                    Status::resource_exhausted("active index catalog resident size overflow")
                })?;
            let next_family = binding.family;
            state.bindings.insert(identity, binding);
            physical_changed |= refresh_family_rebuild_generation(&mut state, next_family)?;
            if let Some(previous_family) = previous_family.filter(|family| *family != next_family) {
                physical_changed |= refresh_family_rebuild_generation(&mut state, previous_family)?;
            }
            mark_catalog_changed(&mut state, physical_changed)?;
            prepare_mutation_commit(&mut state, &backup, &external_recipes)
        })();
        if let Err(error) = result {
            backup.restore(&mut state);
            return Err(error);
        }
        drop(backup);
        drop(state);
        let _ = self.changes.send(CatalogNotice {
            identity,
            physical_changed,
        });
        Ok(())
    }

    /// Apply one committed definition mutation to active catalog state.
    pub(crate) async fn upsert_wait(&self, definition: CatalogDefinition) -> Result<(), Status> {
        self.upsert(definition)
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
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        let current_version = state
            .bindings
            .get(&identity)
            .map(|binding| binding.object_version)
            .unwrap_or(0);
        if current_version >= object_version {
            return Ok(());
        }
        let external_recipes = externally_retained_recipes(&state);
        let backup = CatalogMutationBackup::capture(&state, identity, None);
        let mut physical_changed = false;
        let result = (|| {
            if let Some(previous) = state.bindings.remove(&identity) {
                state.resident_bytes = state
                    .resident_bytes
                    .saturating_sub(binding_resident_bytes(&previous)?);
                physical_changed |= remove_recipe_reference(&mut state, &previous)?;
                remove_query_contract_reference(&mut state, previous.query_contract);
                physical_changed |= refresh_family_rebuild_generation(&mut state, previous.family)?;
            }
            mark_catalog_changed(&mut state, physical_changed)?;
            prepare_mutation_commit(&mut state, &backup, &external_recipes)
        })();
        if let Err(error) = result {
            backup.restore(&mut state);
            return Err(error);
        }
        drop(backup);
        drop(state);
        let _ = self.changes.send(CatalogNotice {
            identity,
            physical_changed,
        });
        Ok(())
    }

    pub(crate) fn remove(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<(), Status> {
        let identity = CatalogIdentity {
            tenant_id,
            bucket_id,
            index_id,
        };
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        if !state.bindings.contains_key(&identity) {
            return Ok(());
        }
        let external_recipes = externally_retained_recipes(&state);
        let backup = CatalogMutationBackup::capture(&state, identity, None);
        let mut physical_changed = false;
        let result = (|| {
            let previous = state
                .bindings
                .remove(&identity)
                .expect("catalog contains the checked binding");
            state.resident_bytes = state
                .resident_bytes
                .saturating_sub(binding_resident_bytes(&previous)?);
            physical_changed = remove_recipe_reference(&mut state, &previous)?;
            remove_query_contract_reference(&mut state, previous.query_contract);
            physical_changed |= refresh_family_rebuild_generation(&mut state, previous.family)?;
            mark_catalog_changed(&mut state, physical_changed)?;
            prepare_mutation_commit(&mut state, &backup, &external_recipes)
        })();
        if let Err(error) = result {
            backup.restore(&mut state);
            return Err(error);
        }
        drop(backup);
        drop(state);
        let _ = self.changes.send(CatalogNotice {
            identity,
            physical_changed,
        });
        Ok(())
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::broadcast::Receiver<CatalogNotice> {
        self.changes.subscribe()
    }

    #[cfg(test)]
    fn snapshot(
        &self,
    ) -> Result<
        (
            u64,
            [u8; 32],
            Vec<LogicalCatalogBinding>,
            Vec<Arc<PhysicalCatalogRecipe>>,
            Vec<LogicalQueryContract>,
        ),
        Status,
    > {
        let state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        Ok((
            state.generation,
            state.physical_generation,
            state.bindings.values().cloned().collect(),
            state
                .recipes
                .values()
                .map(|recipe| Arc::clone(&recipe.physical))
                .collect(),
            state.query_contracts.values().cloned().collect(),
        ))
    }

    pub(crate) fn physical_snapshot(&self) -> Result<Arc<PhysicalCatalogSnapshot>, Status> {
        let state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        Ok(state.published_physical.clone())
    }

    pub(crate) fn resolve(
        &self,
        identity: CatalogIdentity,
    ) -> Result<
        Option<(
            LogicalCatalogBinding,
            Arc<PhysicalCatalogRecipe>,
            LogicalQueryContract,
        )>,
        Status,
    > {
        let state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        let Some(binding) = state.bindings.get(&identity).cloned() else {
            return Ok(None);
        };
        let recipe = state
            .recipes
            .get(&binding.family)
            .map(|recipe| Arc::clone(&recipe.physical))
            .ok_or_else(|| Status::data_loss("logical index has no physical v1 family"))?;
        let contract = state
            .query_contracts
            .get(&binding.query_contract)
            .cloned()
            .ok_or_else(|| Status::data_loss("logical index has no query contract"))?;
        Ok(Some((binding, recipe, contract)))
    }

    pub(crate) fn is_current(
        &self,
        identity: CatalogIdentity,
        object_version: u64,
        family: ProjectionFamilyIdentity,
        physical_generation: [u8; 32],
        membership_recipe: [u8; 32],
    ) -> Result<bool, Status> {
        let state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        let Some(binding) = state.bindings.get(&identity) else {
            return Ok(false);
        };
        let Some(recipe) = state.recipes.get(&family) else {
            return Ok(false);
        };
        Ok(binding.object_version == object_version
            && binding.family == family
            && recipe.physical.physical_generation == physical_generation
            && recipe.physical.membership_recipe == membership_recipe)
    }

    pub(crate) fn resident_bytes(&self) -> Result<usize, Status> {
        let state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        total_resident_bytes(&state)
    }
}

fn recipe_key(recipe: &Arc<PhysicalCatalogRecipe>) -> usize {
    Arc::as_ptr(recipe) as usize
}

struct ExternalRecipeOwnership {
    recipes: BTreeMap<usize, bool>,
    published_snapshot: bool,
}

fn externally_retained_recipes(state: &CatalogState) -> ExternalRecipeOwnership {
    let published_snapshot_reader = Arc::strong_count(&state.published_physical) > 1;
    let mut retained = BTreeMap::new();
    for recipe in state.recipes.values() {
        retained.insert(
            recipe_key(&recipe.physical),
            published_snapshot_reader
                || Arc::strong_count(&recipe.physical) > 2
                || Arc::strong_count(&recipe.physical.projection_plan) > 1,
        );
    }
    ExternalRecipeOwnership {
        recipes: retained,
        published_snapshot: published_snapshot_reader,
    }
}

fn prepare_mutation_commit(
    state: &mut CatalogState,
    backup: &CatalogMutationBackup,
    external_recipes: &ExternalRecipeOwnership,
) -> Result<(), Status> {
    let live = state
        .recipes
        .values()
        .map(|recipe| recipe_key(&recipe.physical))
        .collect::<BTreeSet<_>>();
    let releasable = backup
        .recipes
        .iter()
        .filter_map(|(_, recipe)| recipe.as_ref())
        .filter(|recipe| !live.contains(&recipe_key(&recipe.physical)))
        .filter(|recipe| {
            !external_recipes
                .recipes
                .get(&recipe_key(&recipe.physical))
                .copied()
                .unwrap_or(false)
        })
        .try_fold(0usize, |total, recipe| {
            total
                .checked_add(recipe_resident_bytes(&recipe.physical).ok_or_else(|| {
                    Status::resource_exhausted("active index catalog resident size overflow")
                })?)
                .ok_or_else(|| {
                    Status::resource_exhausted("active index catalog resident size overflow")
                })
        })?;
    let releasable_snapshot = (!Arc::ptr_eq(&state.published_physical, &backup.published_physical)
        && !external_recipes.published_snapshot)
        .then(|| {
            backup
                .published_physical
                .resident_lease
                .as_ref()
                .map_or(0, |lease| lease.bytes)
        })
        .unwrap_or(0);
    let projected = total_resident_bytes(state)?
        .saturating_sub(releasable)
        .saturating_sub(releasable_snapshot);
    if projected > state.maximum_bytes {
        return Err(Status::resource_exhausted(format!(
            "active index catalog requires {} bytes but its OrderingCatalog credit is {} bytes",
            projected, state.maximum_bytes
        )));
    }
    let shared_fields = state
        .query_contracts
        .values()
        .fold(0usize, |bytes, contract| {
            bytes.saturating_add(contract.public_fields._permit.bytes())
        });
    let control_bytes = state.resident_bytes.saturating_sub(shared_fields);
    state.resident_permit.grow_to(control_bytes).map_err(|_| {
        Status::resource_exhausted("active catalog shared working memory is exhausted")
    })?;
    state
        .resident_permit
        .shrink_to(control_bytes)
        .map_err(|error| Status::internal(error.to_string()))?;
    Ok(())
}

fn total_resident_bytes(state: &CatalogState) -> Result<usize, Status> {
    state
        .resident_bytes
        .checked_add(state.recipe_resident.bytes.load(Ordering::Relaxed))
        .ok_or_else(|| Status::resource_exhausted("active index catalog resident size overflow"))
}

fn compact_binding_without_allocations(definition: &CatalogDefinition) -> LogicalCatalogBinding {
    LogicalCatalogBinding {
        identity: definition.identity(),
        object_version: definition.object_version,
        family: definition.projection_family_identity(),
        query_contract: query_contract_identity(definition),
        explicit_rebuild_at_unix_millis: definition.stored.last_explicit_rebuild_at_unix_millis(),
    }
}

fn mark_catalog_changed(state: &mut CatalogState, physical_changed: bool) -> Result<(), Status> {
    state.generation = state
        .generation
        .checked_add(1)
        .ok_or_else(|| Status::resource_exhausted("active index catalog generation overflow"))?;
    if physical_changed {
        state.physical_generation = physical_catalog_generation(
            state
                .recipes
                .values()
                .map(|recipe| recipe.physical.as_ref()),
        );
        let recipes = state
            .recipes
            .values()
            .map(|recipe| Arc::clone(&recipe.physical))
            .collect::<Vec<_>>();
        let next = track_snapshot(
            &state.recipe_resident,
            state.generation,
            state.physical_generation,
            recipes,
        )?;
        let previous = std::mem::replace(&mut state.published_physical, next);
        drop(previous);
    }
    Ok(())
}

fn physical_catalog_generation<'a>(
    recipes: impl IntoIterator<Item = &'a PhysicalCatalogRecipe>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra.index.physical-catalog/v1");
    for recipe in recipes {
        hasher.update(&recipe.family.tenant_id.to_be_bytes());
        hasher.update(&recipe.family.bucket_id.to_be_bytes());
        hasher.update(&recipe.family.family_id);
        hasher.update(&recipe.membership_recipe);
        for field in recipe.fields.keys() {
            hasher.update(field);
        }
    }
    *hasher.finalize().as_bytes()
}

fn compact_binding(definition: &CatalogDefinition) -> LogicalCatalogBinding {
    compact_binding_without_allocations(definition)
}

fn query_contract_identity(definition: &CatalogDefinition) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra.index.logical-query-contract/v1");
    for (field, fingerprint) in definition
        .schema
        .fields
        .iter()
        .zip(definition.recipe_fingerprints.fields.iter())
    {
        hasher.update(&(field.name.len() as u64).to_be_bytes());
        hasher.update(field.name.as_bytes());
        hasher.update(fingerprint);
    }
    *hasher.finalize().as_bytes()
}

fn add_query_contract_reference(
    state: &mut CatalogState,
    definition: &CatalogDefinition,
    identity: [u8; 32],
) -> Result<(), Status> {
    if let Some(contract) = state.query_contracts.get_mut(&identity) {
        contract.references = contract.references.saturating_add(1);
        return Ok(());
    }
    let field_bytes = std::mem::size_of::<ResidentQueryFields>()
        + 2 * std::mem::size_of::<usize>()
        + definition.schema.fields.len() * std::mem::size_of::<(String, [u8; 32])>()
        + definition
            .schema
            .fields
            .iter()
            .map(|field| field.name.len())
            .sum::<usize>();
    let mut permit = state
        .recipe_resident
        .credits
        .acquire(IndexingMemoryStage::OrderingCatalog, field_bytes)
        .map_err(|_| {
            Status::resource_exhausted("query field names shared working memory is exhausted")
        })?;
    let values: Vec<_> = definition
        .schema
        .fields
        .iter()
        .zip(definition.recipe_fingerprints.fields.iter().copied())
        .map(|(field, fingerprint)| (field.name.clone(), fingerprint))
        .collect();
    let actual_bytes = std::mem::size_of::<ResidentQueryFields>()
        + 2 * std::mem::size_of::<usize>()
        + values.capacity() * std::mem::size_of::<(String, [u8; 32])>()
        + values
            .iter()
            .map(|(name, _)| name.capacity())
            .sum::<usize>();
    permit.grow_to(actual_bytes).map_err(|_| {
        Status::resource_exhausted("query field names shared working memory is exhausted")
    })?;
    permit
        .shrink_to(actual_bytes)
        .map_err(|error| Status::internal(error.to_string()))?;
    let contract = LogicalQueryContract {
        identity,
        public_fields: Arc::new(ResidentQueryFields {
            values,
            _permit: permit,
        }),
        references: 1,
    };
    state.resident_bytes = state
        .resident_bytes
        .checked_add(query_contract_resident_bytes(&contract)?)
        .ok_or_else(|| Status::resource_exhausted("active index catalog resident size overflow"))?;
    state.query_contracts.insert(identity, contract);
    Ok(())
}

fn remove_query_contract_reference(state: &mut CatalogState, identity: [u8; 32]) {
    let remove = match state.query_contracts.get_mut(&identity) {
        Some(contract) if contract.references > 1 => {
            contract.references -= 1;
            false
        }
        Some(_) => true,
        None => false,
    };
    if remove && let Some(contract) = state.query_contracts.remove(&identity) {
        state.resident_bytes = state
            .resident_bytes
            .saturating_sub(query_contract_resident_bytes(&contract).unwrap_or(0));
    }
}

fn add_recipe_reference(
    state: &mut CatalogState,
    definition: &CatalogDefinition,
) -> Result<bool, Status> {
    let identity = definition.projection_family_identity();
    let resident_tracker = Arc::clone(&state.recipe_resident);
    match state.recipes.get_mut(&identity) {
        Some(recipe) => {
            let before_state = catalog_recipe_state_resident_bytes(recipe).ok_or_else(|| {
                Status::resource_exhausted("active index catalog resident size overflow")
            })?;
            recipe.references = recipe.references.saturating_add(1);
            if let Some(accepted_at) = definition.stored.last_explicit_rebuild_at_unix_millis() {
                recipe
                    .rebuild_intents
                    .insert(definition.stored.index_id, accepted_at);
            }
            let mut fields = recipe.physical.fields.clone();
            let old_fields = fields.len();
            for (fingerprint, field) in definition
                .recipe_fingerprints
                .fields
                .iter()
                .copied()
                .zip(&definition.schema.fields)
            {
                *recipe.field_references.entry(fingerprint).or_default() += 1;
                fields
                    .entry(fingerprint)
                    .or_insert_with(|| Arc::new(field.clone()));
            }
            let changed = fields.len() != old_fields;
            if changed {
                let mut next = clone_physical_recipe(&recipe.physical, fields)?;
                next.physical_generation = family_physical_generation(&next);
                recipe.physical = track_recipe(&resident_tracker, next)?;
            }
            let after_state = catalog_recipe_state_resident_bytes(recipe).ok_or_else(|| {
                Status::resource_exhausted("active index catalog resident size overflow")
            })?;
            replace_resident_charge(state, before_state, after_state)?;
            return Ok(changed);
        }
        None => {
            let fields = definition
                .recipe_fingerprints
                .fields
                .iter()
                .copied()
                .zip(&definition.schema.fields)
                .map(|(fingerprint, field)| (fingerprint, Arc::new(field.clone())))
                .collect::<BTreeMap<_, _>>();
            let selectors = recipe_selectors(&fields);
            let projection_plan = CompiledScalarProjectionPlan::compile(Arc::clone(&selectors))
                .map_err(schema_status)?;
            let mut recipe = PhysicalCatalogRecipe {
                family: identity,
                storage_tenant: definition.stored.tenant.clone(),
                bucket: definition.stored.bucket.clone(),
                membership_recipe: definition.recipe_fingerprints.membership,
                path_prefix: definition.schema.path_prefix.clone(),
                content_type: definition.schema.content_type_scope.clone(),
                template: Arc::new({
                    let mut schema = definition.schema.clone();
                    schema.fields.clear();
                    schema.physical_order.clear();
                    schema
                }),
                fields,
                projection_plan,
                physical_generation: [0; 32],
                resident_lease: None,
            };
            recipe.physical_generation = family_physical_generation(&recipe);
            let physical = track_recipe(&resident_tracker, recipe)?;
            let recipe_state = CatalogRecipeState {
                rebuild_intents: definition
                    .stored
                    .last_explicit_rebuild_at_unix_millis()
                    .map(|accepted_at| (definition.stored.index_id, accepted_at))
                    .into_iter()
                    .collect(),
                physical,
                references: 1,
                field_references: definition
                    .recipe_fingerprints
                    .fields
                    .iter()
                    .copied()
                    .map(|fingerprint| (fingerprint, 1))
                    .collect(),
            };
            state.resident_bytes = state
                .resident_bytes
                .checked_add(
                    catalog_recipe_state_resident_bytes(&recipe_state).ok_or_else(|| {
                        Status::resource_exhausted("active index catalog resident size overflow")
                    })?,
                )
                .ok_or_else(|| {
                    Status::resource_exhausted("active index catalog resident size overflow")
                })?;
            state.recipes.insert(identity, recipe_state);
        }
    }
    Ok(true)
}

fn remove_recipe_reference(
    state: &mut CatalogState,
    binding: &LogicalCatalogBinding,
) -> Result<bool, Status> {
    let resident_tracker = Arc::clone(&state.recipe_resident);
    let field_ids = state
        .query_contracts
        .get(&binding.query_contract)
        .map(|contract| {
            contract
                .public_fields
                .iter()
                .map(|(_, id)| *id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let remove = match state.recipes.get_mut(&binding.family) {
        Some(recipe) if recipe.references > 1 => {
            let before_state = catalog_recipe_state_resident_bytes(recipe).ok_or_else(|| {
                Status::resource_exhausted("active index catalog resident size overflow")
            })?;
            let mut fields = recipe.physical.fields.clone();
            let old_fields = fields.len();
            recipe.references -= 1;
            if binding.explicit_rebuild_at_unix_millis.is_some() {
                recipe.rebuild_intents.remove(&binding.identity.index_id);
            }
            for field in field_ids {
                match recipe.field_references.get_mut(&field) {
                    Some(references) if *references > 1 => *references -= 1,
                    Some(_) => {
                        recipe.field_references.remove(&field);
                        fields.remove(&field);
                    }
                    None => {}
                }
            }
            let changed = fields.len() != old_fields;
            if changed {
                let mut next = clone_physical_recipe(&recipe.physical, fields)?;
                next.physical_generation = family_physical_generation(&next);
                recipe.physical = track_recipe(&resident_tracker, next)?;
            }
            let after_state = catalog_recipe_state_resident_bytes(recipe).ok_or_else(|| {
                Status::resource_exhausted("active index catalog resident size overflow")
            })?;
            replace_resident_charge(state, before_state, after_state)?;
            return Ok(changed);
        }
        Some(_) => true,
        None => false,
    };
    if remove && let Some(recipe) = state.recipes.remove(&binding.family) {
        let charge = catalog_recipe_state_resident_bytes(&recipe).ok_or_else(|| {
            Status::resource_exhausted("active index catalog resident size overflow")
        })?;
        state.resident_bytes = state.resident_bytes.saturating_sub(charge);
    }
    Ok(remove)
}

fn binding_resident_bytes(binding: &LogicalCatalogBinding) -> Result<usize, Status> {
    let _ = binding;
    Ok(std::mem::size_of::<LogicalCatalogBinding>() + 64)
}

fn catalog_recipe_state_resident_bytes(recipe: &CatalogRecipeState) -> Option<usize> {
    std::mem::size_of::<CatalogRecipeState>()
        .checked_add(64)?
        .checked_add(
            recipe
                .field_references
                .len()
                .checked_mul(std::mem::size_of::<([u8; 32], usize)>() + 64)?,
        )?
        .checked_add(
            recipe
                .rebuild_intents
                .len()
                .checked_mul(std::mem::size_of::<(u64, u64)>() + 64)?,
        )
}

fn replace_resident_charge(
    state: &mut CatalogState,
    before: usize,
    after: usize,
) -> Result<(), Status> {
    if after >= before {
        state.resident_bytes = state
            .resident_bytes
            .checked_add(after - before)
            .ok_or_else(|| {
                Status::resource_exhausted("active index catalog resident size overflow")
            })?;
    } else {
        state.resident_bytes = state.resident_bytes.saturating_sub(before - after);
    }
    Ok(())
}

fn query_contract_resident_bytes(contract: &LogicalQueryContract) -> Result<usize, Status> {
    (std::mem::size_of::<LogicalQueryContract>() + 64)
        .checked_add(contract.public_fields._permit.bytes())
        .ok_or_else(|| Status::resource_exhausted("active index catalog resident size overflow"))
}

fn selector_arc_resident_bytes(selectors: &[String]) -> usize {
    (2 * std::mem::size_of::<usize>())
        .saturating_add(std::mem::size_of_val(selectors))
        .saturating_add(selectors.iter().map(String::capacity).sum::<usize>())
}

fn projection_bundle_resident_bytes(
    selectors: &[String],
    projection_plan: &CompiledScalarProjectionPlan,
) -> usize {
    selector_arc_resident_bytes(selectors)
        .saturating_add(projection_plan.descriptor_resident_bytes())
}

fn clone_physical_recipe(
    current: &PhysicalCatalogRecipe,
    fields: BTreeMap<[u8; 32], Arc<FieldSchema>>,
) -> Result<PhysicalCatalogRecipe, Status> {
    let selectors = recipe_selectors(&fields);
    let projection_plan =
        CompiledScalarProjectionPlan::compile(Arc::clone(&selectors)).map_err(schema_status)?;
    Ok(PhysicalCatalogRecipe {
        family: current.family,
        storage_tenant: current.storage_tenant.clone(),
        bucket: current.bucket.clone(),
        membership_recipe: current.membership_recipe,
        path_prefix: current.path_prefix.clone(),
        content_type: current.content_type.clone(),
        template: Arc::clone(&current.template),
        fields,
        projection_plan,
        physical_generation: current.physical_generation,
        resident_lease: None,
    })
}

fn track_recipe(
    tracker: &Arc<RecipeResidentTracker>,
    mut recipe: PhysicalCatalogRecipe,
) -> Result<Arc<PhysicalCatalogRecipe>, Status> {
    let bytes = recipe_resident_bytes(&recipe)
        .ok_or_else(|| Status::resource_exhausted("active index catalog resident size overflow"))?;
    let permit = tracker
        .credits
        .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
        .map_err(|_| {
            Status::resource_exhausted("catalog recipe shared working memory is exhausted")
        })?;
    tracker
        .bytes
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(bytes)
        })
        .map_err(|_| Status::resource_exhausted("active index catalog resident size overflow"))?;
    let lease = Arc::new(RecipeResidentLease {
        tracker: Arc::clone(tracker),
        bytes,
        _permit: permit,
    });
    Arc::get_mut(&mut recipe.projection_plan)
        .expect("new catalog projection plan is uniquely owned")
        .set_resident_guard(lease.clone());
    recipe.resident_lease = Some(lease);
    Ok(Arc::new(recipe))
}

fn track_snapshot(
    tracker: &Arc<RecipeResidentTracker>,
    generation: u64,
    identity: [u8; 32],
    recipes: Vec<Arc<PhysicalCatalogRecipe>>,
) -> Result<Arc<PhysicalCatalogSnapshot>, Status> {
    let bytes = std::mem::size_of::<PhysicalCatalogSnapshot>()
        .checked_add(2 * std::mem::size_of::<usize>())
        .and_then(|bytes| {
            bytes.checked_add(
                recipes
                    .len()
                    .saturating_mul(std::mem::size_of::<Arc<PhysicalCatalogRecipe>>()),
            )
        })
        .ok_or_else(|| Status::resource_exhausted("active index catalog resident size overflow"))?;
    let permit = tracker
        .credits
        .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
        .map_err(|_| {
            Status::resource_exhausted("catalog snapshot shared working memory is exhausted")
        })?;
    tracker
        .bytes
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(bytes)
        })
        .map_err(|_| Status::resource_exhausted("active index catalog resident size overflow"))?;
    Ok(Arc::new(PhysicalCatalogSnapshot {
        generation,
        identity,
        recipes: Arc::from(recipes),
        resident_lease: Some(Arc::new(RecipeResidentLease {
            tracker: Arc::clone(tracker),
            bytes,
            _permit: permit,
        })),
    }))
}

fn recipe_resident_bytes(recipe: &PhysicalCatalogRecipe) -> Option<usize> {
    let mut bytes = std::mem::size_of::<PhysicalCatalogRecipe>()
        .checked_add(64)?
        .checked_add(recipe.storage_tenant.capacity())?
        .checked_add(recipe.bucket.capacity())?
        .checked_add(recipe.path_prefix.capacity())?
        .checked_add(recipe.content_type.as_ref().map_or(0, String::capacity))?
        .checked_add(
            recipe
                .fields
                .len()
                .checked_mul(std::mem::size_of::<([u8; 32], Arc<FieldSchema>)>() + 64)?,
        )?
        .checked_add(projection_bundle_resident_bytes(
            recipe.projection_plan.pointers(),
            &recipe.projection_plan,
        ))?;
    for field in recipe.fields.values() {
        bytes = bytes
            .checked_add(field.name.capacity())?
            .checked_add(field.source_selector.capacity())?;
    }
    Some(bytes)
}

fn recipe_selectors(fields: &BTreeMap<[u8; 32], Arc<FieldSchema>>) -> Arc<[String]> {
    Arc::from(
        fields
            .values()
            .map(|field| field.source_selector.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
    )
}

/// Bind explicit rebuilds to the existing physical generation, not the stable
/// family identity. Aliases rebuild their one shared family together. Removing
/// an alias with rebuild intent changes this derived generation and may rebuild
/// the surviving family once; no deleted definition leaves a hidden watermark.
fn refresh_family_rebuild_generation(
    state: &mut CatalogState,
    family: ProjectionFamilyIdentity,
) -> Result<bool, Status> {
    let Some(current) = state.recipes.get(&family) else {
        return Ok(false);
    };
    let semantic_generation = family_physical_generation(&current.physical);
    let mut rebuild_hasher = None;
    // BTreeMap order is stable logical index ID order within this family.
    // Ordinary object-version and public field-name changes are not rebuilds.
    for (index_id, accepted_at) in &current.rebuild_intents {
        let hasher = rebuild_hasher.get_or_insert_with(|| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"keldra.index.physical-family-explicit-rebuild/v1");
            hasher.update(&semantic_generation);
            hasher
        });
        hasher.update(&index_id.to_be_bytes());
        hasher.update(&accepted_at.to_be_bytes());
    }
    // Preserve the exact existing generation algorithm for no-rebuild families.
    let generation =
        rebuild_hasher.map_or(semantic_generation, |hasher| *hasher.finalize().as_bytes());
    if generation == current.physical.physical_generation {
        return Ok(false);
    }
    let mut next = clone_physical_recipe(&current.physical, current.physical.fields.clone())?;
    next.physical_generation = generation;
    let next = track_recipe(&state.recipe_resident, next)?;
    state
        .recipes
        .get_mut(&family)
        .expect("checked physical family exists")
        .physical = next;
    Ok(true)
}

fn family_physical_generation(recipe: &PhysicalCatalogRecipe) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra.index.physical-family-catalog/v1");
    hasher.update(&recipe.family.family_id);
    hasher.update(&recipe.membership_recipe);
    hasher.update(&(recipe.path_prefix.len() as u64).to_be_bytes());
    hasher.update(recipe.path_prefix.as_bytes());
    match &recipe.content_type {
        Some(content_type) => {
            hasher.update(&[1]);
            hasher.update(&(content_type.len() as u64).to_be_bytes());
            hasher.update(content_type.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    };
    for field in recipe.fields.keys() {
        hasher.update(field);
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
