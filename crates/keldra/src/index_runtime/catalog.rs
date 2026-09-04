//! Process-local active index catalog compiled from ordinary definitions.
//!
//! The catalog is a disposable, version-monotonic projection. Definition
//! changes mutate active state directly and broadcast only a best-effort wake;
//! there is no bounded builder handoff or per-definition assignment queue.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use keldra_index::typed_json::{FieldId, FieldSchema, RecipeFingerprints, TypedJsonSchema};
use keldra_index::v6::{
    IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryPermit, IndexingMemoryStage,
};
use tonic::Status;

use crate::index_service::{StoredIndexDefinition, definition_path};

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

/// Exact format-v6 physical-family identity for one tenant/bucket source scope.
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
        let specification = stored.specification()?;
        let schema = compile_typed_json_schema(
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
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalQueryContract {
    pub(crate) identity: [u8; 32],
    pub(crate) public_fields: Arc<Vec<(String, [u8; 32])>>,
    references: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalCatalogRecipe {
    pub(crate) family: ProjectionFamilyIdentity,
    pub(crate) storage_tenant: String,
    pub(crate) bucket: String,
    pub(crate) membership_recipe: [u8; 32],
    pub(crate) path_prefix: String,
    pub(crate) content_type: Option<String>,
    template: Arc<TypedJsonSchema>,
    pub(crate) fields: BTreeMap<[u8; 32], Arc<FieldSchema>>,
    pub(crate) physical_generation: [u8; 32],
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
}

#[derive(Clone)]
pub(crate) struct IndexCatalog {
    inner: Arc<Mutex<CatalogState>>,
    changes: tokio::sync::broadcast::Sender<CatalogNotice>,
    _ordering_catalog_memory: Arc<IndexingMemoryPermit>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CatalogNotice {
    pub(crate) identity: CatalogIdentity,
    pub(crate) physical_changed: bool,
}

struct CatalogState {
    bindings: BTreeMap<CatalogIdentity, LogicalCatalogBinding>,
    recipes: BTreeMap<ProjectionFamilyIdentity, PhysicalCatalogRecipe>,
    query_contracts: BTreeMap<[u8; 32], LogicalQueryContract>,
    generation: u64,
    physical_generation: [u8; 32],
    resident_bytes: usize,
    maximum_bytes: usize,
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
            .acquire(IndexingMemoryStage::OrderingCatalog, maximum_bytes)
            .map_err(|_| Status::resource_exhausted("active index catalog memory unavailable"))?;
        let (changes, _) = tokio::sync::broadcast::channel(1_024);
        Ok(Self {
            inner: Arc::new(Mutex::new(CatalogState {
                bindings: BTreeMap::new(),
                recipes: BTreeMap::new(),
                query_contracts: BTreeMap::new(),
                generation: 1,
                physical_generation: physical_catalog_generation(std::iter::empty::<
                    &PhysicalCatalogRecipe,
                >()),
                resident_bytes: 0,
                maximum_bytes,
            })),
            changes,
            _ordering_catalog_memory: Arc::new(permit),
        })
    }
    pub(crate) fn upsert(&self, definition: CatalogDefinition) -> Result<(), Status> {
        definition.validate()?;
        let identity = definition.identity();
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
        ensure_upsert_capacity(&state, &definition)?;
        let mut physical_changed = false;
        if let Some(previous) = state.bindings.remove(&identity) {
            state.resident_bytes = state
                .resident_bytes
                .saturating_sub(binding_resident_bytes(&previous)?);
            physical_changed |= remove_recipe_reference(&mut state, &previous);
            remove_query_contract_reference(&mut state, previous.query_contract);
        }
        let binding = compact_binding(&definition);
        physical_changed |= add_recipe_reference(&mut state, &definition)?;
        add_query_contract_reference(&mut state, &definition, binding.query_contract)?;
        state.resident_bytes = state
            .resident_bytes
            .checked_add(binding_resident_bytes(&binding)?)
            .ok_or_else(|| {
                Status::resource_exhausted("active index catalog resident size overflow")
            })?;
        state.bindings.insert(identity, binding);
        mark_catalog_changed(&mut state, physical_changed)?;
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
        let mut physical_changed = false;
        if let Some(previous) = state.bindings.remove(&identity) {
            state.resident_bytes = state
                .resident_bytes
                .saturating_sub(binding_resident_bytes(&previous)?);
            physical_changed |= remove_recipe_reference(&mut state, &previous);
            remove_query_contract_reference(&mut state, previous.query_contract);
        }
        mark_catalog_changed(&mut state, physical_changed)?;
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
        let Some(previous) = state.bindings.remove(&identity) else {
            return Ok(());
        };
        state.resident_bytes = state
            .resident_bytes
            .saturating_sub(binding_resident_bytes(&previous)?);
        let physical_changed = remove_recipe_reference(&mut state, &previous);
        remove_query_contract_reference(&mut state, previous.query_contract);
        mark_catalog_changed(&mut state, physical_changed)?;
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

    pub(crate) fn snapshot(
        &self,
    ) -> Result<
        (
            u64,
            [u8; 32],
            Vec<LogicalCatalogBinding>,
            Vec<PhysicalCatalogRecipe>,
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
            state.recipes.values().cloned().collect(),
            state.query_contracts.values().cloned().collect(),
        ))
    }

    pub(crate) fn resident_bytes(&self) -> Result<usize, Status> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?
            .resident_bytes)
    }

    pub(crate) fn hot_router_snapshot(
        &self,
    ) -> Result<([u8; 32], Vec<super::hot_ingress::CompiledHotRoute>), Status> {
        let state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        let routes = state
            .recipes
            .values()
            .map(|recipe| super::hot_ingress::CompiledHotRoute {
                tenant_id: recipe.family.tenant_id,
                bucket_id: recipe.family.bucket_id,
                path_prefix: recipe.path_prefix.clone(),
                content_type: recipe.content_type.clone(),
                pointers: recipe
                    .template
                    .fields
                    .iter()
                    .map(|field| field.source_selector.clone())
                    .collect(),
            })
            .collect();
        Ok((state.physical_generation, routes))
    }
}

fn ensure_upsert_capacity(
    state: &CatalogState,
    definition: &CatalogDefinition,
) -> Result<(), Status> {
    let identity = definition.identity();
    let released = state
        .bindings
        .get(&identity)
        .map(binding_resident_bytes)
        .transpose()?
        .unwrap_or(0);
    let mut needed = binding_resident_bytes(&compact_binding_without_allocations(definition))?;
    let contract = query_contract_identity(definition);
    if !state.query_contracts.contains_key(&contract) {
        needed = needed
            .checked_add(estimated_query_contract_bytes(definition)?)
            .ok_or_else(|| Status::resource_exhausted("active index catalog size overflow"))?;
    }
    let family = definition.projection_family_identity();
    needed = needed
        .checked_add(estimated_new_family_recipe_bytes(
            state.recipes.get(&family),
            definition,
        )?)
        .ok_or_else(|| Status::resource_exhausted("active index catalog size overflow"))?;
    let projected = state
        .resident_bytes
        .saturating_sub(released)
        .checked_add(needed)
        .ok_or_else(|| Status::resource_exhausted("active index catalog size overflow"))?;
    if projected > state.maximum_bytes {
        return Err(Status::resource_exhausted(format!(
            "active index catalog requires {projected} bytes but its OrderingCatalog credit is {} bytes",
            state.maximum_bytes
        )));
    }
    Ok(())
}

fn compact_binding_without_allocations(definition: &CatalogDefinition) -> LogicalCatalogBinding {
    LogicalCatalogBinding {
        identity: definition.identity(),
        object_version: definition.object_version,
        family: definition.projection_family_identity(),
        query_contract: query_contract_identity(definition),
    }
}

fn estimated_query_contract_bytes(definition: &CatalogDefinition) -> Result<usize, Status> {
    definition.schema.fields.iter().try_fold(
        std::mem::size_of::<LogicalQueryContract>() + 64,
        |bytes, field| {
            bytes
                .checked_add(std::mem::size_of::<(String, [u8; 32])>() + field.name.len())
                .ok_or_else(|| Status::resource_exhausted("active index catalog size overflow"))
        },
    )
}

fn estimated_new_family_recipe_bytes(
    current: Option<&PhysicalCatalogRecipe>,
    definition: &CatalogDefinition,
) -> Result<usize, Status> {
    let mut bytes = if current.is_none() {
        std::mem::size_of::<PhysicalCatalogRecipe>()
            + 64
            + definition.stored.tenant.len()
            + definition.stored.bucket.len()
            + definition.schema.path_prefix.len()
            + definition
                .schema
                .content_type_scope
                .as_ref()
                .map_or(0, String::len)
    } else {
        0
    };
    for (fingerprint, field) in definition
        .recipe_fingerprints
        .fields
        .iter()
        .zip(&definition.schema.fields)
    {
        if current.is_some_and(|recipe| recipe.fields.contains_key(fingerprint)) {
            continue;
        }
        bytes = bytes
            .checked_add(
                std::mem::size_of::<([u8; 32], Arc<FieldSchema>)>()
                    + std::mem::size_of::<([u8; 32], usize)>()
                    + 128
                    + std::mem::size_of::<FieldSchema>()
                    + field.name.len()
                    + field.source_selector.len(),
            )
            .ok_or_else(|| Status::resource_exhausted("active index catalog size overflow"))?;
    }
    Ok(bytes)
}

fn mark_catalog_changed(state: &mut CatalogState, physical_changed: bool) -> Result<(), Status> {
    state.generation = state
        .generation
        .checked_add(1)
        .ok_or_else(|| Status::resource_exhausted("active index catalog generation overflow"))?;
    if physical_changed {
        state.physical_generation = physical_catalog_generation(state.recipes.values());
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
    let contract = LogicalQueryContract {
        identity,
        public_fields: Arc::new(
            definition
                .schema
                .fields
                .iter()
                .zip(definition.recipe_fingerprints.fields.iter().copied())
                .map(|(field, fingerprint)| (field.name.clone(), fingerprint))
                .collect(),
        ),
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
    match state.recipes.get_mut(&identity) {
        Some(recipe) => {
            let before = recipe_resident_bytes(recipe).ok_or_else(|| {
                Status::resource_exhausted("active index catalog resident size overflow")
            })?;
            let old_fields = recipe.fields.len();
            recipe.references = recipe.references.saturating_add(1);
            for (fingerprint, field) in definition
                .recipe_fingerprints
                .fields
                .iter()
                .copied()
                .zip(&definition.schema.fields)
            {
                *recipe.field_references.entry(fingerprint).or_default() += 1;
                recipe
                    .fields
                    .entry(fingerprint)
                    .or_insert_with(|| Arc::new(field.clone()));
            }
            let after = recipe_resident_bytes(recipe).ok_or_else(|| {
                Status::resource_exhausted("active index catalog resident size overflow")
            })?;
            state.resident_bytes = state
                .resident_bytes
                .checked_add(after.saturating_sub(before))
                .ok_or_else(|| {
                    Status::resource_exhausted("active index catalog resident size overflow")
                })?;
            let changed = recipe.fields.len() != old_fields;
            if changed {
                recipe.physical_generation = family_physical_generation(recipe);
            }
            return Ok(changed);
        }
        None => {
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
                fields: definition
                    .recipe_fingerprints
                    .fields
                    .iter()
                    .copied()
                    .zip(&definition.schema.fields)
                    .map(|(fingerprint, field)| (fingerprint, Arc::new(field.clone())))
                    .collect(),
                physical_generation: [0; 32],
                references: 1,
                field_references: definition
                    .recipe_fingerprints
                    .fields
                    .iter()
                    .copied()
                    .map(|fingerprint| (fingerprint, 1))
                    .collect(),
            };
            recipe.physical_generation = family_physical_generation(&recipe);
            state.resident_bytes = state
                .resident_bytes
                .checked_add(recipe_resident_bytes(&recipe).ok_or_else(|| {
                    Status::resource_exhausted("active index catalog resident size overflow")
                })?)
                .ok_or_else(|| {
                    Status::resource_exhausted("active index catalog resident size overflow")
                })?;
            state.recipes.insert(identity, recipe);
        }
    }
    Ok(true)
}

fn remove_recipe_reference(state: &mut CatalogState, binding: &LogicalCatalogBinding) -> bool {
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
            let before = recipe_resident_bytes(recipe).unwrap_or(0);
            let old_fields = recipe.fields.len();
            recipe.references -= 1;
            for field in field_ids {
                match recipe.field_references.get_mut(&field) {
                    Some(references) if *references > 1 => *references -= 1,
                    Some(_) => {
                        recipe.field_references.remove(&field);
                        recipe.fields.remove(&field);
                    }
                    None => {}
                }
            }
            let after = recipe_resident_bytes(recipe).unwrap_or(before);
            if recipe.fields.len() != old_fields {
                recipe.physical_generation = family_physical_generation(recipe);
            }
            state.resident_bytes = state
                .resident_bytes
                .saturating_sub(before.saturating_sub(after));
            return recipe.fields.len() != old_fields;
        }
        Some(_) => true,
        None => false,
    };
    if remove && let Some(recipe) = state.recipes.remove(&binding.family) {
        state.resident_bytes = state
            .resident_bytes
            .saturating_sub(recipe_resident_bytes(&recipe).unwrap_or(0));
    }
    remove
}

fn binding_resident_bytes(binding: &LogicalCatalogBinding) -> Result<usize, Status> {
    let _ = binding;
    Ok(std::mem::size_of::<LogicalCatalogBinding>() + 64)
}

fn query_contract_resident_bytes(contract: &LogicalQueryContract) -> Result<usize, Status> {
    contract.public_fields.iter().try_fold(
        std::mem::size_of::<LogicalQueryContract>() + 64,
        |bytes, (name, _)| {
            bytes
                .checked_add(std::mem::size_of::<(String, [u8; 32])>())
                .and_then(|value| value.checked_add(name.capacity()))
                .ok_or_else(|| {
                    Status::resource_exhausted("active index catalog resident size overflow")
                })
        },
    )
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
        .checked_add(
            recipe
                .field_references
                .len()
                .checked_mul(std::mem::size_of::<([u8; 32], usize)>() + 64)?,
        )?;
    for field in recipe.fields.values() {
        bytes = bytes
            .checked_add(field.name.capacity())?
            .checked_add(field.source_selector.capacity())?;
    }
    Some(bytes)
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
mod tests {
    use keldra_api::v1::{
        CreateIndexRequest, IndexField, IndexFieldCapability, IndexFieldCardinality,
        IndexSpecification, KeywordIndexField, TypedJsonIndexSpec, index_field,
    };

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
                            keldra_api::v1::index_specification::Specification::TypedJson(
                                TypedJsonIndexSpec {
                                    fields: vec![keyword_field("value", "/value")],
                                    physical_order: Vec::new(),
                                },
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

    fn wide_typed_definition(index_id: u64) -> CatalogDefinition {
        let fields = (0..32)
            .map(|field| IndexField {
                name: format!("public_field_name_{field}"),
                json_pointer: format!("/payload/value_{field}"),
                cardinality: IndexFieldCardinality::Single as i32,
                capabilities: vec![IndexFieldCapability::Exact as i32],
                field_type: Some(index_field::FieldType::Keyword(KeywordIndexField {})),
            })
            .collect();
        typed_definition(index_id, fields)
    }

    fn typed_definition(index_id: u64, fields: Vec<IndexField>) -> CatalogDefinition {
        CatalogDefinition::new(
            1,
            2,
            1,
            StoredIndexDefinition::create(
                "tenant".into(),
                CreateIndexRequest {
                    bucket: "bucket".into(),
                    name: format!("wide-{index_id}"),
                    path_prefix: "objects/".into(),
                    content_type: "application/json".into(),
                    specification: Some(IndexSpecification {
                        specification: Some(
                            keldra_api::v1::index_specification::Specification::TypedJson(
                                TypedJsonIndexSpec {
                                    fields,
                                    physical_order: Vec::new(),
                                },
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

    fn keyword_field(name: &str, pointer: &str) -> IndexField {
        IndexField {
            name: name.into(),
            json_pointer: pointer.into(),
            cardinality: IndexFieldCardinality::Single as i32,
            capabilities: vec![IndexFieldCapability::Exact as i32],
            field_type: Some(index_field::FieldType::Keyword(KeywordIndexField {})),
        }
    }

    #[test]
    fn changes_update_active_catalog_without_an_admission_queue() {
        let catalog = IndexCatalog::default();
        let first = definition(1, 2, 9);
        let mut replacement = first.clone();
        replacement.object_version = 2;
        catalog.upsert(first).unwrap();
        catalog.upsert(replacement.clone()).unwrap();
        let (_, _, bindings, recipes, contracts) = catalog.snapshot().unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].object_version, 2);
        assert_eq!(recipes.len(), 1);
        assert_eq!(contracts.len(), 1);
    }

    #[test]
    fn active_catalog_does_not_have_a_logical_definition_capacity_gate() {
        let catalog = IndexCatalog::default();
        let first = definition(1, 2, 9);
        let second = definition(3, 4, 10);
        catalog.upsert(first.clone()).unwrap();
        catalog.upsert(second).unwrap();
        assert_eq!(catalog.snapshot().unwrap().2.len(), 2);
        catalog
            .remove(first.tenant_id, first.bucket_id, first.stored.index_id)
            .unwrap();
        assert_eq!(catalog.snapshot().unwrap().2.len(), 1);
    }

    #[tokio::test]
    async fn definition_delete_is_version_monotonic() {
        let catalog = IndexCatalog::default();
        let definition = definition(1, 2, 9);
        let identity = definition.identity();
        catalog.upsert(definition).unwrap();
        catalog.delete_wait(identity, 2).await.unwrap();
        catalog.remove(1, 2, 9).unwrap();
        assert!(catalog.snapshot().unwrap().2.is_empty());
    }

    #[tokio::test]
    async fn ordered_recreation_does_not_retain_a_tombstone() {
        let catalog = IndexCatalog::default();
        let original = definition(1, 2, 9);
        let identity = original.identity();
        catalog.upsert(original).unwrap();
        catalog.delete_wait(identity, 3).await.unwrap();
        assert!(catalog.snapshot().unwrap().2.is_empty());

        let mut recreated = definition(1, 2, 9);
        recreated.object_version = 4;
        catalog.upsert(recreated).unwrap();
        assert_eq!(catalog.snapshot().unwrap().2[0].object_version, 4);
    }

    #[test]
    fn ordinary_definition_compiles_one_bound_typed_json_schema_and_fingerprint() {
        let definition = definition(1, 2, 9);
        assert_eq!(definition.schema.path_prefix, "");
        assert_eq!(definition.schema.fields[0].name, "value");
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
        let base = wide_typed_definition(1);
        let catalog = IndexCatalog::default();
        let mut first_physical_generation = None;
        for index_id in 1..=250_000_u64 {
            let mut logical = base.clone();
            logical.stored.index_id = index_id;
            logical.stored.name = format!("index-{index_id}");
            catalog.upsert(logical).unwrap();
            if index_id == 1 {
                first_physical_generation = Some(catalog.snapshot().unwrap().1);
            }
        }
        let (generation, physical_generation, logical, physical, contracts) =
            catalog.snapshot().unwrap();
        assert_eq!(generation, 250_001);
        assert_eq!(logical.len(), 250_000);
        assert_eq!(physical.len(), 1);
        assert_eq!(contracts.len(), 1);
        assert_eq!(physical[0].fields.len(), 32);
        assert_ne!(physical_generation, [0; 32]);
        assert_eq!(Some(physical_generation), first_physical_generation);
        // The active catalog retains compact logical bindings and one interned
        // schema. It must not approach the footprint of 250K cloned schemas.
        assert!(catalog.resident_bytes().unwrap() < 96 * 1024 * 1024);
    }

    #[test]
    fn overlapping_field_subsets_compile_to_one_family_union() {
        let catalog = IndexCatalog::default();
        catalog
            .upsert(typed_definition(
                1,
                vec![keyword_field("a", "/a"), keyword_field("b", "/b")],
            ))
            .unwrap();
        let first_generation = catalog.snapshot().unwrap().1;
        catalog
            .upsert(typed_definition(
                2,
                vec![keyword_field("bee", "/b"), keyword_field("c", "/c")],
            ))
            .unwrap();
        let (_, second_generation, bindings, families, contracts) = catalog.snapshot().unwrap();
        assert_eq!(bindings.len(), 2);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].fields.len(), 3);
        assert_eq!(contracts.len(), 2);
        assert_ne!(first_generation, second_generation);
        let projection = families[0].projection_schema().unwrap();
        assert_eq!(projection.fields.len(), 3);
        assert!(
            projection
                .fields
                .iter()
                .enumerate()
                .all(|(ordinal, field)| field.id.get() == ordinal as u32)
        );

        // An equivalent alias changes only the compact logical/query binding,
        // never the physical family generation or recipe union.
        catalog
            .upsert(typed_definition(
                3,
                vec![keyword_field("aye", "/a"), keyword_field("bee", "/b")],
            ))
            .unwrap();
        let (_, alias_generation, _, families, _) = catalog.snapshot().unwrap();
        assert_eq!(alias_generation, second_generation);
        assert_eq!(families[0].fields.len(), 3);
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
