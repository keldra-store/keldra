//! Bounded source-node projection shared by assigned index assemblers.
//!
//! The source journal and exact object versions remain authoritative. This
//! process-local mapper caches only disposable definition-neutral JSON facts
//! and prepared definition projections. A cache miss deterministically reads
//! and projects the exact source again.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use keldra_index::IndexKind;
use keldra_index::v4::build::MergeMutation;
use keldra_index::v4::{FieldId, FieldSchema, Schema};
use keldra_index::v5::{ProjectedDocumentState, projected_document_states};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;

use super::catalog::{
    CatalogDefinition, CatalogIdentity, PhysicalRecipeIdentity, ProjectionFamilyIdentity,
};
use super::cpu::IndexCpuPool;
use super::json_projection::{ProjectedScalarPointers, project_scalar_pointers};
use super::source::{IndexBuildDiagnostics, IndexBuildObject, IndexSourceMutation};
use super::v4_projection::{
    project_mutation, project_typed_json_from_shared_scalars, projected_mutation_resident_bytes,
    source_matches_schema,
};
use super::working_memory::{IndexWorkingMemory, WorkingMemoryAccount, WorkingMemoryPermit};

const MAPPER_STRIPES: usize = 64;
const CACHE_ENTRY_FIXED_BYTES: usize = 256;
const OUTPUT_ENTRY_FIXED_BYTES: usize = 128;
const PLAN_POINTER_FIXED_BYTES: usize = 128;
const TELEMETRY_REPORT_INTERVAL: u64 = 16_384;

#[derive(Clone)]
pub(crate) struct SharedProjectionMapper {
    inner: Arc<MapperInner>,
}

struct MapperInner {
    reader: ClusterObjectReader,
    cpu: IndexCpuPool,
    definitions: Mutex<DefinitionPlans>,
    cache: Mutex<ProjectionCache>,
    stripes: [tokio::sync::Mutex<()>; MAPPER_STRIPES],
    cache_limit: usize,
    mapping_limit: usize,
    telemetry: MapperTelemetry,
    _memory: WorkingMemoryPermit,
}

#[derive(Default)]
struct MapperTelemetry {
    requests: AtomicU64,
    payload_parses: AtomicU64,
    selected_hits: AtomicU64,
    prepared_hits: AtomicU64,
    union_bypasses: AtomicU64,
}

#[derive(Clone)]
struct RegisteredDefinition {
    tenant_id: u64,
    bucket_id: u64,
    route: ProjectionRoute,
    membership: PhysicalRecipeIdentity,
    selectors: Vec<String>,
    recipes: Vec<(PhysicalRecipeIdentity, String)>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProjectionRoute {
    path_prefix: String,
    content_type: Option<String>,
}

#[derive(Default)]
struct DefinitionPlans {
    definitions: BTreeMap<CatalogIdentity, RegisteredDefinition>,
    logical_families: BTreeMap<CatalogIdentity, ProjectionFamilyIdentity>,
    families: BTreeMap<ProjectionFamilyIdentity, RegisteredFamily>,
    buckets: BTreeMap<(u64, u64), BucketSelectorPlan>,
    membership_recipes: BTreeMap<PhysicalRecipeIdentity, RegisteredMembership>,
    field_recipes: BTreeMap<PhysicalRecipeIdentity, RegisteredRecipe>,
}

struct RegisteredFamily {
    storage_tenant: String,
    bucket: String,
    template: Schema,
    definitions: usize,
    fields: BTreeMap<[u8; 32], RegisteredFamilyField>,
}

#[derive(Clone)]
pub(crate) struct ProjectionFamilyPlan {
    pub(crate) identity: ProjectionFamilyIdentity,
    pub(crate) storage_tenant: String,
    pub(crate) bucket: String,
    pub(crate) schema: Schema,
    pub(crate) schema_fingerprint: [u8; 32],
}

struct RegisteredFamilyField {
    field: FieldSchema,
    references: usize,
}

struct RegisteredMembership {
    route: ProjectionRoute,
    references: usize,
}

struct RegisteredRecipe {
    selector: String,
    references: usize,
}

#[derive(Default)]
struct BucketSelectorPlan {
    routes: BTreeMap<ProjectionRoute, RouteSelectorPlan>,
}

#[derive(Default)]
struct RouteSelectorPlan {
    selector_references: BTreeMap<String, usize>,
    compiled: Option<CompiledSelectorPlan>,
}

#[derive(Clone)]
struct CompiledSelectorPlan {
    pointers: Arc<Vec<String>>,
    workspace_bytes: usize,
    plan_hash: [u8; 32],
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct SourceProjectionKey {
    tenant_id: u64,
    bucket_id: u64,
    path: String,
    version: u64,
    committed_at_unix_millis: u64,
    content_type: Option<String>,
    content_hash: [u8; 32],
    content_length: u64,
    plan_hash: [u8; 32],
}

struct ProjectionCache {
    entries: HashMap<SourceProjectionKey, CachedProjection>,
    recency: BTreeSet<(u64, SourceProjectionKey)>,
    used_bytes: usize,
    clock: u64,
}

struct CachedProjection {
    selected: Option<Arc<ProjectedScalarPointers>>,
    outputs: HashMap<[u8; 32], CachedOutput>,
    resident_bytes: usize,
    last_used: u64,
}

struct CachedOutput {
    mutation: Arc<MergeMutation>,
    diagnostics: IndexBuildDiagnostics,
    resident_bytes: usize,
}

struct ProjectionPlan {
    key: SourceProjectionKey,
    pointers: Arc<Vec<String>>,
    workspace_bytes: usize,
}

fn add_registered_definition(
    plans: &mut DefinitionPlans,
    definition: &RegisteredDefinition,
) -> Result<(), Status> {
    let recipe_increments = recipe_reference_increments(definition)?;
    let selector_increments = selector_reference_increments(definition)?;
    if plans
        .membership_recipes
        .get(&definition.membership)
        .is_some_and(|recipe| recipe.route != definition.route)
        || definition.recipes.iter().any(|(identity, selector)| {
            plans
                .field_recipes
                .get(identity)
                .is_some_and(|recipe| recipe.selector != *selector)
        })
    {
        return Err(Status::data_loss(
            "physical recipe identity resolved to conflicting canonical semantics",
        ));
    }
    plans
        .membership_recipes
        .get(&definition.membership)
        .map_or(Ok(()), |recipe| {
            recipe.references.checked_add(1).map(|_| ()).ok_or_else(|| {
                Status::resource_exhausted("membership recipe reference count overflow")
            })
        })?;
    for (identity, increment) in &recipe_increments {
        if let Some(recipe) = plans.field_recipes.get(identity) {
            recipe.references.checked_add(*increment).ok_or_else(|| {
                Status::resource_exhausted("field recipe reference count overflow")
            })?;
        }
    }
    let route_references = plans
        .buckets
        .get(&(definition.tenant_id, definition.bucket_id))
        .and_then(|bucket| bucket.routes.get(&definition.route));
    for (selector, increment) in &selector_increments {
        if let Some(references) =
            route_references.and_then(|route| route.selector_references.get(selector))
        {
            references
                .checked_add(*increment)
                .ok_or_else(|| Status::resource_exhausted("selector reference count overflow"))?;
        }
    }
    match plans.membership_recipes.get_mut(&definition.membership) {
        Some(recipe) => {
            if recipe.route != definition.route {
                return Err(Status::data_loss(
                    "one physical membership recipe resolved to conflicting routes",
                ));
            }
            recipe.references = recipe.references.checked_add(1).ok_or_else(|| {
                Status::resource_exhausted("membership recipe reference count overflow")
            })?;
        }
        None => {
            plans.membership_recipes.insert(
                definition.membership,
                RegisteredMembership {
                    route: definition.route.clone(),
                    references: 1,
                },
            );
        }
    }
    for (identity, selector) in &definition.recipes {
        match plans.field_recipes.get_mut(identity) {
            Some(recipe) => {
                if recipe.selector != *selector {
                    return Err(Status::data_loss(
                        "one physical field recipe resolved to conflicting selectors",
                    ));
                }
                recipe.references = recipe.references.checked_add(1).ok_or_else(|| {
                    Status::resource_exhausted("field recipe reference count overflow")
                })?;
            }
            None => {
                plans.field_recipes.insert(
                    *identity,
                    RegisteredRecipe {
                        selector: selector.clone(),
                        references: 1,
                    },
                );
            }
        }
    }
    let bucket = plans
        .buckets
        .entry((definition.tenant_id, definition.bucket_id))
        .or_default();
    for selector in &definition.selectors {
        let route = bucket.routes.entry(definition.route.clone()).or_default();
        let references = route
            .selector_references
            .entry(selector.clone())
            .or_default();
        *references = references
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("selector reference count overflow"))?;
    }
    let route = bucket
        .routes
        .get_mut(&definition.route)
        .expect("inserted projection route remains present");
    route.compiled = Some(compile_selector_references(&route.selector_references));
    Ok(())
}

fn remove_registered_definition(
    plans: &mut DefinitionPlans,
    definition: RegisteredDefinition,
) -> Result<(), Status> {
    let recipe_decrements = recipe_reference_increments(&definition)?;
    let selector_decrements = selector_reference_increments(&definition)?;
    let key = (definition.tenant_id, definition.bucket_id);
    let route = plans
        .buckets
        .get(&key)
        .and_then(|bucket| bucket.routes.get(&definition.route))
        .ok_or_else(|| Status::internal("registered projection route is absent"))?;
    if !plans
        .membership_recipes
        .get(&definition.membership)
        .is_some_and(|recipe| recipe.route == definition.route && recipe.references > 0)
        || definition.recipes.iter().any(|(identity, selector)| {
            !plans
                .field_recipes
                .get(identity)
                .is_some_and(|recipe| recipe.selector == *selector && recipe.references > 0)
        })
        || recipe_decrements.iter().any(|(identity, decrement)| {
            !plans
                .field_recipes
                .get(identity)
                .is_some_and(|recipe| recipe.references >= *decrement)
        })
        || selector_decrements.iter().any(|(selector, decrement)| {
            !route
                .selector_references
                .get(selector)
                .is_some_and(|references| *references >= *decrement)
        })
    {
        return Err(Status::internal(
            "registered physical recipe reference is absent or inconsistent",
        ));
    }
    let remove_membership = {
        let recipe = plans
            .membership_recipes
            .get_mut(&definition.membership)
            .ok_or_else(|| Status::internal("registered membership recipe is absent"))?;
        if recipe.route != definition.route {
            return Err(Status::data_loss(
                "registered membership recipe route changed",
            ));
        }
        recipe.references = recipe
            .references
            .checked_sub(1)
            .ok_or_else(|| Status::internal("membership recipe reference count underflow"))?;
        recipe.references == 0
    };
    if remove_membership {
        plans.membership_recipes.remove(&definition.membership);
    }
    for (identity, selector) in &definition.recipes {
        let remove = {
            let recipe = plans
                .field_recipes
                .get_mut(identity)
                .ok_or_else(|| Status::internal("registered physical field recipe is absent"))?;
            if recipe.selector != *selector {
                return Err(Status::data_loss(
                    "registered physical field recipe selector changed",
                ));
            }
            recipe.references = recipe.references.checked_sub(1).ok_or_else(|| {
                Status::internal("physical field recipe reference count underflow")
            })?;
            recipe.references == 0
        };
        if remove {
            plans.field_recipes.remove(identity);
        }
    }
    let Some(bucket) = plans.buckets.get_mut(&key) else {
        return Err(Status::internal(
            "registered projection definition has no compiled bucket plan",
        ));
    };
    let route = bucket.routes.get_mut(&definition.route).ok_or_else(|| {
        Status::internal("registered projection definition has no compiled route")
    })?;
    for selector in &definition.selectors {
        let remove = {
            let references = route.selector_references.get_mut(selector).ok_or_else(|| {
                Status::internal("registered projection selector reference is absent")
            })?;
            *references = references.checked_sub(1).ok_or_else(|| {
                Status::internal("registered projection selector reference underflow")
            })?;
            *references == 0
        };
        if remove {
            route.selector_references.remove(selector);
        }
    }
    if route.selector_references.is_empty() {
        bucket.routes.remove(&definition.route);
    } else {
        route.compiled = Some(compile_selector_references(&route.selector_references));
    }
    if bucket.routes.is_empty() {
        plans.buckets.remove(&key);
    }
    Ok(())
}

fn recipe_reference_increments(
    definition: &RegisteredDefinition,
) -> Result<BTreeMap<PhysicalRecipeIdentity, usize>, Status> {
    let mut increments = BTreeMap::new();
    for (identity, _) in &definition.recipes {
        let count = increments.entry(*identity).or_insert(0usize);
        *count = count
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("field recipe reference count overflow"))?;
    }
    Ok(increments)
}

fn selector_reference_increments(
    definition: &RegisteredDefinition,
) -> Result<BTreeMap<String, usize>, Status> {
    let mut increments = BTreeMap::new();
    for selector in &definition.selectors {
        let count = increments.entry(selector.clone()).or_insert(0usize);
        *count = count
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("selector reference count overflow"))?;
    }
    Ok(increments)
}

fn add_family_definition(
    plans: &mut DefinitionPlans,
    definition: &CatalogDefinition,
) -> Result<(), Status> {
    if definition.schema.kind != IndexKind::TypedJson {
        return Ok(());
    }
    let identity = definition.projection_family_identity();
    let mut template = definition.schema.clone();
    template.fields.clear();
    template.physical_order.clear();
    let recipes = definition
        .recipe_fingerprints
        .fields
        .iter()
        .copied()
        .zip(&definition.schema.fields)
        .collect::<Vec<_>>();
    if let Some(family) = plans.families.get(&identity) {
        if family.template != template {
            return Err(Status::data_loss(
                "one projection family resolved to conflicting membership semantics",
            ));
        }
        family
            .definitions
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("projection family references overflow"))?;
        for (recipe, field) in &recipes {
            if let Some(existing) = family.fields.get(recipe) {
                if !same_physical_field(&existing.field, field) {
                    return Err(Status::data_loss(
                        "one field recipe resolved to conflicting physical semantics",
                    ));
                }
                existing.references.checked_add(1).ok_or_else(|| {
                    Status::resource_exhausted("projection family field references overflow")
                })?;
            }
        }
    }
    let family = plans
        .families
        .entry(identity)
        .or_insert_with(|| RegisteredFamily {
            storage_tenant: definition.stored.tenant.clone(),
            bucket: definition.stored.bucket.clone(),
            template,
            definitions: 0,
            fields: BTreeMap::new(),
        });
    if family.storage_tenant != definition.stored.tenant
        || family.bucket != definition.stored.bucket
    {
        return Err(Status::data_loss(
            "one projection family resolved to conflicting storage authority",
        ));
    }
    family.definitions = family
        .definitions
        .checked_add(1)
        .ok_or_else(|| Status::resource_exhausted("projection family references overflow"))?;
    for (recipe, field) in recipes {
        match family.fields.get_mut(&recipe) {
            Some(existing) => {
                existing.references = existing.references.checked_add(1).ok_or_else(|| {
                    Status::resource_exhausted("projection family field references overflow")
                })?;
            }
            None => {
                family.fields.insert(
                    recipe,
                    RegisteredFamilyField {
                        field: field.clone(),
                        references: 1,
                    },
                );
            }
        }
    }
    if plans
        .logical_families
        .insert(definition.identity(), identity)
        .is_some()
    {
        return Err(Status::internal(
            "projection family logical definition was registered twice",
        ));
    }
    Ok(())
}

fn remove_family_definition(
    plans: &mut DefinitionPlans,
    identity: CatalogIdentity,
    definition: &RegisteredDefinition,
) -> Result<(), Status> {
    let Some(family_identity) = plans.logical_families.remove(&identity) else {
        return Ok(());
    };
    let remove_family = {
        let family = plans
            .families
            .get_mut(&family_identity)
            .ok_or_else(|| Status::internal("registered projection family is absent"))?;
        family.definitions = family
            .definitions
            .checked_sub(1)
            .ok_or_else(|| Status::internal("projection family references underflow"))?;
        let recipes = definition
            .recipes
            .iter()
            .map(|(identity, _)| identity.fingerprint)
            .collect::<Vec<_>>();
        for recipe in recipes {
            let field = family
                .fields
                .get_mut(&recipe)
                .expect("collected family field remains present");
            field.references = field
                .references
                .checked_sub(1)
                .ok_or_else(|| Status::internal("projection family field references underflow"))?;
            if field.references == 0 {
                family.fields.remove(&recipe);
            }
        }
        family.definitions == 0
    };
    if remove_family {
        plans.families.remove(&family_identity);
    }
    Ok(())
}

fn same_physical_field(left: &FieldSchema, right: &FieldSchema) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.id = FieldId::new(0);
    right.id = FieldId::new(0);
    left.name.clear();
    right.name.clear();
    left == right
}

fn compile_selector_plan(schema: &Schema) -> CompiledSelectorPlan {
    let selectors = schema
        .fields
        .iter()
        .map(|field| field.source_selector.clone())
        .collect::<BTreeSet<_>>();
    compile_selectors(selectors.into_iter())
}

fn compile_selector_references(selectors: &BTreeMap<String, usize>) -> CompiledSelectorPlan {
    compile_selectors(selectors.keys().cloned())
}

fn matching_selector_plans(
    bucket: &BucketSelectorPlan,
    object: &IndexBuildObject,
) -> Vec<CompiledSelectorPlan> {
    let mut prefixes = BTreeSet::from([String::new(), object.path.clone()]);
    for (offset, byte) in object.path.bytes().enumerate() {
        if byte == b'/' {
            prefixes.insert(object.path[..offset].to_owned());
            prefixes.insert(object.path[..=offset].to_owned());
        }
    }
    let mut routes = BTreeSet::new();
    for path_prefix in prefixes {
        routes.insert(ProjectionRoute {
            path_prefix: path_prefix.clone(),
            content_type: None,
        });
        if let Some(content_type) = object.content_type.as_ref() {
            routes.insert(ProjectionRoute {
                path_prefix,
                content_type: Some(content_type.clone()),
            });
        }
    }
    routes
        .into_iter()
        .filter_map(|route| bucket.routes.get(&route)?.compiled.clone())
        .collect()
}

fn merge_selector_plans(plans: Vec<CompiledSelectorPlan>) -> Option<CompiledSelectorPlan> {
    if plans.len() == 1 {
        return plans.into_iter().next();
    }
    let mut pointers = BTreeSet::new();
    for plan in plans {
        pointers.extend(plan.pointers.iter().cloned());
    }
    (!pointers.is_empty()).then(|| compile_selectors(pointers.into_iter()))
}

fn compile_selectors(selectors: impl Iterator<Item = String>) -> CompiledSelectorPlan {
    let pointers = selectors.collect::<Vec<_>>();
    let workspace_bytes = pointers.iter().fold(0usize, |bytes, pointer| {
        bytes.saturating_add(
            PLAN_POINTER_FIXED_BYTES
                .saturating_add(pointer.len())
                .saturating_mul(2),
        )
    });
    let mut plan = blake3::Hasher::new();
    for pointer in &pointers {
        plan.update(&(pointer.len() as u64).to_be_bytes());
        plan.update(pointer.as_bytes());
    }
    CompiledSelectorPlan {
        pointers: Arc::new(pointers),
        workspace_bytes,
        plan_hash: *plan.finalize().as_bytes(),
    }
}

impl SharedProjectionMapper {
    pub(crate) async fn new(
        reader: ClusterObjectReader,
        cpu: IndexCpuPool,
        memory: IndexWorkingMemory,
        bytes: u64,
    ) -> Result<Self, Status> {
        let permit = memory
            .acquire_up_to(WorkingMemoryAccount::SharedProjection, bytes, bytes)
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let admitted = usize::try_from(permit.bytes()).map_err(|_| {
            Status::failed_precondition("shared projection memory exceeds this platform")
        })?;
        let mapping_limit = admitted / 2;
        let cache_limit = admitted.saturating_sub(mapping_limit);
        if mapping_limit == 0 || cache_limit == 0 {
            return Err(Status::failed_precondition(
                "shared projection memory cannot provide cache and mapping workspace",
            ));
        }
        Ok(Self {
            inner: Arc::new(MapperInner {
                reader,
                cpu,
                definitions: Mutex::new(DefinitionPlans::default()),
                cache: Mutex::new(ProjectionCache {
                    entries: HashMap::new(),
                    recency: BTreeSet::new(),
                    used_bytes: 0,
                    clock: 0,
                }),
                stripes: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
                cache_limit,
                mapping_limit,
                telemetry: MapperTelemetry::default(),
                _memory: permit,
            }),
        })
    }

    pub(crate) fn upsert(&self, definition: &CatalogDefinition) -> Result<(), Status> {
        let mut plans = self
            .inner
            .definitions
            .lock()
            .map_err(|_| Status::internal("shared projection definition lock is poisoned"))?;
        if let Some(previous) = plans.definitions.remove(&definition.identity()) {
            remove_family_definition(&mut plans, definition.identity(), &previous)?;
            remove_registered_definition(&mut plans, previous)?;
        }
        if definition.schema.kind == IndexKind::TypedJson {
            let field_recipe_identities = definition.field_recipe_identities();
            let registered = RegisteredDefinition {
                tenant_id: definition.tenant_id,
                bucket_id: definition.bucket_id,
                route: ProjectionRoute {
                    path_prefix: definition.schema.path_prefix.clone(),
                    content_type: definition.schema.content_type_scope.clone(),
                },
                membership: definition.membership_recipe_identity(),
                selectors: definition
                    .schema
                    .fields
                    .iter()
                    .map(|field| field.source_selector.clone())
                    .collect(),
                recipes: field_recipe_identities
                    .into_iter()
                    .zip(
                        definition
                            .schema
                            .fields
                            .iter()
                            .map(|field| field.source_selector.clone()),
                    )
                    .collect(),
            };
            add_registered_definition(&mut plans, &registered)?;
            if let Err(error) = add_family_definition(&mut plans, definition) {
                remove_registered_definition(&mut plans, registered)?;
                return Err(error);
            }
            plans.definitions.insert(definition.identity(), registered);
        }
        Ok(())
    }

    pub(crate) fn remove(&self, identity: CatalogIdentity) -> Result<(), Status> {
        let mut plans = self
            .inner
            .definitions
            .lock()
            .map_err(|_| Status::internal("shared projection definition lock is poisoned"))?;
        if let Some(previous) = plans.definitions.remove(&identity) {
            remove_family_definition(&mut plans, identity, &previous)?;
            remove_registered_definition(&mut plans, previous)?;
        }
        Ok(())
    }

    /// Compile the current distinct physical recipe union for one family.
    /// Logical aliases and duplicate definitions consume no fields here; the
    /// returned schema grows only when a new physical recipe is registered.
    pub(crate) fn family_schema(
        &self,
        identity: ProjectionFamilyIdentity,
    ) -> Result<Option<Schema>, Status> {
        let plans = self
            .inner
            .definitions
            .lock()
            .map_err(|_| Status::internal("shared projection definition lock is poisoned"))?;
        let Some(family) = plans.families.get(&identity) else {
            return Ok(None);
        };
        compile_family_schema(family).map(Some)
    }

    pub(crate) fn family_plan(
        &self,
        identity: ProjectionFamilyIdentity,
    ) -> Result<Option<ProjectionFamilyPlan>, Status> {
        let plans = self
            .inner
            .definitions
            .lock()
            .map_err(|_| Status::internal("shared projection definition lock is poisoned"))?;
        let Some(family) = plans.families.get(&identity) else {
            return Ok(None);
        };
        let schema = compile_family_schema(family)?;
        let schema_fingerprint = schema.fingerprint().map_err(index_status)?;
        Ok(Some(ProjectionFamilyPlan {
            identity,
            storage_tenant: family.storage_tenant.clone(),
            bucket: family.bucket.clone(),
            schema,
            schema_fingerprint,
        }))
    }

    /// Project one source once through the current distinct-recipe union for a
    /// physical family. Logical aliases and equivalent definitions never enter
    /// this loop. Half of the caller's bound is reserved for the native
    /// projection and half for canonical persisted state while both coexist.
    pub(crate) async fn project_family(
        &self,
        identity: ProjectionFamilyIdentity,
        source: IndexSourceMutation,
        maximum_projection_bytes: usize,
    ) -> Result<(Vec<ProjectedDocumentState>, IndexBuildDiagnostics), Status> {
        let schema = self
            .family_schema(identity)?
            .ok_or_else(|| Status::failed_precondition("projection family is not registered"))?;
        let native_limit = maximum_projection_bytes / 2;
        if native_limit == 0 {
            return Err(Status::resource_exhausted(
                "projection family has no admitted native workspace",
            ));
        }
        let payload = match &source {
            IndexSourceMutation::Upsert(object) if source_matches_schema(&schema, object) => Some(
                self.inner
                    .reader
                    .open_blob_payload(&keldra_store::BlobRef {
                        hash: object.content_hash,
                        length: object.content_length,
                    })
                    .await?,
            ),
            _ => None,
        };
        let projected = self
            .inner
            .cpu
            .submit(move || {
                let mut payload = payload;
                project_mutation(
                    &schema,
                    source,
                    payload
                        .as_mut()
                        .map(|reader| reader as &mut dyn std::io::Read),
                    native_limit,
                )
                .and_then(|(mutation, diagnostics)| {
                    let states = match mutation {
                        MergeMutation::Upsert(source) => {
                            projected_document_states(&schema, &source)?
                        }
                        MergeMutation::Delete(_) => Vec::new(),
                    };
                    let retained = states.iter().try_fold(0_usize, |total, state| {
                        total
                            .checked_add(state.resident_bytes()?)
                            .ok_or(keldra_index::IndexError::OffsetOverflow)
                    })?;
                    if retained > native_limit {
                        return Err(keldra_index::IndexError::ResourceLimit {
                            needed: retained,
                            limit: native_limit,
                        });
                    }
                    Ok((states, diagnostics))
                })
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(index_status)?;
        tracing::debug!(
            projection.family = %hex::encode(identity.family_id),
            projection.records = projected.0.len(),
            monotonic_counter.keldra_index_projection_family_source_passes_total = 1_u64,
            "source projected once through distinct family recipes"
        );
        Ok(projected)
    }

    pub(crate) async fn project(
        &self,
        definition: &CatalogDefinition,
        source: IndexSourceMutation,
        maximum_projection_bytes: usize,
    ) -> Result<(MergeMutation, IndexBuildDiagnostics), Status> {
        self.observe_request();
        if definition.schema.kind != IndexKind::TypedJson {
            return Err(Status::internal(
                "non-Typed-JSON source entered the shared scalar mapper",
            ));
        }
        let object = match &source {
            IndexSourceMutation::Upsert(object)
                if source_matches_schema(&definition.schema, object) =>
            {
                object
            }
            _ => {
                return project_typed_json_from_shared_scalars(
                    &definition.schema,
                    source,
                    None,
                    maximum_projection_bytes,
                )
                .map_err(index_status);
            }
        };
        let plan = match self.plan(definition, object) {
            Ok(plan) => plan,
            Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                self.inner
                    .telemetry
                    .union_bypasses
                    .fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    projection.cache = "union_bypass",
                    monotonic_counter.keldra_index_shared_projection_union_bypasses_total = 1_u64,
                    "shared selector plan exceeded its workspace; using exact definition projection"
                );
                return self
                    .project_definition(definition, source, maximum_projection_bytes)
                    .await;
            }
            Err(status) => return Err(status),
        };
        if let Some(output) = self.cached_output(
            &plan.key,
            definition.schema_fingerprint,
            maximum_projection_bytes,
        )? {
            return Ok(output);
        }

        let stripe = cache_stripe(&plan.key);
        let _exclusive_source = self.inner.stripes[stripe].lock().await;
        if let Some(output) = self.cached_output(
            &plan.key,
            definition.schema_fingerprint,
            maximum_projection_bytes,
        )? {
            return Ok(output);
        }

        let selected = match self.cached_selected(&plan.key)? {
            Some(selected) => selected,
            None => match self.map_source(&plan, object).await {
                Ok(selected) => selected,
                // A union of many definitions can be larger than this
                // mapper's bounded workspace even though the requesting
                // definition still fits its own admitted builder lane. The
                // optimization must never introduce a new functional limit:
                // reopen the exact payload and use the released per-definition
                // projector for this source.
                Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                    self.inner
                        .telemetry
                        .union_bypasses
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        projection.cache = "union_bypass",
                        monotonic_counter.keldra_index_shared_projection_union_bypasses_total =
                            1_u64,
                        "shared selector union exceeded its workspace; using exact definition projection"
                    );
                    return self
                        .project_definition(definition, source, maximum_projection_bytes)
                        .await;
                }
                Err(status) => return Err(status),
            },
        };
        let schema = definition.schema.clone();
        let source_for_projection = source.clone();
        let selected_for_projection = selected.clone();
        let assembly_started = Instant::now();
        let projected = self
            .inner
            .cpu
            .submit(move || {
                project_typed_json_from_shared_scalars(
                    &schema,
                    source_for_projection,
                    selected_for_projection.as_deref(),
                    maximum_projection_bytes,
                )
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(index_status)?;
        let assembly_seconds = assembly_started.elapsed().as_secs_f64();
        self.cache_output(
            plan.key,
            definition.schema_fingerprint,
            selected,
            &projected,
        )?;
        tracing::debug!(
            histogram.keldra_index_shared_projection_assembly_duration_seconds = assembly_seconds,
            "index assembler consumed shared source facts"
        );
        Ok(projected)
    }

    async fn project_definition(
        &self,
        definition: &CatalogDefinition,
        source: IndexSourceMutation,
        maximum_projection_bytes: usize,
    ) -> Result<(MergeMutation, IndexBuildDiagnostics), Status> {
        let payload = match &source {
            IndexSourceMutation::Upsert(object)
                if source_matches_schema(&definition.schema, object) =>
            {
                let reference = keldra_store::BlobRef {
                    hash: object.content_hash,
                    length: object.content_length,
                };
                let payload = self.inner.reader.open_blob_payload(&reference).await?;
                tracing::debug!(
                    monotonic_counter.keldra_index_projection_payload_fetches_total = 1_u64,
                    monotonic_counter.keldra_index_projection_payload_bytes_total =
                        object.content_length,
                    projection.cache = "union_bypass",
                    "exact source payload fetched for definition projection"
                );
                Some(payload)
            }
            _ => None,
        };
        let schema = definition.schema.clone();
        self.inner
            .cpu
            .submit(move || {
                let mut payload = payload;
                project_mutation(
                    &schema,
                    source,
                    payload
                        .as_mut()
                        .map(|reader| reader as &mut dyn std::io::Read),
                    maximum_projection_bytes,
                )
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(index_status)
    }

    fn plan(
        &self,
        definition: &CatalogDefinition,
        object: &IndexBuildObject,
    ) -> Result<ProjectionPlan, Status> {
        let matching_plans = self
            .inner
            .definitions
            .lock()
            .map_err(|_| Status::internal("shared projection definition lock is poisoned"))?
            .buckets
            .get(&(definition.tenant_id, definition.bucket_id))
            .map(|bucket| matching_selector_plans(bucket, object))
            .unwrap_or_default();
        let mut workspace_bytes = CACHE_ENTRY_FIXED_BYTES
            .checked_add(object.path.capacity())
            .and_then(|bytes| {
                bytes.checked_add(object.content_type.as_ref().map_or(0, String::capacity))
            })
            .ok_or_else(|| Status::resource_exhausted("shared projection plan size overflow"))?;
        let compiled = merge_selector_plans(matching_plans)
            .unwrap_or_else(|| compile_selector_plan(&definition.schema));
        workspace_bytes = workspace_bytes
            .checked_add(compiled.workspace_bytes)
            .ok_or_else(|| Status::resource_exhausted("shared projection plan size overflow"))?;
        if workspace_bytes >= self.inner.mapping_limit {
            return Err(Status::resource_exhausted(
                "shared projection selector union exceeds its mapping workspace",
            ));
        }
        Ok(ProjectionPlan {
            key: SourceProjectionKey {
                tenant_id: definition.tenant_id,
                bucket_id: definition.bucket_id,
                path: object.path.clone(),
                version: object.version,
                committed_at_unix_millis: object.committed_at_unix_millis,
                content_type: object.content_type.clone(),
                content_hash: object.content_hash,
                content_length: object.content_length,
                plan_hash: compiled.plan_hash,
            },
            pointers: compiled.pointers,
            workspace_bytes,
        })
    }

    async fn map_source(
        &self,
        plan: &ProjectionPlan,
        object: &IndexBuildObject,
    ) -> Result<Option<Arc<ProjectedScalarPointers>>, Status> {
        let map_started = Instant::now();
        let reference = keldra_store::BlobRef {
            hash: object.content_hash,
            length: object.content_length,
        };
        let mut payload = self.inner.reader.open_blob_payload(&reference).await?;
        tracing::debug!(
            monotonic_counter.keldra_index_projection_payload_fetches_total = 1_u64,
            monotonic_counter.keldra_index_projection_payload_bytes_total = object.content_length,
            projection.cache = "miss",
            "exact source payload fetched by shared projection mapper"
        );
        let pointers = plan.pointers.clone();
        let mapping_limit = self
            .inner
            .mapping_limit
            .checked_sub(plan.workspace_bytes)
            .ok_or_else(|| {
                Status::resource_exhausted(
                    "shared projection selector union exhausts its mapping workspace",
                )
            })?;
        let mapped = self
            .inner
            .cpu
            .submit(move || project_scalar_pointers(&mut payload, &pointers, mapping_limit))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(index_status)?
            .map(Arc::new);
        self.cache_selected(plan.key.clone(), mapped.clone())?;
        self.inner
            .telemetry
            .payload_parses
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            projection.cache = "miss",
            projection.selected_pointers = plan.pointers.len() as u64,
            histogram.keldra_index_shared_projection_map_duration_seconds =
                map_started.elapsed().as_secs_f64(),
            monotonic_counter.keldra_index_shared_projection_payload_parses_total = 1_u64,
            "index source payload mapped once for assigned definitions"
        );
        Ok(mapped)
    }

    fn cached_output(
        &self,
        key: &SourceProjectionKey,
        schema: [u8; 32],
        maximum_projection_bytes: usize,
    ) -> Result<Option<(MergeMutation, IndexBuildDiagnostics)>, Status> {
        let mut cache = self.cache()?;
        let tick = cache.tick();
        if !cache.touch(key, tick) {
            return Ok(None);
        }
        let entry = cache.entries.get(key).expect("touched cache entry exists");
        let Some(output) = entry.outputs.get(&schema) else {
            return Ok(None);
        };
        if output.resident_bytes > maximum_projection_bytes {
            return Ok(None);
        }
        tracing::debug!(
            projection.cache = "prepared_hit",
            monotonic_counter.keldra_index_shared_projection_prepared_hits_total = 1_u64,
            "index assembler reused one prepared source projection"
        );
        self.inner
            .telemetry
            .prepared_hits
            .fetch_add(1, Ordering::Relaxed);
        Ok(Some(((*output.mutation).clone(), output.diagnostics)))
    }

    fn cached_selected(
        &self,
        key: &SourceProjectionKey,
    ) -> Result<Option<Option<Arc<ProjectedScalarPointers>>>, Status> {
        let mut cache = self.cache()?;
        let tick = cache.tick();
        if !cache.touch(key, tick) {
            return Ok(None);
        }
        let entry = cache.entries.get(key).expect("touched cache entry exists");
        tracing::debug!(
            projection.cache = "selected_hit",
            monotonic_counter.keldra_index_shared_projection_selected_hits_total = 1_u64,
            "index assembler reused definition-neutral selected values"
        );
        self.inner
            .telemetry
            .selected_hits
            .fetch_add(1, Ordering::Relaxed);
        Ok(Some(entry.selected.clone()))
    }

    fn cache_selected(
        &self,
        key: SourceProjectionKey,
        selected: Option<Arc<ProjectedScalarPointers>>,
    ) -> Result<(), Status> {
        let selected_bytes = selected
            .as_deref()
            .map_or(Ok(1usize), ProjectedScalarPointers::resident_bytes)
            .map_err(index_status)?;
        let resident = CACHE_ENTRY_FIXED_BYTES
            .checked_add(key.path.capacity())
            .and_then(|bytes| {
                bytes.checked_add(key.content_type.as_ref().map_or(0, String::capacity))
            })
            .and_then(|bytes| bytes.checked_add(selected_bytes))
            .ok_or_else(|| Status::resource_exhausted("shared projection cache size overflow"))?;
        if resident > self.inner.cache_limit {
            return Ok(());
        }
        let mut cache = self.cache()?;
        let tick = cache.tick();
        if cache.entries.contains_key(&key) {
            return Ok(());
        }
        cache.used_bytes = cache.used_bytes.saturating_add(resident);
        cache.entries.insert(
            key.clone(),
            CachedProjection {
                selected,
                outputs: HashMap::new(),
                resident_bytes: resident,
                last_used: tick,
            },
        );
        cache.recency.insert((tick, key));
        cache.evict_to(self.inner.cache_limit);
        Ok(())
    }

    fn cache_output(
        &self,
        key: SourceProjectionKey,
        schema: [u8; 32],
        selected: Option<Arc<ProjectedScalarPointers>>,
        projected: &(MergeMutation, IndexBuildDiagnostics),
    ) -> Result<(), Status> {
        self.cache_selected(key.clone(), selected)?;
        let mutation_bytes =
            projected_mutation_resident_bytes(&projected.0).map_err(index_status)?;
        let resident = OUTPUT_ENTRY_FIXED_BYTES
            .checked_add(mutation_bytes)
            .ok_or_else(|| Status::resource_exhausted("prepared projection size overflow"))?;
        if resident > self.inner.cache_limit {
            return Ok(());
        }
        let mut cache = self.cache()?;
        let tick = cache.tick();
        if !cache.make_room(self.inner.cache_limit, resident, Some(&key)) {
            return Ok(());
        }
        if !cache.touch(&key, tick) {
            return Ok(());
        }
        let Some(entry) = cache.entries.get_mut(&key) else {
            return Ok(());
        };
        if entry.outputs.contains_key(&schema) {
            return Ok(());
        }
        entry.outputs.insert(
            schema,
            CachedOutput {
                mutation: Arc::new(projected.0.clone()),
                diagnostics: projected.1,
                resident_bytes: mutation_bytes,
            },
        );
        entry.resident_bytes = entry.resident_bytes.saturating_add(resident);
        cache.used_bytes = cache.used_bytes.saturating_add(resident);
        cache.evict_to(self.inner.cache_limit);
        Ok(())
    }

    fn cache(&self) -> Result<std::sync::MutexGuard<'_, ProjectionCache>, Status> {
        self.inner
            .cache
            .lock()
            .map_err(|_| Status::internal("shared projection cache lock is poisoned"))
    }

    fn observe_request(&self) {
        let requests = self
            .inner
            .telemetry
            .requests
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if requests % TELEMETRY_REPORT_INTERVAL != 0 {
            return;
        }
        let (cache_entries, cache_bytes) = self
            .inner
            .cache
            .lock()
            .map(|cache| (cache.entries.len() as u64, cache.used_bytes as u64))
            .unwrap_or((0, 0));
        let (logical_definitions, projection_families, membership_recipes, field_recipes) = self
            .inner
            .definitions
            .lock()
            .map(|plans| {
                (
                    plans.definitions.len() as u64,
                    plans.families.len() as u64,
                    plans.membership_recipes.len() as u64,
                    plans.field_recipes.len() as u64,
                )
            })
            .unwrap_or((0, 0, 0, 0));
        tracing::info!(
            projection.requests = requests,
            projection.payload_parses = self.inner.telemetry.payload_parses.load(Ordering::Relaxed),
            projection.selected_hits = self.inner.telemetry.selected_hits.load(Ordering::Relaxed),
            projection.prepared_hits = self.inner.telemetry.prepared_hits.load(Ordering::Relaxed),
            projection.union_bypasses = self.inner.telemetry.union_bypasses.load(Ordering::Relaxed),
            projection.cache_entries = cache_entries,
            projection.cache_bytes = cache_bytes,
            projection.logical_definitions = logical_definitions,
            projection.families = projection_families,
            projection.membership_recipes = membership_recipes,
            projection.field_recipes = field_recipes,
            "shared index projection cumulative summary"
        );
    }
}

fn compile_family_schema(family: &RegisteredFamily) -> Result<Schema, Status> {
    let mut schema = family.template.clone();
    schema.fields = family
        .fields
        .values()
        .map(|registered| registered.field.clone())
        .collect();
    schema.canonicalize_physical_fields().map_err(index_status)
}

impl ProjectionCache {
    fn tick(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }

    fn touch(&mut self, key: &SourceProjectionKey, tick: u64) -> bool {
        let Some(entry) = self.entries.get_mut(key) else {
            return false;
        };
        self.recency.remove(&(entry.last_used, key.clone()));
        entry.last_used = tick;
        self.recency.insert((tick, key.clone()));
        true
    }

    fn evict_to(&mut self, limit: usize) {
        while self.used_bytes > limit {
            let Some((tick, oldest)) = self.recency.iter().next().cloned() else {
                self.used_bytes = 0;
                return;
            };
            self.recency.remove(&(tick, oldest.clone()));
            if let Some(removed) = self.entries.remove(&oldest) {
                self.used_bytes = self.used_bytes.saturating_sub(removed.resident_bytes);
            }
        }
    }

    fn make_room(
        &mut self,
        limit: usize,
        additional: usize,
        preserve: Option<&SourceProjectionKey>,
    ) -> bool {
        while self.used_bytes.saturating_add(additional) > limit {
            let Some((tick, oldest)) = self
                .recency
                .iter()
                .find(|(_, key)| preserve != Some(key))
                .cloned()
            else {
                return false;
            };
            self.recency.remove(&(tick, oldest.clone()));
            if let Some(removed) = self.entries.remove(&oldest) {
                self.used_bytes = self.used_bytes.saturating_sub(removed.resident_bytes);
            }
        }
        true
    }
}

fn cache_stripe(key: &SourceProjectionKey) -> usize {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hash);
    usize::try_from(hash.finish()).unwrap_or(0) % MAPPER_STRIPES
}

fn index_status(error: keldra_index::IndexError) -> Status {
    match error {
        keldra_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        keldra_index::IndexError::Io(_) => Status::unavailable(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use keldra_api::v1::index_field;
    use keldra_api::v1::index_specification::Specification;
    use keldra_api::v1::{
        CreateIndexRequest, IndexField, IndexFieldCapability, IndexFieldCardinality,
        IndexSpecification, KeywordIndexField, TypedJsonIndexSpec,
    };

    use super::*;
    use crate::index_service::StoredIndexDefinition;

    fn key(ordinal: u64) -> SourceProjectionKey {
        SourceProjectionKey {
            tenant_id: 1,
            bucket_id: 2,
            path: format!("records/{ordinal}.json"),
            version: ordinal,
            committed_at_unix_millis: ordinal,
            content_type: Some("application/json".into()),
            content_hash: [ordinal as u8; 32],
            content_length: 72 * 1024,
            plan_hash: [0x55; 32],
        }
    }

    fn entry(bytes: usize, last_used: u64) -> CachedProjection {
        CachedProjection {
            selected: None,
            outputs: HashMap::new(),
            resident_bytes: bytes,
            last_used,
        }
    }

    #[test]
    fn cache_room_evicts_oldest_without_dropping_the_source_being_assembled() {
        let retained = key(1);
        let oldest = key(2);
        let newest = key(3);
        let mut cache = ProjectionCache {
            entries: HashMap::from([
                (retained.clone(), entry(30, 1)),
                (oldest.clone(), entry(30, 2)),
                (newest.clone(), entry(30, 3)),
            ]),
            recency: BTreeSet::from([
                (1, retained.clone()),
                (2, oldest.clone()),
                (3, newest.clone()),
            ]),
            used_bytes: 90,
            clock: 3,
        };

        assert!(cache.make_room(100, 35, Some(&retained)));
        assert!(cache.entries.contains_key(&retained));
        assert!(!cache.entries.contains_key(&oldest));
        assert!(cache.entries.contains_key(&newest));
        assert_eq!(cache.used_bytes, 60);
    }

    #[test]
    fn exact_source_and_plan_identity_are_part_of_the_cache_key() {
        let original = key(7);
        let mut changed_bytes = original.clone();
        changed_bytes.content_hash = [0xaa; 32];
        let mut changed_plan = original.clone();
        changed_plan.plan_hash = [0xbb; 32];

        assert_ne!(original, changed_bytes);
        assert_ne!(original, changed_plan);
    }

    fn registered(
        path_prefix: &str,
        content_type: Option<&str>,
        selectors: &[&str],
    ) -> RegisteredDefinition {
        let selectors = selectors
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        RegisteredDefinition {
            tenant_id: 1,
            bucket_id: 2,
            route: ProjectionRoute {
                path_prefix: path_prefix.into(),
                content_type: content_type.map(str::to_owned),
            },
            membership: PhysicalRecipeIdentity {
                tenant_id: 1,
                bucket_id: 2,
                fingerprint: *blake3::hash(
                    format!("{path_prefix}\0{}", content_type.unwrap_or_default()).as_bytes(),
                )
                .as_bytes(),
            },
            recipes: selectors
                .iter()
                .map(|selector| {
                    (
                        PhysicalRecipeIdentity {
                            tenant_id: 1,
                            bucket_id: 2,
                            fingerprint: *blake3::hash(selector.as_bytes()).as_bytes(),
                        },
                        selector.clone(),
                    )
                })
                .collect(),
            selectors,
        }
    }

    fn object(path: &str, content_type: Option<&str>) -> IndexBuildObject {
        IndexBuildObject {
            path: path.into(),
            version: 1,
            committed_at_unix_millis: 2,
            content_type: content_type.map(str::to_owned),
            content_hash: [3; 32],
            content_length: 4,
        }
    }

    fn family_definition(index_id: u64, name: &str, pointer: &str) -> CatalogDefinition {
        let field = IndexField {
            name: name.into(),
            json_pointer: pointer.into(),
            cardinality: IndexFieldCardinality::Single as i32,
            capabilities: vec![IndexFieldCapability::Exact as i32],
            field_type: Some(index_field::FieldType::Keyword(KeywordIndexField {})),
        };
        let stored = StoredIndexDefinition::create(
            "tenant".into(),
            CreateIndexRequest {
                bucket: "bucket".into(),
                name: format!("family-{index_id}"),
                path_prefix: "records/".into(),
                content_type: "application/json".into(),
                specification: Some(IndexSpecification {
                    specification: Some(Specification::TypedJson(TypedJsonIndexSpec {
                        fields: vec![field],
                        physical_order: Vec::new(),
                    })),
                }),
                command_id: format!("family-{index_id}"),
            },
            index_id,
        )
        .unwrap();
        CatalogDefinition::new(1, 2, 1, stored).unwrap()
    }

    #[test]
    fn catalog_updates_compile_direct_scope_and_content_routes() {
        let mut plans = DefinitionPlans::default();
        let global = registered("", None, &["/id"]);
        let records = registered("records/", Some("application/json"), &["/class"]);
        let wrong_scope = registered("other/", None, &["/never"]);
        let wrong_type = registered("records/", Some("text/plain"), &["/also_never"]);
        for definition in [&global, &records, &wrong_scope, &wrong_type] {
            add_registered_definition(&mut plans, definition).unwrap();
        }

        let bucket = &plans.buckets[&(1, 2)];
        let merged = merge_selector_plans(matching_selector_plans(
            bucket,
            &object("records/a.json", Some("application/json")),
        ))
        .unwrap();

        assert_eq!(merged.pointers.as_ref(), &["/class", "/id"]);
    }

    #[test]
    fn equivalent_definitions_reference_one_compiled_selector() {
        let mut plans = DefinitionPlans::default();
        let first = registered("records/", None, &["/id"]);
        let second = registered("records/", None, &["/id"]);
        add_registered_definition(&mut plans, &first).unwrap();
        add_registered_definition(&mut plans, &second).unwrap();
        let route = &plans.buckets[&(1, 2)].routes[&first.route];
        assert_eq!(route.selector_references["/id"], 2);
        assert_eq!(route.compiled.as_ref().unwrap().pointers.len(), 1);
        assert_eq!(plans.membership_recipes.len(), 1);
        assert_eq!(
            plans.membership_recipes.values().next().unwrap().references,
            2
        );
        assert_eq!(plans.field_recipes.len(), 1);
        assert_eq!(plans.field_recipes.values().next().unwrap().references, 2);

        remove_registered_definition(&mut plans, first).unwrap();
        assert_eq!(
            plans.buckets[&(1, 2)].routes[&second.route].selector_references["/id"],
            1
        );
        remove_registered_definition(&mut plans, second).unwrap();
        assert!(!plans.buckets.contains_key(&(1, 2)));
        assert!(plans.membership_recipes.is_empty());
        assert!(plans.field_recipes.is_empty());
    }

    #[test]
    fn duplicate_logical_bindings_release_one_shared_recipe_exactly() {
        let mut plans = DefinitionPlans::default();
        let definition = registered("records/", None, &["/id", "/id"]);
        add_registered_definition(&mut plans, &definition).unwrap();
        assert_eq!(plans.field_recipes.len(), 1);
        assert_eq!(plans.field_recipes.values().next().unwrap().references, 2);
        assert_eq!(
            plans.buckets[&(1, 2)].routes[&definition.route].selector_references["/id"],
            2
        );
        remove_registered_definition(&mut plans, definition).unwrap();
        assert!(plans.field_recipes.is_empty());
        assert!(plans.buckets.is_empty());
    }

    #[test]
    fn family_catalog_grows_with_distinct_recipes_not_logical_aliases() {
        let first = family_definition(1, "state", "/state");
        let alias = family_definition(2, "renamed", "/state");
        let additional = family_definition(3, "priority", "/priority");
        let identity = first.projection_family_identity();
        assert_eq!(identity, alias.projection_family_identity());
        assert_eq!(identity, additional.projection_family_identity());

        let mut plans = DefinitionPlans::default();
        for definition in [&first, &alias, &additional] {
            add_family_definition(&mut plans, definition).unwrap();
        }
        let family = &plans.families[&identity];
        assert_eq!(family.definitions, 3);
        assert_eq!(family.fields.len(), 2);
        let schema = compile_family_schema(family).unwrap();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(
            schema
                .recipe_fingerprints()
                .unwrap()
                .fields
                .into_iter()
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
    }
}
