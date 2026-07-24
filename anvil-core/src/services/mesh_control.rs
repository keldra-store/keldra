use crate::anvil_api::mesh_control_service_server::MeshControlService;
use crate::anvil_api::*;
use crate::mesh_lifecycle::{
    BootstrapMeshLifecycleProjection, CellDescriptor, CreateRegionDescriptor,
    LifecycleState as CoreLifecycleState, NodeCapability as CoreNodeCapability, NodeDescriptor,
    NodeDrainDescriptor, RegionDescriptor, RegisterCellDescriptor, RegisterNodeDescriptor,
    capacity_json_hash,
};
use crate::system_realm::SystemAdminRelation;
use crate::{AppState, access_control, auth, bucket_journal, middleware};
use tonic::{Request, Response, Status};

const MAX_BOOTSTRAP_REGIONS: usize = 256;
const MAX_BOOTSTRAP_CELLS: usize = 1024;
const MAX_BOOTSTRAP_NODES: usize = 4096;
const MAX_BOOTSTRAP_COREMETA_ROWS: usize = 4096;

fn mesh_transaction_id(options: Option<&WriteOptions>) -> Result<Option<&str>, Status> {
    crate::services::saga_reserved::write_options_transaction_id(options)
}

#[tonic::async_trait]
impl MeshControlService for AppState {
    async fn bootstrap_mesh_topology(
        &self,
        request: Request<BootstrapMeshTopologyRequest>,
    ) -> Result<Response<BootstrapMeshTopologyResponse>, Status> {
        require_mesh_admin(&request, self, SystemAdminRelation::ManageNodes).await?;
        let request_id = request_id(&request);
        let req = request.into_inner();
        validate_bootstrap_topology_size(&req)?;
        if req.regions.is_empty() || req.cells.is_empty() || req.nodes.is_empty() {
            return Err(Status::invalid_argument(
                "bootstrap topology requires regions, cells, and nodes",
            ));
        }
        if mesh_topology_exists(self).await? {
            return Ok(Response::new(BootstrapMeshTopologyResponse {
                write: Some(mesh_write_response(
                    request_id,
                    "mesh-topology-already-initialised".to_string(),
                    None,
                )),
                canonical_coremeta_rows: Vec::new(),
                already_initialised: true,
            }));
        }

        let regions = req
            .regions
            .into_iter()
            .map(|region| CreateRegionDescriptor {
                mesh_id: self.config.mesh_id.clone(),
                region: region.region_id.clone(),
                public_base_url: region.endpoint,
                virtual_host_suffix: format!("{}.anvil-storage.com", region.region_id),
                placement_weight: 100,
                default_cell: None,
            })
            .collect::<Vec<_>>();
        let cells = req
            .cells
            .into_iter()
            .map(|cell| RegisterCellDescriptor {
                mesh_id: self.config.mesh_id.clone(),
                region: cell.region_id,
                cell_id: cell.cell_id,
                placement_weight: 100,
                failure_domain: cell.failure_domain,
            })
            .collect::<Vec<_>>();
        let nodes = req
            .nodes
            .into_iter()
            .map(|node| {
                let capabilities = parse_node_capabilities(&node.capabilities)?;
                Ok(RegisterNodeDescriptor {
                    mesh_id: self.config.mesh_id.clone(),
                    node_id: node.node_id,
                    region: node.region_id,
                    cell_id: node.cell_id,
                    receipt_signing_public_key: node.receipt_signing_public_key,
                    public_api_addr: node.advertise_addr,
                    capabilities,
                    capacity_json: node.capacity_json,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let canonical_coremeta_rows = if req.canonical_coremeta_rows.is_empty() {
            for node in &nodes {
                self.core_store
                    .register_node_receipt_signing_public_key(
                        &node.node_id,
                        &node.receipt_signing_public_key,
                    )
                    .map_err(mesh_status)?;
            }
            crate::mesh_lifecycle::install_bootstrap_lifecycle_projection(
                &self.storage,
                &self.core_store,
                BootstrapMeshLifecycleProjection {
                    regions: regions.clone(),
                    cells: cells.clone(),
                    nodes: nodes.clone(),
                },
            )
            .map_err(mesh_status)?;
            let exported_rows = self
                .core_store
                .export_portable_coremeta_bootstrap_rows(MAX_BOOTSTRAP_COREMETA_ROWS)
                .map_err(mesh_status)?;
            encode_bootstrap_snapshot_rows(exported_rows)
        } else {
            let rows = decode_bootstrap_snapshot_rows(req.canonical_coremeta_rows)?;
            self.core_store
                .install_portable_coremeta_bootstrap_rows(&rows)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Vec::new()
        };
        ensure_bootstrap_topology_matches(self, &regions, &cells, &nodes).await?;

        Ok(Response::new(BootstrapMeshTopologyResponse {
            write: Some(mesh_write_response(
                request_id,
                "mesh-topology-bootstrap".to_string(),
                None,
            )),
            canonical_coremeta_rows,
            already_initialised: false,
        }))
    }

    async fn put_region(
        &self,
        request: Request<PutRegionRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        require_mesh_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let claims = admin_claims(&request)?;
        let request_id = request_id(&request);
        let req = request.into_inner();
        let transaction_id = mesh_transaction_id(req.options.as_ref())?;
        let mutation_id = if let Some(transaction_id) = transaction_id {
            put_region_in_transaction(self, &claims, &req, transaction_id).await?
        } else {
            let mut created = false;
            if let Some(region) = find_region(self, &req.region_id).await? {
                ensure_region_put_matches(&region, &req)?;
            } else {
                self.persistence
                    .create_region_descriptor(CreateRegionDescriptor {
                        mesh_id: self.config.mesh_id.clone(),
                        region: req.region_id.clone(),
                        public_base_url: req.endpoint,
                        virtual_host_suffix: format!("{}.anvil-storage.com", req.region_id),
                        placement_weight: 1,
                        default_cell: None,
                    })
                    .await
                    .map_err(mesh_status)?;
                created = true;
            }
            apply_region_target_state(self, &req.region_id, req.state.as_str()).await?;
            if created {
                crate::access_control::grant_region_defaults(
                    &self.persistence,
                    &req.region_id,
                    &claims.sub,
                    "mesh control region creation",
                )
                .await
                .map_err(mesh_status)?;
            }
            req.region_id
        };
        Ok(Response::new(mesh_write_response(
            request_id,
            mutation_id,
            transaction_id,
        )))
    }

    async fn put_cell(
        &self,
        request: Request<PutCellRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        require_mesh_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let claims = admin_claims(&request)?;
        let request_id = request_id(&request);
        let req = request.into_inner();
        let transaction_id = mesh_transaction_id(req.options.as_ref())?;
        let mutation_id = if let Some(transaction_id) = transaction_id {
            put_cell_in_transaction(self, &claims, &req, transaction_id).await?
        } else {
            let mut created = false;
            if let Some(cell) = find_cell(self, &req.region_id, &req.cell_id).await? {
                ensure_cell_put_matches(&cell, &req)?;
            } else {
                self.persistence
                    .register_cell_descriptor(RegisterCellDescriptor {
                        mesh_id: self.config.mesh_id.clone(),
                        region: req.region_id.clone(),
                        cell_id: req.cell_id.clone(),
                        placement_weight: 1,
                        failure_domain: req.failure_domain.clone(),
                    })
                    .await
                    .map_err(mesh_status)?;
                created = true;
            }
            apply_cell_target_state(self, &req.region_id, &req.cell_id, req.state.as_str()).await?;
            if created {
                crate::access_control::grant_cell_defaults(
                    &self.persistence,
                    &req.region_id,
                    &req.cell_id,
                    &claims.sub,
                    "mesh control cell creation",
                )
                .await
                .map_err(mesh_status)?;
            }
            req.cell_id
        };
        Ok(Response::new(mesh_write_response(
            request_id,
            mutation_id,
            transaction_id,
        )))
    }

    async fn put_node(
        &self,
        request: Request<PutNodeRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        require_mesh_admin(&request, self, SystemAdminRelation::ManageNodes).await?;
        let claims = admin_claims(&request)?;
        let request_id = request_id(&request);
        let req = request.into_inner();
        let transaction_id = mesh_transaction_id(req.options.as_ref())?;
        let capabilities = parse_node_capabilities(&req.capabilities)?;
        let requested_state = req.state.clone();
        let mutation_id = if let Some(transaction_id) = transaction_id {
            put_node_in_transaction(self, &claims, &req, capabilities, transaction_id).await?
        } else {
            let mut created = false;
            if let Some(node) = find_node(self, &req.node_id).await? {
                ensure_node_put_matches(&node, &req, &capabilities)?;
            } else {
                self.persistence
                    .register_node_descriptor(RegisterNodeDescriptor {
                        mesh_id: self.config.mesh_id.clone(),
                        node_id: req.node_id.clone(),
                        region: req.region_id.clone(),
                        cell_id: req.cell_id.clone(),
                        receipt_signing_public_key: req.receipt_signing_public_key.clone(),
                        public_api_addr: req.advertise_addr.clone(),
                        capabilities,
                        capacity_json: req.capacity_json.clone(),
                    })
                    .await
                    .map_err(mesh_status)?;
                created = true;
            }
            apply_node_target_state(self, &req.node_id, requested_state.as_str()).await?;
            if created {
                crate::access_control::grant_node_defaults(
                    &self.persistence,
                    &req.region_id,
                    &req.cell_id,
                    &req.node_id,
                    &claims.sub,
                    "mesh control node creation",
                )
                .await
                .map_err(mesh_status)?;
                crate::access_control::grant_internal_node_system_access(
                    &self.persistence,
                    &req.node_id,
                    &claims.sub,
                    "mesh control node creation",
                )
                .await
                .map_err(mesh_status)?;
            }
            req.node_id
        };
        Ok(Response::new(mesh_write_response(
            request_id,
            mutation_id,
            transaction_id,
        )))
    }

    async fn drain_node(
        &self,
        request: Request<DrainNodeRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        require_mesh_admin(&request, self, SystemAdminRelation::ManageNodes).await?;
        let request_id = request_id(&request);
        let req = request.into_inner();
        let expected_generation = if let Some(generation) = req
            .context
            .as_ref()
            .map(|ctx| ctx.expected_generation)
            .filter(|generation| *generation != 0)
        {
            generation
        } else {
            current_node_generation(self, &req.node_id).await?
        };
        self.persistence
            .transition_node_descriptor(
                &req.node_id,
                expected_generation,
                CoreLifecycleState::Draining,
                Some(NodeDrainDescriptor {
                    started_at: chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    graceful_timeout_ms: req.graceful_timeout_ms,
                    force_after_timeout: req.force_after_timeout,
                }),
            )
            .await
            .map_err(mesh_status)?;
        Ok(Response::new(mesh_write_response(
            request_id,
            req.node_id,
            None,
        )))
    }

    async fn drain_cell(
        &self,
        request: Request<DrainCellRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        require_mesh_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let request_id = request_id(&request);
        let req = request.into_inner();
        let expected_generation = if let Some(generation) = req
            .context
            .as_ref()
            .map(|ctx| ctx.expected_generation)
            .filter(|generation| *generation != 0)
        {
            generation
        } else {
            current_cell_generation(self, &req.region, &req.cell_id).await?
        };
        self.persistence
            .transition_cell_descriptor(
                &req.region,
                &req.cell_id,
                expected_generation,
                CoreLifecycleState::Draining,
            )
            .await
            .map_err(mesh_status)?;
        Ok(Response::new(mesh_write_response(
            request_id,
            req.cell_id,
            None,
        )))
    }

    async fn move_bucket(
        &self,
        request: Request<MoveBucketRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        require_mesh_admin(&request, self, SystemAdminRelation::ManageRouting).await?;
        let request_id = request_id(&request);
        let claims = admin_claims(&request)?;
        let req = request.into_inner();
        let tenant_id = resolve_mesh_tenant_id(self, &req.tenant_id).await?;
        require_mesh_bucket_manage(self, &claims, tenant_id, &req.bucket_name).await?;
        let transaction_id = mesh_transaction_id(req.options.as_ref())?;
        let mutation_id = if let Some(transaction_id) = transaction_id {
            move_bucket_in_transaction(self, &claims, tenant_id, &req, transaction_id).await?
        } else {
            let bucket = self
                .persistence
                .move_bucket_home_region(tenant_id, &req.bucket_name, &req.target_region_id)
                .await
                .map_err(mesh_status)?;
            format!("bucket:{}:region:{}", bucket.name, bucket.region)
        };
        Ok(Response::new(mesh_write_response(
            request_id,
            mutation_id,
            transaction_id,
        )))
    }

    async fn get_partition_map(
        &self,
        request: Request<GetPartitionMapRequest>,
    ) -> Result<Response<PartitionMap>, Status> {
        require_mesh_admin(&request, self, SystemAdminRelation::ViewSystem).await?;
        let claims = admin_claims(&request)?;
        let req = request.into_inner();
        let mut rows = Vec::new();
        rows.extend(
            self.persistence
                .list_region_descriptors()
                .await
                .map_err(mesh_status)?
                .into_iter()
                .map(|region| {
                    format!(
                        "region:{}:{:?}:{}",
                        region.region, region.state, region.generation
                    )
                }),
        );
        rows.extend(
            self.persistence
                .list_cell_descriptors(None)
                .await
                .map_err(mesh_status)?
                .into_iter()
                .map(|cell| {
                    format!(
                        "cell:{}/{}:{:?}:{}",
                        cell.region, cell.cell_id, cell.state, cell.generation
                    )
                }),
        );
        rows.extend(
            self.persistence
                .list_node_descriptors(None, None)
                .await
                .map_err(mesh_status)?
                .into_iter()
                .map(|node| {
                    format!(
                        "node:{}/{}/{}:{:?}:{}",
                        node.region, node.cell_id, node.node_id, node.state, node.generation
                    )
                }),
        );
        if !req.scope.is_empty() {
            rows.retain(|row| row.contains(&req.scope));
        }
        let epoch = crate::services::collection_cursor::collection_revision(
            rows.iter().map(|row| (row.as_str(), 0)),
        );
        let filters = [("scope", req.scope.as_str())];
        let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
        let (rows, page) = crate::services::collection_cursor::paginate(
            rows,
            req.page.as_ref(),
            "anvil.MeshControlService/GetPartitionMap",
            &filters,
            &principal_scope,
            "partition_row.asc",
            self.config.jwt_secret.as_bytes(),
            String::as_str,
            |_| 0,
        )?;
        Ok(Response::new(PartitionMap {
            epoch: crate::services::collection_cursor::content_generation(&[epoch.as_bytes()]),
            partition_rows: rows,
            page: Some(page),
        }))
    }
}

async fn require_mesh_admin<T>(
    request: &Request<T>,
    state: &AppState,
    relation: SystemAdminRelation,
) -> Result<(), Status> {
    let claims = admin_claims(request)?;
    let allowed = crate::system_realm::check_admin_relation(
        &state.storage,
        &state.config.mesh_id,
        &claims,
        relation,
    )
    .await
    .map_err(|error| Status::internal(error.to_string()))?;
    if allowed {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "Missing system realm admin relation {}",
            relation.as_str()
        )))
    }
}

async fn resolve_mesh_tenant_id(state: &AppState, tenant_ref: &str) -> Result<i64, Status> {
    let tenant_ref = tenant_ref.trim();
    if tenant_ref.is_empty() {
        return Err(Status::invalid_argument("tenant_id is required"));
    }
    if let Ok(tenant_id) = tenant_ref.parse::<i64>() {
        return Ok(tenant_id);
    }
    state
        .persistence
        .get_tenant_by_name(tenant_ref)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .map(|tenant| tenant.id)
        .ok_or_else(|| Status::not_found("Tenant not found"))
}

async fn require_mesh_bucket_manage(
    state: &AppState,
    claims: &auth::Claims,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<(), Status> {
    let bucket = bucket_journal::read_current_bucket(&state.storage, tenant_id, bucket_name)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("bucket not found"))?;
    access_control::require_system_realm_permission(
        &state.storage,
        claims,
        crate::system_realm::SYSTEM_BUCKET_NAMESPACE,
        &access_control::bucket_object_id(&bucket),
        "manage_bucket",
    )
    .await
}

fn admin_claims<T>(request: &Request<T>) -> Result<auth::Claims, Status> {
    request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing authenticated admin principal"))
}

async fn find_region(
    state: &AppState,
    region_id: &str,
) -> Result<Option<RegionDescriptor>, Status> {
    Ok(state
        .persistence
        .list_region_descriptors()
        .await
        .map_err(mesh_status)?
        .into_iter()
        .find(|region| region.region == region_id))
}

async fn find_cell(
    state: &AppState,
    region_id: &str,
    cell_id: &str,
) -> Result<Option<CellDescriptor>, Status> {
    Ok(state
        .persistence
        .list_cell_descriptors(Some(region_id))
        .await
        .map_err(mesh_status)?
        .into_iter()
        .find(|cell| cell.region == region_id && cell.cell_id == cell_id))
}

async fn find_node(state: &AppState, node_id: &str) -> Result<Option<NodeDescriptor>, Status> {
    Ok(state
        .persistence
        .list_node_descriptors(None, None)
        .await
        .map_err(mesh_status)?
        .into_iter()
        .find(|node| node.node_id == node_id))
}

async fn put_region_in_transaction(
    state: &AppState,
    claims: &auth::Claims,
    req: &PutRegionRequest,
    transaction_id: &str,
) -> Result<String, Status> {
    let target = parse_lifecycle_state(req.state.as_str())?;
    let principal = mesh_transaction_principal(claims);
    crate::mesh_lifecycle::put_region_in_transaction(
        &state.storage,
        CreateRegionDescriptor {
            mesh_id: state.config.mesh_id.clone(),
            region: req.region_id.clone(),
            public_base_url: req.endpoint.clone(),
            virtual_host_suffix: format!("{}.anvil-storage.com", req.region_id),
            placement_weight: 1,
            default_cell: None,
        },
        target,
        transaction_id,
        &principal,
    )
    .await
    .map_err(mesh_status)?;
    Ok(req.region_id.clone())
}

async fn put_cell_in_transaction(
    state: &AppState,
    claims: &auth::Claims,
    req: &PutCellRequest,
    transaction_id: &str,
) -> Result<String, Status> {
    let target = parse_lifecycle_state(req.state.as_str())?;
    let principal = mesh_transaction_principal(claims);
    crate::mesh_lifecycle::put_cell_in_transaction(
        &state.storage,
        RegisterCellDescriptor {
            mesh_id: state.config.mesh_id.clone(),
            region: req.region_id.clone(),
            cell_id: req.cell_id.clone(),
            placement_weight: 1,
            failure_domain: req.failure_domain.clone(),
        },
        target,
        transaction_id,
        &principal,
    )
    .await
    .map_err(mesh_status)?;
    Ok(req.cell_id.clone())
}

async fn put_node_in_transaction(
    state: &AppState,
    claims: &auth::Claims,
    req: &PutNodeRequest,
    capabilities: Vec<CoreNodeCapability>,
    transaction_id: &str,
) -> Result<String, Status> {
    let target = parse_lifecycle_state(req.state.as_str())?;
    let principal = mesh_transaction_principal(claims);
    crate::mesh_lifecycle::put_node_in_transaction(
        &state.storage,
        RegisterNodeDescriptor {
            mesh_id: state.config.mesh_id.clone(),
            node_id: req.node_id.clone(),
            region: req.region_id.clone(),
            cell_id: req.cell_id.clone(),
            receipt_signing_public_key: req.receipt_signing_public_key.clone(),
            public_api_addr: req.advertise_addr.clone(),
            capabilities,
            capacity_json: req.capacity_json.clone(),
        },
        target,
        transaction_id,
        &principal,
    )
    .await
    .map_err(mesh_status)?;
    Ok(req.node_id.clone())
}

async fn move_bucket_in_transaction(
    state: &AppState,
    claims: &auth::Claims,
    tenant_id: i64,
    req: &MoveBucketRequest,
    transaction_id: &str,
) -> Result<String, Status> {
    let mut bucket =
        bucket_journal::read_current_bucket(&state.storage, tenant_id, &req.bucket_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::not_found("bucket not found"))?;
    if bucket.region == req.target_region_id {
        return Ok(format!("bucket:{}:region:{}", bucket.name, bucket.region));
    }
    crate::mesh_lifecycle::ensure_region_accepts_new_writes(&state.storage, &req.target_region_id)
        .await
        .map_err(mesh_status)?;
    let target_cell =
        crate::mesh_lifecycle::list_cells(&state.storage, Some(&req.target_region_id))
            .await
            .map_err(mesh_status)?
            .into_iter()
            .filter(|cell| cell.state == CoreLifecycleState::Active)
            .min_by(|left, right| left.cell_id.cmp(&right.cell_id))
            .ok_or_else(|| Status::failed_precondition("target region has no active cell"))?;
    let tenant =
        crate::mesh_directory::TenantId::new(tenant_id.to_string()).map_err(mesh_status)?;
    let name =
        crate::mesh_directory::BucketName::canonicalize(&req.bucket_name).map_err(mesh_status)?;
    let key = crate::mesh_directory::BucketLocatorKey::new(tenant, name);
    let existing = crate::mesh_directory::read_bucket_locator(&state.storage, &key)
        .await
        .map_err(mesh_status)?
        .ok_or_else(|| Status::not_found("bucket locator not found"))?;
    let principal = mesh_transaction_principal(claims);
    let mut moved = existing.clone();
    moved.home_region = crate::mesh_directory::RegionName::new(req.target_region_id.clone())
        .map_err(mesh_status)?;
    moved.home_cell =
        crate::mesh_directory::CellId::new(target_cell.cell_id.clone()).map_err(mesh_status)?;
    moved.status = crate::mesh_directory::BucketLocatorStatus::Active;
    moved.updated_at = chrono::Utc::now().to_rfc3339();
    moved.generation = existing.generation.saturating_add(1);
    bucket.region = req.target_region_id.clone();
    bucket_journal::stage_bucket_mutation_in_transaction(
        &state.storage,
        &bucket,
        bucket_journal::BucketJournalMutation::Update,
        transaction_id,
        &principal,
    )
    .await
    .map_err(mesh_status)?;
    crate::mesh_directory::write_bucket_locator_in_transaction(
        &state.storage,
        &moved,
        false,
        transaction_id,
        &principal,
    )
    .await
    .map_err(mesh_status)?;
    Ok(format!("bucket:{}:region:{}", bucket.name, bucket.region))
}

fn mesh_transaction_principal(claims: &auth::Claims) -> String {
    crate::object_manager::transaction_principal_from_claims(claims)
}

fn ensure_region_put_matches(
    region: &RegionDescriptor,
    req: &PutRegionRequest,
) -> Result<(), Status> {
    if !req.endpoint.is_empty() && region.public_base_url != req.endpoint {
        return Err(Status::failed_precondition(format!(
            "region {} already exists with endpoint {}",
            region.region, region.public_base_url
        )));
    }
    Ok(())
}

fn ensure_cell_put_matches(cell: &CellDescriptor, req: &PutCellRequest) -> Result<(), Status> {
    if cell.failure_domain != req.failure_domain {
        return Err(Status::failed_precondition(format!(
            "cell {}/{} already exists with failure domain {}",
            cell.region, cell.cell_id, cell.failure_domain
        )));
    }
    Ok(())
}

fn ensure_node_put_matches(
    node: &NodeDescriptor,
    req: &PutNodeRequest,
    capabilities: &[CoreNodeCapability],
) -> Result<(), Status> {
    let capacity_hash = capacity_json_hash(&req.capacity_json).map_err(mesh_status)?;
    if node.region != req.region_id
        || node.cell_id != req.cell_id
        || node.receipt_signing_public_key != req.receipt_signing_public_key
        || node.public_api_addr != req.advertise_addr
        || node.capabilities != capabilities
        || node.capacity_json_hash != capacity_hash
    {
        return Err(Status::failed_precondition(format!(
            "node {} already exists with different immutable descriptor fields",
            node.node_id
        )));
    }
    Ok(())
}

async fn current_node_generation(state: &AppState, node_id: &str) -> Result<u64, Status> {
    state
        .persistence
        .list_node_descriptors(None, None)
        .await
        .map_err(mesh_status)?
        .into_iter()
        .find(|node| node.node_id == node_id)
        .map(|node| node.generation)
        .ok_or_else(|| Status::not_found("node not found"))
}

async fn current_cell_generation(
    state: &AppState,
    region: &str,
    cell_id: &str,
) -> Result<u64, Status> {
    state
        .persistence
        .list_cell_descriptors(Some(region))
        .await
        .map_err(mesh_status)?
        .into_iter()
        .find(|cell| cell.region == region && cell.cell_id == cell_id)
        .map(|cell| cell.generation)
        .ok_or_else(|| Status::not_found("cell not found"))
}

async fn apply_region_target_state(
    state: &AppState,
    region_id: &str,
    requested_state: &str,
) -> Result<(), Status> {
    let Some(target) = parse_lifecycle_state(requested_state)? else {
        return Ok(());
    };
    let current = state
        .persistence
        .list_region_descriptors()
        .await
        .map_err(mesh_status)?
        .into_iter()
        .find(|region| region.region == region_id)
        .ok_or_else(|| Status::not_found("region not found after registration"))?;
    if current.state == target {
        return Ok(());
    }
    state
        .persistence
        .transition_region_descriptor(region_id, current.generation, target)
        .await
        .map_err(mesh_status)?;
    Ok(())
}

async fn apply_cell_target_state(
    state: &AppState,
    region_id: &str,
    cell_id: &str,
    requested_state: &str,
) -> Result<(), Status> {
    let Some(target) = parse_lifecycle_state(requested_state)? else {
        return Ok(());
    };
    let current = state
        .persistence
        .list_cell_descriptors(None)
        .await
        .map_err(mesh_status)?
        .into_iter()
        .find(|cell| cell.region == region_id && cell.cell_id == cell_id)
        .ok_or_else(|| Status::not_found("cell not found after registration"))?;
    if current.state == target {
        return Ok(());
    }
    state
        .persistence
        .transition_cell_descriptor(&current.region, cell_id, current.generation, target)
        .await
        .map_err(mesh_status)?;
    Ok(())
}

async fn apply_node_target_state(
    state: &AppState,
    node_id: &str,
    requested_state: &str,
) -> Result<(), Status> {
    let Some(target) = parse_lifecycle_state(requested_state)? else {
        return Ok(());
    };
    let current = state
        .persistence
        .list_node_descriptors(None, None)
        .await
        .map_err(mesh_status)?
        .into_iter()
        .find(|node| node.node_id == node_id)
        .ok_or_else(|| Status::not_found("node not found after registration"))?;
    if current.state == target {
        return Ok(());
    }
    state
        .persistence
        .transition_node_descriptor(node_id, current.generation, target, None)
        .await
        .map_err(mesh_status)?;
    Ok(())
}

fn parse_lifecycle_state(value: &str) -> Result<Option<CoreLifecycleState>, Status> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == "joining" {
        return Ok(None);
    }
    match normalized.as_str() {
        "active" => Ok(Some(CoreLifecycleState::Active)),
        "read_only" | "readonly" => Ok(Some(CoreLifecycleState::ReadOnly)),
        "draining" => Ok(Some(CoreLifecycleState::Draining)),
        "drained" => Ok(Some(CoreLifecycleState::Drained)),
        "drained_with_exceptions" => Ok(Some(CoreLifecycleState::DrainedWithExceptions)),
        "offline" => Ok(Some(CoreLifecycleState::Offline)),
        "removed" => Ok(Some(CoreLifecycleState::Removed)),
        _ => Err(Status::invalid_argument(format!(
            "unsupported lifecycle state {value}"
        ))),
    }
}

fn parse_node_capabilities(values: &[String]) -> Result<Vec<CoreNodeCapability>, Status> {
    if values.is_empty() {
        return Ok(vec![
            CoreNodeCapability::Object,
            CoreNodeCapability::Index,
            CoreNodeCapability::Metadata,
        ]);
    }
    values
        .iter()
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "object" | "objects" => Ok(CoreNodeCapability::Object),
            "index" | "indexes" | "indices" => Ok(CoreNodeCapability::Index),
            "personaldb" | "personal_db" => Ok(CoreNodeCapability::PersonalDb),
            "metadata" | "coremeta" => Ok(CoreNodeCapability::Metadata),
            "gateway" | "gateways" => Ok(CoreNodeCapability::Gateway),
            "admin" => Ok(CoreNodeCapability::Admin),
            _ => Err(Status::invalid_argument(format!(
                "unsupported node capability {value}"
            ))),
        })
        .collect()
}

async fn mesh_topology_exists(state: &AppState) -> Result<bool, Status> {
    Ok(!state
        .persistence
        .list_region_descriptors()
        .await
        .map_err(mesh_status)?
        .is_empty()
        || !state
            .persistence
            .list_cell_descriptors(None)
            .await
            .map_err(mesh_status)?
            .is_empty()
        || !state
            .persistence
            .list_node_descriptors(None, None)
            .await
            .map_err(mesh_status)?
            .is_empty())
}

fn encode_bootstrap_snapshot_rows(
    rows: Vec<crate::core_store::CoreMetaEncodedOwnedRow>,
) -> Vec<CoreMetaRowMutation> {
    rows.into_iter()
        .map(|row| CoreMetaRowMutation {
            row_hash: crate::core_store::core_meta_encoded_row_hash_with_delete(
                &row.cf,
                &row.core_meta_key,
                &row.value_envelope,
                row.delete_marker,
            ),
            column_family: row.cf,
            core_meta_key: row.core_meta_key,
            value_envelope: row.value_envelope,
            delete_marker: row.delete_marker,
        })
        .collect()
}

fn decode_bootstrap_snapshot_rows(
    rows: Vec<CoreMetaRowMutation>,
) -> Result<Vec<crate::core_store::CoreMetaEncodedOwnedRow>, Status> {
    let mut seen = std::collections::BTreeSet::new();
    rows.into_iter()
        .map(|row| {
            let actual_hash = crate::core_store::core_meta_encoded_row_hash_with_delete(
                &row.column_family,
                &row.core_meta_key,
                &row.value_envelope,
                row.delete_marker,
            );
            if row.row_hash != actual_hash {
                return Err(Status::invalid_argument(
                    "bootstrap CoreMeta row_hash mismatch",
                ));
            }
            if !seen.insert((row.column_family.clone(), row.core_meta_key.clone())) {
                return Err(Status::invalid_argument(
                    "bootstrap CoreMeta snapshot contains duplicate rows",
                ));
            }
            Ok(crate::core_store::CoreMetaEncodedOwnedRow {
                cf: row.column_family,
                core_meta_key: row.core_meta_key,
                value_envelope: row.value_envelope,
                delete_marker: row.delete_marker,
                root_key_hash: String::new(),
                root_generation: 0,
                visibility_state: crate::core_store::CoreMetaVisibilityState::Unspecified,
            })
        })
        .collect()
}

async fn ensure_bootstrap_topology_matches(
    state: &AppState,
    expected_regions: &[CreateRegionDescriptor],
    expected_cells: &[RegisterCellDescriptor],
    expected_nodes: &[RegisterNodeDescriptor],
) -> Result<(), Status> {
    let regions = state
        .persistence
        .list_region_descriptors()
        .await
        .map_err(mesh_status)?;
    let cells = state
        .persistence
        .list_cell_descriptors(None)
        .await
        .map_err(mesh_status)?;
    let nodes = state
        .persistence
        .list_node_descriptors(None, None)
        .await
        .map_err(mesh_status)?;
    if regions.len() != expected_regions.len()
        || cells.len() != expected_cells.len()
        || nodes.len() != expected_nodes.len()
    {
        return Err(Status::failed_precondition(
            "bootstrap CoreMeta snapshot topology cardinality mismatch",
        ));
    }

    for expected in expected_regions {
        let Some(actual) = regions
            .iter()
            .find(|region| region.region == expected.region)
        else {
            return Err(Status::failed_precondition(
                "bootstrap CoreMeta snapshot is missing a requested region",
            ));
        };
        if actual.mesh_id != expected.mesh_id
            || actual.state != CoreLifecycleState::Active
            || actual.public_base_url != expected.public_base_url
            || actual.virtual_host_suffix != expected.virtual_host_suffix
            || actual.placement_weight != expected.placement_weight
            || actual.default_cell != expected.default_cell
        {
            return Err(Status::failed_precondition(
                "bootstrap CoreMeta snapshot region descriptor mismatch",
            ));
        }
    }
    for expected in expected_cells {
        let Some(actual) = cells
            .iter()
            .find(|cell| cell.region == expected.region && cell.cell_id == expected.cell_id)
        else {
            return Err(Status::failed_precondition(
                "bootstrap CoreMeta snapshot is missing a requested cell",
            ));
        };
        if actual.mesh_id != expected.mesh_id
            || actual.state != CoreLifecycleState::Active
            || actual.placement_weight != expected.placement_weight
            || actual.failure_domain != expected.failure_domain
        {
            return Err(Status::failed_precondition(
                "bootstrap CoreMeta snapshot cell descriptor mismatch",
            ));
        }
    }
    for expected in expected_nodes {
        let Some(actual) = nodes.iter().find(|node| node.node_id == expected.node_id) else {
            return Err(Status::failed_precondition(
                "bootstrap CoreMeta snapshot is missing a requested node",
            ));
        };
        if actual.mesh_id != expected.mesh_id
            || actual.region != expected.region
            || actual.cell_id != expected.cell_id
            || actual.receipt_signing_public_key != expected.receipt_signing_public_key
            || actual.public_api_addr != expected.public_api_addr
            || actual.capabilities != expected.capabilities
            || actual.capacity_json_hash
                != capacity_json_hash(&expected.capacity_json).map_err(mesh_status)?
            || actual.state != CoreLifecycleState::Active
        {
            return Err(Status::failed_precondition(
                "bootstrap CoreMeta snapshot node descriptor mismatch",
            ));
        }
    }
    Ok(())
}

fn mesh_write_response(
    request_id: String,
    mutation_id: String,
    transaction_id: Option<&str>,
) -> WriteResponse {
    WriteResponse {
        request_id,
        mutation_id,
        state: if transaction_id.is_some() {
            WriteState::Staged as i32
        } else {
            WriteState::Finalised as i32
        },
        root_generation: None,
        transaction_manifest_ref: None,
        idempotency_outcome: "accepted".to_string(),
        retry_after_hint: None,
        finalisation_error: None,
        saga: None,
    }
}

fn validate_bootstrap_topology_size(req: &BootstrapMeshTopologyRequest) -> Result<(), Status> {
    for (actual, maximum, field) in [
        (req.regions.len(), MAX_BOOTSTRAP_REGIONS, "regions"),
        (req.cells.len(), MAX_BOOTSTRAP_CELLS, "cells"),
        (req.nodes.len(), MAX_BOOTSTRAP_NODES, "nodes"),
        (
            req.canonical_coremeta_rows.len(),
            MAX_BOOTSTRAP_COREMETA_ROWS,
            "canonical_coremeta_rows",
        ),
    ] {
        if actual > maximum {
            return Err(Status::invalid_argument(format!(
                "bootstrap topology {field} must not exceed {maximum} entries"
            )));
        }
    }
    Ok(())
}

fn request_id<T>(request: &Request<T>) -> String {
    request
        .extensions()
        .get::<middleware::AnvilRequestId>()
        .map(|request_id| request_id.0.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string())
}

#[cfg(test)]
mod bootstrap_contract_tests {
    use super::*;

    #[test]
    fn bootstrap_topology_collections_are_bounded() {
        let request = BootstrapMeshTopologyRequest {
            regions: vec![PutRegionRequest::default(); MAX_BOOTSTRAP_REGIONS + 1],
            ..Default::default()
        };
        assert_eq!(
            validate_bootstrap_topology_size(&request)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

fn mesh_status(error: impl std::fmt::Display) -> Status {
    let message = error.to_string();
    if message.contains("TransactionNotFound") {
        Status::not_found("TransactionNotFound")
    } else if message.contains("TransactionPrincipalMismatch") {
        Status::permission_denied("TransactionPrincipalMismatch")
    } else if message.contains("TransactionScopeMismatch") {
        Status::failed_precondition("TransactionScopeMismatch")
    } else if message.contains("TransactionExpired")
        || message.contains("TransactionRolledBack")
        || message.contains("TransactionAlreadyCommitted")
        || message.contains("TransactionNotOpen")
        || message.contains("TransactionNotCommittable")
    {
        Status::failed_precondition(message)
    } else if message.contains("TransactionConflict") {
        Status::aborted("TransactionConflict")
    } else if message.contains("idempotency conflict") {
        Status::already_exists("TransactionConflict")
    } else if message.contains("not found") {
        Status::not_found(message)
    } else if message.contains("generation conflict") || message.contains("transition denied") {
        Status::failed_precondition(message)
    } else if message.contains("invalid") || message.contains("already exists") {
        Status::invalid_argument(message)
    } else {
        Status::internal(message)
    }
}
