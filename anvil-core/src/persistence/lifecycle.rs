use super::*;

const MAX_NODE_RUNTIME_BLOCKERS: usize = 256;
const TASK_LEASE_PAGE_SIZE: usize = 256;

fn push_runtime_blocker(blockers: &mut Vec<String>, blocker: String) -> bool {
    blockers.push(blocker);
    blockers.len() == MAX_NODE_RUNTIME_BLOCKERS
}

fn runtime_cursor_error(kind: &str) -> crate::mesh_lifecycle::LifecycleError {
    crate::mesh_lifecycle::LifecycleError::InvalidArgument(format!(
        "{kind} page cursor did not advance"
    ))
}

impl Persistence {
    pub async fn create_region(&self, name: &str) -> Result<bool> {
        let permit = self.control_write_permit().await?;
        control_journal::create_region_with_permit(
            &self.storage,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_regions(&self) -> Result<Vec<String>> {
        let revision = control_journal::current_control_collection_revision(&self.storage).await?;
        let mut cursor = None;
        let mut regions = Vec::new();
        loop {
            let page =
                control_journal::page_regions(&self.storage, &revision, cursor.as_deref(), 512)
                    .await?;
            regions.extend(page.regions);
            let Some(next) = page.next_tuple_key else {
                return Ok(regions);
            };
            cursor = Some(next);
        }
    }

    pub async fn create_region_descriptor(
        &self,
        input: crate::mesh_lifecycle::CreateRegionDescriptor,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::RegionDescriptor> {
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
            &input.region,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::create_region_with_control(
            &self.storage,
            input,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn transition_region_descriptor(
        &self,
        region: &str,
        expected_generation: u64,
        target: crate::mesh_lifecycle::LifecycleState,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::RegionDescriptor> {
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
            region,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::transition_region_with_control(
            &self.storage,
            region,
            expected_generation,
            target,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn activate_region_descriptor(
        &self,
        region: &str,
        expected_generation: u64,
        checkpoint: &crate::mesh_lifecycle::ActivationCheckpoint,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::RegionDescriptor> {
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
            region,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::REGION_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::activate_region_with_control(
            &self.storage,
            region,
            expected_generation,
            checkpoint,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn list_region_descriptors(
        &self,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<crate::mesh_lifecycle::RegionDescriptor>> {
        crate::mesh_lifecycle::list_regions(&self.storage).await
    }

    pub async fn register_cell_descriptor(
        &self,
        input: crate::mesh_lifecycle::RegisterCellDescriptor,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::CellDescriptor> {
        let record_key = format!("{}/{}", input.region, input.cell_id);
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::CELL_DESCRIPTOR_STREAM_FAMILY,
            &record_key,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::CELL_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::register_cell_with_control(
            &self.storage,
            input,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn transition_cell_descriptor(
        &self,
        region: &str,
        cell_id: &str,
        expected_generation: u64,
        target: crate::mesh_lifecycle::LifecycleState,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::CellDescriptor> {
        let record_key = format!("{region}/{cell_id}");
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::CELL_DESCRIPTOR_STREAM_FAMILY,
            &record_key,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::CELL_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::transition_cell_with_control(
            &self.storage,
            region,
            cell_id,
            expected_generation,
            target,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn list_cell_descriptors(
        &self,
        region_filter: Option<&str>,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<crate::mesh_lifecycle::CellDescriptor>> {
        crate::mesh_lifecycle::list_cells(&self.storage, region_filter).await
    }

    pub async fn register_node_descriptor(
        &self,
        input: crate::mesh_lifecycle::RegisterNodeDescriptor,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::NodeDescriptor> {
        let record_key = format!("{}/{}/{}", input.region, input.cell_id, input.node_id);
        let node_id = input.node_id.clone();
        let receipt_signing_public_key = input.receipt_signing_public_key.clone();
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::NODE_DESCRIPTOR_STREAM_FAMILY,
            &record_key,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::NODE_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        let descriptor = crate::mesh_lifecycle::register_node_with_control(
            &self.storage,
            input,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await?;
        let store = CoreStore::new(self.storage.clone())
            .await
            .map_err(|err| crate::mesh_lifecycle::LifecycleError::Other(err.into()))?;
        store
            .register_node_receipt_signing_public_key(&node_id, &receipt_signing_public_key)
            .map_err(|err| crate::mesh_lifecycle::LifecycleError::Other(err.into()))?;
        Ok(descriptor)
    }

    pub async fn transition_node_descriptor(
        &self,
        node_id: &str,
        expected_generation: u64,
        target: crate::mesh_lifecycle::LifecycleState,
        drain: Option<crate::mesh_lifecycle::NodeDrainDescriptor>,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::mesh_lifecycle::NodeDescriptor> {
        let node = crate::mesh_lifecycle::list_nodes(&self.storage, None, None)
            .await?
            .into_iter()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| crate::mesh_lifecycle::LifecycleError::NotFound {
                resource_kind: "node",
                resource_id: node_id.to_string(),
            })?;
        if node.generation != expected_generation {
            return Err(crate::mesh_lifecycle::LifecycleError::GenerationConflict {
                resource_kind: "node",
                resource_id: node_id.to_string(),
                expected: expected_generation,
                current: node.generation,
            });
        }
        crate::mesh_lifecycle::validate_node_transition(node.state, target).map_err(|_| {
            crate::mesh_lifecycle::LifecycleError::LifecycleTransitionDenied {
                resource_kind: "node",
                resource_id: node_id.to_string(),
                from: node.state,
                to: target,
            }
        })?;
        match target {
            crate::mesh_lifecycle::LifecycleState::Drained => {
                self.ensure_node_has_no_runtime_ownership(node_id).await?;
            }
            crate::mesh_lifecycle::LifecycleState::Offline
            | crate::mesh_lifecycle::LifecycleState::Removed => {
                self.force_expire_node_runtime_ownership(node_id).await?;
            }
            _ => {}
        }
        let record_key = format!("{}/{}/{}", node.region, node.cell_id, node.node_id);
        let partition = crate::mesh_lifecycle::lifecycle_control_partition(
            crate::mesh_lifecycle::NODE_DESCRIPTOR_STREAM_FAMILY,
            &record_key,
        );
        let permit = self
            .mesh_control_write_permit_for_stream(
                crate::mesh_lifecycle::NODE_DESCRIPTOR_STREAM_FAMILY,
                &partition,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        crate::mesh_lifecycle::transition_node_with_control(
            &self.storage,
            node_id,
            expected_generation,
            target,
            drain,
            crate::mesh_lifecycle::LifecycleControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
    }

    pub async fn node_runtime_ownership_blockers(
        &self,
        node_id: &str,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<String>> {
        let now_nanos = current_time_nanos().map_err(|err| {
            crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
        })?;
        self.node_runtime_ownership_blockers_at(node_id, now_nanos)
            .await
    }

    pub(super) async fn ensure_node_has_no_runtime_ownership(
        &self,
        node_id: &str,
    ) -> crate::mesh_lifecycle::LifecycleResult<()> {
        let blockers = self.node_runtime_ownership_blockers(node_id).await?;
        if blockers.is_empty() {
            return Ok(());
        }
        Err(crate::mesh_lifecycle::LifecycleError::InvalidArgument(
            format!(
                "node {node_id} drain cannot complete: {} runtime ownership record(s) still exist: {}",
                blockers.len(),
                blockers.join(", ")
            ),
        ))
    }

    pub(super) async fn node_runtime_ownership_blockers_at(
        &self,
        node_id: &str,
        now_nanos: i64,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<String>> {
        let mut blockers = Vec::new();
        let mut partition_cursor = None;
        loop {
            let page = list_partition_owners_for_node_page(
                &self.storage,
                node_id,
                partition_cursor.as_ref(),
                MAX_PARTITION_FENCE_PAGE_SIZE,
                &self.partition_owner_signing_key,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
            for owner in page.owners {
                if push_runtime_blocker(
                    &mut blockers,
                    format!(
                        "partition_owner:{}/{}:{:?}:fence={}",
                        owner.partition_family, owner.partition_id, owner.status, owner.fence_token
                    ),
                ) {
                    blockers.sort();
                    return Ok(blockers);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if partition_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.as_str() >= next_cursor.as_str())
            {
                return Err(runtime_cursor_error("partition owner"));
            }
            partition_cursor = Some(next_cursor);
        }

        let mut ownership_cursor = None;
        loop {
            let page = list_active_ownership_fences_for_node_page(
                &self.storage,
                node_id,
                now_nanos,
                ownership_cursor.as_ref(),
                MAX_PARTITION_FENCE_PAGE_SIZE,
                &self.partition_owner_signing_key,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
            for record in page.fences {
                if push_runtime_blocker(
                    &mut blockers,
                    format!(
                        "ownership_fence:{}/{}:{:?}:fence={}",
                        record.resource.resource_kind.as_str(),
                        record.resource.resource_id,
                        record.state,
                        record.fence
                    ),
                ) {
                    blockers.sort();
                    return Ok(blockers);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if ownership_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.as_str() >= next_cursor.as_str())
            {
                return Err(runtime_cursor_error("ownership fence"));
            }
            ownership_cursor = Some(next_cursor);
        }

        let mut task_cursor: Option<Vec<u8>> = None;
        loop {
            let page = task_lease::list_active_task_leases_for_node_page(
                &self.storage,
                node_id,
                now_nanos,
                &self.partition_owner_signing_key,
                task_cursor.as_deref(),
                TASK_LEASE_PAGE_SIZE,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
            for lease in page.leases {
                if push_runtime_blocker(
                    &mut blockers,
                    format!(
                        "task_lease:{}:{}:fence={}",
                        lease.task_kind, lease.task_id, lease.fence_token
                    ),
                ) {
                    blockers.sort();
                    return Ok(blockers);
                }
            }
            let Some(next_cursor) = page.next_tuple_key else {
                break;
            };
            if task_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.as_slice() >= next_cursor.as_slice())
            {
                return Err(runtime_cursor_error("task lease"));
            }
            task_cursor = Some(next_cursor);
        }
        blockers.sort();
        Ok(blockers)
    }

    pub(super) async fn force_expire_node_runtime_ownership(
        &self,
        node_id: &str,
    ) -> crate::mesh_lifecycle::LifecycleResult<()> {
        let now_nanos = current_time_nanos().map_err(|err| {
            crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
        })?;
        let mut partition_cursor = None;
        loop {
            let page = list_partition_owners_for_node_page(
                &self.storage,
                node_id,
                partition_cursor.as_ref(),
                MAX_PARTITION_FENCE_PAGE_SIZE,
                &self.partition_owner_signing_key,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
            for owner in page.owners {
                force_expire_partition_owner_for_node(
                    &self.storage,
                    &owner.partition_family,
                    &owner.partition_id,
                    node_id,
                    now_nanos,
                    &self.partition_owner_signing_key,
                )
                .await
                .map_err(|err| {
                    crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
                })?;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if partition_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.as_str() >= next_cursor.as_str())
            {
                return Err(runtime_cursor_error("partition owner"));
            }
            partition_cursor = Some(next_cursor);
        }

        let admin = OwnershipPrincipal {
            tenant_id: 0,
            principal_kind: "node_admin".to_string(),
            principal_id: self.owner_node_id.clone(),
            actor_instance_id: self.owner_node_id.clone(),
            display_name: self.owner_node_id.clone(),
            region: self.region.clone(),
            cell: self.cell_id.clone(),
        };
        let mut ownership_cursor = None;
        loop {
            let page = list_active_ownership_fences_for_node_page(
                &self.storage,
                node_id,
                now_nanos,
                ownership_cursor.as_ref(),
                MAX_PARTITION_FENCE_PAGE_SIZE,
                &self.partition_owner_signing_key,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
            for record in page.fences {
                let mut admin = admin.clone();
                admin.tenant_id = record.owner.tenant_id;
                force_expire_ownership(
                    &self.storage,
                    ForceExpireOwnership {
                        request_id: format!(
                            "node-force-expire-{}-{}",
                            node_id,
                            record.resource.resource_id.replace('/', "-")
                        ),
                        idempotency_key: format!(
                            "node-force-expire-{}-{}-{}",
                            node_id, record.resource.resource_id, record.fence
                        ),
                        resource: record.resource,
                        admin: admin.clone(),
                        reason: format!(
                            "node {node_id} transitioned to non-owning lifecycle state"
                        ),
                        now_nanos,
                    },
                    &self.partition_owner_signing_key,
                )
                .await
                .map_err(|err| {
                    crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
                })?;
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if ownership_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.as_str() >= next_cursor.as_str())
            {
                return Err(runtime_cursor_error("ownership fence"));
            }
            ownership_cursor = Some(next_cursor);
        }

        let mut task_cursor: Option<Vec<u8>> = None;
        loop {
            let page = task_lease::list_active_task_leases_for_node_page(
                &self.storage,
                node_id,
                now_nanos,
                &self.partition_owner_signing_key,
                task_cursor.as_deref(),
                TASK_LEASE_PAGE_SIZE,
            )
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
            for lease in page.leases {
                task_lease::force_release_task_lease(
                    &self.storage,
                    lease.owner.tenant_id,
                    &lease.task_id,
                    &self.partition_owner_signing_key,
                )
                .await
                .map_err(|err| {
                    crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
                })?;
            }
            let Some(next_cursor) = page.next_tuple_key else {
                break;
            };
            if task_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.as_slice() >= next_cursor.as_slice())
            {
                return Err(runtime_cursor_error("task lease"));
            }
            task_cursor = Some(next_cursor);
        }
        Ok(())
    }

    pub async fn list_node_descriptors(
        &self,
        region_filter: Option<&str>,
        cell_filter: Option<&str>,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<crate::mesh_lifecycle::NodeDescriptor>> {
        crate::mesh_lifecycle::list_nodes(&self.storage, region_filter, cell_filter).await
    }

    pub async fn create_host_alias_descriptor(
        &self,
        routing_config: &crate::routing::RoutingConfig,
        input: crate::mesh_lifecycle::CreateHostAliasDescriptor,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::routing::HostAliasDescriptor> {
        let descriptor =
            crate::mesh_lifecycle::create_host_alias(&self.storage, routing_config, input).await?;
        let partition = mesh_directory::host_alias_partition(&descriptor.hostname)
            .map_err(mesh_directory_lifecycle_error)?;
        let permit = self
            .mesh_control_write_permit(mesh_directory::RoutingRecordFamily::HostAlias, &partition)
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        mesh_directory::write_host_alias_descriptor(
            &self.storage,
            &descriptor,
            mesh_directory::MeshControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
        .map_err(mesh_directory_lifecycle_error)?;
        Ok(descriptor)
    }

    pub async fn create_host_alias_descriptor_in_transaction(
        &self,
        routing_config: &crate::routing::RoutingConfig,
        input: crate::mesh_lifecycle::CreateHostAliasDescriptor,
        transaction_id: &str,
        principal: &str,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::routing::HostAliasDescriptor> {
        let descriptor = crate::mesh_lifecycle::create_host_alias_in_transaction(
            &self.storage,
            routing_config,
            input,
            transaction_id,
            principal,
        )
        .await?;
        mesh_directory::write_host_alias_descriptor_in_transaction(
            &self.storage,
            &descriptor,
            true,
            transaction_id,
            principal,
        )
        .await
        .map_err(mesh_directory_lifecycle_error)?;
        Ok(descriptor)
    }

    pub async fn transition_host_alias_descriptor(
        &self,
        hostname: &str,
        expected_generation: u64,
        target: crate::routing::HostAliasState,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::routing::HostAliasDescriptor> {
        let descriptor = crate::mesh_lifecycle::transition_host_alias(
            &self.storage,
            hostname,
            expected_generation,
            target,
        )
        .await?;
        let partition = mesh_directory::host_alias_partition(&descriptor.hostname)
            .map_err(mesh_directory_lifecycle_error)?;
        let permit = self
            .mesh_control_write_permit(mesh_directory::RoutingRecordFamily::HostAlias, &partition)
            .await
            .map_err(|err| {
                crate::mesh_lifecycle::LifecycleError::InvalidArgument(err.to_string())
            })?;
        mesh_directory::write_host_alias_descriptor(
            &self.storage,
            &descriptor,
            mesh_directory::MeshControlWriteAuthority {
                permit: &permit,
                signing_key: &self.partition_owner_signing_key,
            },
        )
        .await
        .map_err(mesh_directory_lifecycle_error)?;
        Ok(descriptor)
    }

    pub async fn transition_host_alias_descriptor_in_transaction(
        &self,
        hostname: &str,
        expected_generation: u64,
        target: crate::routing::HostAliasState,
        transaction_id: &str,
        principal: &str,
    ) -> crate::mesh_lifecycle::LifecycleResult<crate::routing::HostAliasDescriptor> {
        let descriptor = crate::mesh_lifecycle::transition_host_alias_in_transaction(
            &self.storage,
            hostname,
            expected_generation,
            target,
            transaction_id,
            principal,
        )
        .await?;
        mesh_directory::write_host_alias_descriptor_in_transaction(
            &self.storage,
            &descriptor,
            false,
            transaction_id,
            principal,
        )
        .await
        .map_err(mesh_directory_lifecycle_error)?;
        Ok(descriptor)
    }

    pub async fn get_host_alias_descriptor(
        &self,
        hostname: &str,
    ) -> crate::mesh_lifecycle::LifecycleResult<Option<crate::routing::HostAliasDescriptor>> {
        mesh_directory::read_host_alias_descriptor(&self.storage, hostname)
            .await
            .map_err(mesh_directory_lifecycle_error)
    }

    pub async fn list_host_alias_descriptors(
        &self,
        region_filter: Option<&str>,
    ) -> crate::mesh_lifecycle::LifecycleResult<Vec<crate::routing::HostAliasDescriptor>> {
        crate::mesh_lifecycle::list_host_aliases(&self.storage, region_filter).await
    }
}
