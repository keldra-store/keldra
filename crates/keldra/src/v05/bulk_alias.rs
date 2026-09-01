//! Independent sealed dispatch for BulkWrite items addressed through links.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use keldra_api::v1::bulk_outcome::Outcome;
use keldra_api::v1::{BulkOperation, BulkOutcome};
use keldra_store::{
    BatchOperation, BlobRef, ObjectKey, PublishRequest, PutMode, ResolvedObjectLink, VersionId,
    object_link_command_fingerprint,
};
use tonic::Status;

use super::*;

const MAX_PARALLEL_ALIAS_MUTATIONS: usize = 32;

pub(super) struct AliasReplayProbe {
    pub(super) lookup: crate::programs::BuiltInReplayLookup,
    pub(super) requested: ObjectKey,
    pub(super) command_id: String,
    pub(super) fingerprint: [u8; 32],
}

pub(super) struct PreparedBulkItem {
    pub(super) index: usize,
    pub(super) operation: Option<BulkOperation>,
    pub(super) requested: ObjectKey,
    pub(super) definition_intent: Option<keldra_store::DefinitionMutationIntent>,
    pub(super) resolution: Option<object_link::ResolvedAddress>,
    pub(super) replay_receipt: Option<ApiMutationReceipt>,
    pub(super) inbound_bytes: u64,
}

pub(super) struct BulkPrepareResult {
    pub(super) items: Vec<PreparedBulkItem>,
    pub(super) outcomes: Vec<BulkOutcome>,
    pub(super) validation_duration: std::time::Duration,
    pub(super) identity_resolution_duration: std::time::Duration,
    pub(super) authorization_duration: std::time::Duration,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn prepare_before_live_dispatch(
    service: &ObjectServiceImpl,
    caller: &Caller,
    path_access: &object_path_access::ObjectPathAccess,
    plugin_scope: Option<&crate::authentication::PluginObjectScope>,
    operations: Vec<BulkOperation>,
    replay_checked: bool,
    deadline: tokio::time::Instant,
) -> Result<BulkPrepareResult, Status> {
    let validation_started = std::time::Instant::now();
    let mut outcomes = Vec::new();
    let mut pending = Vec::with_capacity(operations.len());
    for (index, operation) in operations.into_iter().enumerate() {
        match bulk::validate_operation(&operation, service.max_blob_bytes) {
            Ok((key, permission)) => {
                match object_path_access::require_key(path_access, &key)
                    .and_then(|()| require_plugin_key_scope(plugin_scope, &key))
                    .and_then(|()| require_caller_tenant(caller, &key))
                {
                    Ok(()) => pending.push(Some((
                        index,
                        operation,
                        key,
                        permission,
                        object_path_access::definition_intent(path_access, index),
                    ))),
                    Err(error) => outcomes.push(bulk_authorization_failure(index, &error)),
                }
            }
            Err(error) => outcomes.push(BulkOutcome {
                index: index as u32,
                outcome: Some(Outcome::Failure(api_request_failure(error))),
            }),
        }
    }
    let validation_duration = validation_started.elapsed();
    let identity_started = std::time::Instant::now();

    let mut probes = Vec::new();
    let mut probe_positions = Vec::new();
    if !replay_checked && service.programs.generalized_atomic_paths_active()? {
        for (position, item) in pending.iter().enumerate() {
            let Some((_, operation, key, _, _)) = item else {
                continue;
            };
            if let Some(probe) = replay_probe(operation, key)? {
                probe_positions.push(position);
                probes.push(probe);
            }
        }
    }
    let lookups = probes
        .iter()
        .zip(&probe_positions)
        .map(|(probe, position)| {
            let mut lookup = probe.lookup;
            let original_index = pending[*position]
                .as_ref()
                .expect("replay positions originate from pending items")
                .0;
            lookup.original_index = u32::try_from(original_index)
                .map_err(|_| Status::resource_exhausted("bulk replay index exceeds u32"))?;
            Ok(lookup)
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let replay_results = if lookups.is_empty() {
        Vec::new()
    } else {
        match service.programs.executor_replay_routing_target()? {
            Some((target, address, nomination_log_index)) => {
                service
                    .cluster_peers
                    .route_built_in_replay_batch(
                        target,
                        &address,
                        nomination_log_index,
                        &lookups,
                        deadline_remaining(deadline)?,
                    )
                    .await?
            }
            None => {
                service
                    .programs
                    .replay_builtin_object_transactions(&lookups)
                    .await?
            }
        }
    };
    if replay_results.len() != probes.len() {
        return Err(Status::data_loss(
            "built-in replay lookup returned the wrong result count",
        ));
    }

    let mut prepared = Vec::with_capacity(pending.len());
    for ((position, probe), result) in probe_positions.into_iter().zip(probes).zip(replay_results) {
        match result {
            Err(error) => {
                let (index, _, _, _, _) = pending[position]
                    .take()
                    .expect("replay positions originate from pending items");
                outcomes.push(BulkOutcome {
                    index: index as u32,
                    outcome: Some(Outcome::Failure(api_request_failure(error))),
                });
            }
            Ok(None) => {}
            Ok(Some(result)) => {
                let (index, operation, requested, permission, definition_intent) = pending
                    [position]
                    .take()
                    .expect("replay positions originate from pending items");
                match object_link::bulk_replay_result(
                    result,
                    &probe.requested,
                    &probe.command_id,
                    probe.fingerprint,
                    probe.lookup.authority_kind,
                ) {
                    Ok((canonical, receipt)) => {
                        match object_path_access::require_key(path_access, &canonical)
                            .and_then(|()| require_plugin_key_scope(plugin_scope, &canonical))
                        {
                            Ok(()) => prepared.push((
                                PreparedBulkItem {
                                    index,
                                    operation: None,
                                    requested,
                                    definition_intent,
                                    resolution: None,
                                    replay_receipt: Some(receipt),
                                    inbound_bytes: bulk::operation_inbound_bytes(&operation),
                                },
                                canonical,
                                permission,
                            )),
                            Err(error) => outcomes.push(bulk_authorization_failure(index, &error)),
                        }
                    }
                    Err(error) => outcomes.push(BulkOutcome {
                        index: index as u32,
                        outcome: Some(Outcome::Failure(api_request_failure(error))),
                    }),
                }
            }
        }
    }

    let mut resolution_cache = BTreeMap::new();
    for item in pending.into_iter().flatten() {
        let (index, operation, key, permission, definition_intent) = item;
        match resolve_cached(&mut resolution_cache, &key, |key| {
            object_link::resolve_current(service, key)
        })
        .await
        {
            Ok(resolution) => {
                let canonical = resolution.canonical().clone();
                match object_path_access::require_key(path_access, &canonical)
                    .and_then(|()| require_plugin_key_scope(plugin_scope, &canonical))
                {
                    Ok(()) => prepared.push((
                        PreparedBulkItem {
                            index,
                            operation: Some(operation),
                            requested: key,
                            definition_intent,
                            resolution: Some(resolution),
                            replay_receipt: None,
                            inbound_bytes: 0,
                        },
                        canonical,
                        permission,
                    )),
                    Err(error) => outcomes.push(bulk_authorization_failure(index, &error)),
                }
            }
            Err(error) => outcomes.push(BulkOutcome {
                index: index as u32,
                outcome: Some(Outcome::Failure(api_request_failure(error))),
            }),
        }
    }

    let identity_resolution_duration = identity_started.elapsed();
    let authorization_started = std::time::Instant::now();
    let checks = prepared
        .iter()
        .map(|(_, canonical, permission)| (canonical.clone(), *permission))
        .collect::<Vec<_>>();
    let allowed = if object_path_access::is_internal(path_access) {
        vec![true; checks.len()]
    } else if checks.is_empty() {
        Vec::new()
    } else {
        service
            .authoritative_system
            .allows_objects(caller, &checks)
            .await?
    };
    if allowed.len() != prepared.len() {
        return Err(Status::data_loss(
            "bulk authorization returned the wrong result count",
        ));
    }
    let authorization_duration = authorization_started.elapsed();
    let mut items = Vec::with_capacity(prepared.len());
    for ((item, _, _), allowed) in prepared.into_iter().zip(allowed) {
        if allowed {
            items.push(item);
        } else {
            outcomes.push(bulk_authorization_failure(
                item.index,
                &Status::permission_denied("bulk object operation is not authorized"),
            ));
        }
    }
    Ok(BulkPrepareResult {
        items,
        outcomes,
        validation_duration,
        identity_resolution_duration,
        authorization_duration,
    })
}

async fn resolve_cached<F, Fut>(
    cache: &mut BTreeMap<ObjectKey, Result<object_link::ResolvedAddress, Status>>,
    key: &ObjectKey,
    resolve: F,
) -> Result<object_link::ResolvedAddress, Status>
where
    F: FnOnce(ObjectKey) -> Fut,
    Fut: Future<Output = Result<object_link::ResolvedAddress, Status>>,
{
    if let Some(cached) = cache.get(key) {
        return cached.clone();
    }
    let resolved = resolve(key.clone()).await;
    cache.insert(key.clone(), resolved.clone());
    resolved
}

pub(super) fn replay_probe(
    operation: &BulkOperation,
    requested: &ObjectKey,
) -> Result<Option<AliasReplayProbe>, Status> {
    use keldra_api::v1::bulk_operation::Operation;

    let (authority_kind, command_id, fingerprint) = match operation.operation.as_ref() {
        Some(Operation::PutIfAbsent(_)) => return Ok(None),
        Some(Operation::Put(request)) | Some(Operation::PutImmutable(request)) => {
            let mode = if matches!(
                operation.operation.as_ref(),
                Some(Operation::PutImmutable(_))
            ) {
                PutMode::PutImmutable
            } else {
                PutMode::Put
            };
            let authority_kind = if mode == PutMode::PutImmutable {
                object_link::PUT_IMMUTABLE_THROUGH_LINK_AUTHORITY_KIND
            } else {
                object_link::PUT_THROUGH_LINK_AUTHORITY_KIND
            };
            let command_id = request.command_id.clone();
            let publish = PublishRequest {
                key: requested.clone(),
                blob: BlobRef {
                    hash: *blake3::hash(&request.bytes).as_bytes(),
                    length: request.bytes.len() as u64,
                },
                content_type: content_type(request.content_type.clone())?,
                mode,
                command_id: Some(command_id.clone()),
                durability: durability(request.durability)?,
            };
            (
                authority_kind,
                command_id,
                object_link::linked_put_fingerprint(requested, &publish),
            )
        }
        Some(Operation::PutIfVersion(request)) => {
            let command_id = request.command_id.clone();
            let publish = PublishRequest {
                key: requested.clone(),
                blob: BlobRef {
                    hash: *blake3::hash(&request.bytes).as_bytes(),
                    length: request.bytes.len() as u64,
                },
                content_type: content_type(request.content_type.clone())?,
                mode: PutMode::PutIfVersion(VersionId(request.expected_version)),
                command_id: Some(command_id.clone()),
                durability: durability(request.durability)?,
            };
            (
                object_link::PUT_THROUGH_LINK_AUTHORITY_KIND,
                command_id,
                object_link::linked_put_fingerprint(requested, &publish),
            )
        }
        Some(Operation::Delete(request)) => {
            let durability = durability(request.durability)?;
            (
                object_link::UNLINK_OBJECT_AUTHORITY_KIND,
                request.command_id.clone(),
                object_link_command_fingerprint(requested, None, durability),
            )
        }
        Some(Operation::DeleteIfVersion(request)) => {
            let durability = durability(request.durability)?;
            (
                object_link::UNLINK_OBJECT_AUTHORITY_KIND,
                request.command_id.clone(),
                object_link::conditional_unlink_fingerprint(
                    requested,
                    VersionId(request.expected_version),
                    durability,
                ),
            )
        }
        None => return Ok(None),
    };
    Ok(Some(AliasReplayProbe {
        lookup: crate::programs::BuiltInReplayLookup {
            original_index: 0,
            authority_kind,
            contract_version: object_link::OBJECT_LINK_CONTRACT_VERSION,
            invocation_id: crate::programs::builtin_invocation_identity(
                authority_kind,
                &command_id,
            ),
            input_fingerprint: fingerprint,
        },
        requested: requested.clone(),
        command_id,
        fingerprint,
    }))
}

pub(super) struct AliasBulkItem {
    pub(super) index: usize,
    pub(super) operation: BulkOperation,
    pub(super) link: ResolvedObjectLink,
}

pub(super) async fn execute(
    service: ObjectServiceImpl,
    caller: Caller,
    bearer: String,
    items: Vec<AliasBulkItem>,
    deadline: tokio::time::Instant,
    meter_public: bool,
) -> Result<Vec<BulkOutcome>, Status> {
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_ALIAS_MUTATIONS));
    let mut tasks = tokio::task::JoinSet::new();
    for (_, items) in group_by_target(items) {
        let service = service.clone();
        let caller = caller.clone();
        let bearer = bearer.clone();
        let permits = permits.clone();
        tasks.spawn(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .map_err(|_| Status::internal("bulk alias dispatcher stopped"))?;
            let mut outcomes = Vec::with_capacity(items.len());
            for item in items {
                let atomic_deadline =
                    atomic_operation_deadline(deadline, service.atomic_program_timeout)?;
                let result = run_request_until(
                    atomic_deadline,
                    execute_one(
                        &service,
                        &caller,
                        &bearer,
                        item.operation,
                        item.link,
                        atomic_deadline,
                        meter_public,
                    ),
                    "bulk alias mutation deadline exceeded",
                )
                .await;
                outcomes.push(BulkOutcome {
                    index: item.index as u32,
                    outcome: Some(match result {
                        Ok(receipt) => Outcome::Receipt(receipt),
                        Err(error) => Outcome::Failure(api_request_failure(error)),
                    }),
                });
            }
            Ok::<_, Status>(outcomes)
        });
    }
    let mut outcomes = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        outcomes.extend(
            joined
                .map_err(|error| Status::internal(format!("bulk alias task failed: {error}")))??,
        );
    }
    Ok(outcomes)
}

fn atomic_operation_deadline(
    request_deadline: tokio::time::Instant,
    atomic_maximum: std::time::Duration,
) -> Result<tokio::time::Instant, Status> {
    tokio::time::Instant::now()
        .checked_add(atomic_maximum)
        .map(|atomic| atomic.min(request_deadline))
        .ok_or_else(|| Status::internal("configured atomic program timeout exceeds clock"))
}

fn group_by_target(items: Vec<AliasBulkItem>) -> BTreeMap<ObjectKey, Vec<AliasBulkItem>> {
    let mut groups = BTreeMap::<ObjectKey, Vec<AliasBulkItem>>::new();
    for item in items {
        groups
            .entry(item.link.target.clone())
            .or_default()
            .push(item);
    }
    groups
}

async fn execute_one(
    service: &ObjectServiceImpl,
    caller: &Caller,
    bearer: &str,
    operation: BulkOperation,
    link: ResolvedObjectLink,
    deadline: tokio::time::Instant,
    meter_public: bool,
) -> Result<ApiMutationReceipt, Status> {
    match batch_operation(operation, service.max_blob_bytes)? {
        BatchOperation::Put(put) => {
            service
                .distribution
                .require_durability_available(put.durability)?;
            let mut upload = service.store.begin_blob_upload().await.map_err(status)?;
            let mut length = 0;
            write_upload_chunk(&mut upload, &mut length, &put.bytes, service.max_blob_bytes)
                .await?;
            let blob = service
                .store
                .seal_blob_upload(upload)
                .await
                .map_err(status)?;
            if meter_public {
                service.record_accounting_inbound(&link.link, length);
            }
            let command_id = put.command_id.clone().ok_or_else(|| {
                Status::invalid_argument("bulk linked Put command ID is required")
            })?;
            let metadata = PutMetadata {
                key: link.target.clone(),
                link: Some(link.clone()),
                content_type: put.content_type.clone(),
                command_id: command_id.clone(),
                durability: put.durability,
                mode: put.mode,
            };
            let upload_token = service.issue_upload_token(caller, &metadata)?;
            let header = require_upload_phase(service.verify_put_token(caller, &upload_token)?)?;
            let ready_token = service.issue_ready_token(caller, header, &blob)?;
            let receipt = object_link::publish_through_link(
                service,
                PublishRequest {
                    key: link.target.clone(),
                    blob,
                    content_type: put.content_type,
                    mode: put.mode,
                    command_id: Some(command_id),
                    durability: put.durability,
                },
                link,
                service.distribution.local_node().0,
                bearer,
                ready_token,
                false,
                deadline,
            )
            .await?;
            Ok(receipt)
        }
        BatchOperation::Delete(delete) => {
            service
                .distribution
                .require_durability_available(delete.durability)?;
            let command_id = delete.command_id.as_deref().ok_or_else(|| {
                Status::invalid_argument("bulk linked Delete command ID is required")
            })?;
            object_link::bulk_delete_through_link(
                service,
                caller,
                link,
                delete.precondition,
                command_id,
                delete.durability,
                bearer,
                deadline,
            )
            .await
        }
        BatchOperation::Publish(_) | BatchOperation::Clone(_) => Err(Status::internal(
            "BulkWrite produced an unsupported alias mutation",
        )),
    }
}

pub(super) fn authorization_key(
    resolution: &object_link::ResolvedAddress,
    requested: &ObjectKey,
) -> ObjectKey {
    match resolution {
        object_link::ResolvedAddress::Ordinary(_) => requested.clone(),
        object_link::ResolvedAddress::Link(link) => link.target.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use keldra_api::v1::bulk_operation::Operation;
    use keldra_api::v1::{BulkPutRequest, Durability, ObjectAddress};
    use keldra_store::VersionId;

    use super::*;

    fn key(path: &str) -> ObjectKey {
        ObjectKey::new("tenant", "bucket", path).unwrap()
    }

    #[test]
    fn mixed_ordinary_and_alias_items_authorize_their_effective_objects() {
        let ordinary = key("ordinary");
        let alias = key("alias");
        let target = key("target");
        assert_eq!(
            authorization_key(
                &object_link::ResolvedAddress::Ordinary(ordinary.clone()),
                &ordinary,
            ),
            ordinary,
        );
        assert_eq!(
            authorization_key(
                &object_link::ResolvedAddress::Link(ResolvedObjectLink {
                    link: alias.clone(),
                    descriptor_version: VersionId(4),
                    target: target.clone(),
                }),
                &alias,
            ),
            target,
        );
    }

    #[test]
    fn distinct_aliases_of_one_target_remain_distinct_items_with_one_auth_target() {
        let target = key("target");
        let aliases = [key("alias-a"), key("alias-b")];
        let resolved = aliases.clone().map(|link| {
            object_link::ResolvedAddress::Link(ResolvedObjectLink {
                link,
                descriptor_version: VersionId(7),
                target: target.clone(),
            })
        });
        assert_ne!(
            match &resolved[0] {
                object_link::ResolvedAddress::Link(link) => &link.link,
                _ => unreachable!(),
            },
            match &resolved[1] {
                object_link::ResolvedAddress::Link(link) => &link.link,
                _ => unreachable!(),
            },
        );
        assert!(
            resolved
                .iter()
                .zip(&aliases)
                .all(|(resolution, requested)| {
                    authorization_key(resolution, requested) == target
                })
        );
    }

    #[test]
    fn same_target_alias_items_share_one_ordered_dispatch_lane() {
        let target = key("target");
        let operation = BulkOperation::default();
        let groups = group_by_target(vec![
            AliasBulkItem {
                index: 8,
                operation: operation.clone(),
                link: ResolvedObjectLink {
                    link: key("alias-a"),
                    descriptor_version: VersionId(3),
                    target: target.clone(),
                },
            },
            AliasBulkItem {
                index: 2,
                operation,
                link: ResolvedObjectLink {
                    link: key("alias-b"),
                    descriptor_version: VersionId(4),
                    target: target.clone(),
                },
            },
        ]);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[&target]
                .iter()
                .map(|item| item.index)
                .collect::<Vec<_>>(),
            vec![8, 2],
        );
    }

    #[tokio::test]
    async fn duplicate_ordinary_paths_share_one_live_resolution() {
        let requested = key("ordinary");
        let calls = AtomicUsize::new(0);
        let mut cache = BTreeMap::new();

        for _ in 0..3 {
            let resolved = resolve_cached(&mut cache, &requested, |key| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(object_link::ResolvedAddress::Ordinary(key))
            })
            .await
            .unwrap();
            assert_eq!(resolved.canonical(), &requested);
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn replay_probe_uses_only_public_requested_put_identity() {
        let requested = key("alias");
        let request = BulkPutRequest {
            address: Some(ObjectAddress {
                tenant: "tenant".into(),
                bucket: "bucket".into(),
                path: "alias".into(),
            }),
            bytes: b"payload".to_vec(),
            content_type: "text/plain".into(),
            command_id: "command".into(),
            durability: Durability::Local as i32,
        };
        let operation = BulkOperation {
            operation: Some(Operation::Put(request.clone())),
        };
        let probe = replay_probe(&operation, &requested).unwrap().unwrap();
        let publish = PublishRequest {
            key: requested.clone(),
            blob: BlobRef {
                hash: *blake3::hash(b"payload").as_bytes(),
                length: 7,
            },
            content_type: Some("text/plain".into()),
            mode: PutMode::Put,
            command_id: Some("command".into()),
            durability: keldra_store::Durability::Local,
        };
        assert_eq!(
            probe.fingerprint,
            object_link::linked_put_fingerprint(&requested, &publish)
        );
        assert_eq!(
            replay_probe(
                &BulkOperation {
                    operation: Some(Operation::PutIfAbsent(request)),
                },
                &requested,
            )
            .unwrap()
            .map(|_| ()),
            None
        );
    }

    #[test]
    fn alias_mutations_retain_the_atomic_maximum_inside_a_long_bulk() {
        let now = tokio::time::Instant::now();
        let short_request = now + std::time::Duration::from_secs(1);
        assert_eq!(
            atomic_operation_deadline(short_request, std::time::Duration::from_secs(30)).unwrap(),
            short_request,
        );

        let long_request = now + std::time::Duration::from_secs(300);
        let bounded =
            atomic_operation_deadline(long_request, std::time::Duration::from_secs(30)).unwrap();
        assert!(bounded < long_request);
        assert!(bounded <= tokio::time::Instant::now() + std::time::Duration::from_secs(30));
    }
}
