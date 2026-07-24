use super::*;
use std::collections::{BTreeMap, BTreeSet};

const GATEWAY_MOUNT_ROUTE_SCHEMA: &str = "anvil.gateway.mount_route.v1";
const ROUTE_KIND_EXACT_HOST: &str = "exact-host";
const ROUTE_KIND_VIRTUAL_HOST: &str = "virtual-host";
const ROUTE_KIND_PATH_STYLE: &str = "path-style";
const MAX_ROUTE_MATCHES: usize = 2;

#[derive(Clone, PartialEq, Message)]
struct GatewayMountRouteRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    route_kind: String,
    #[prost(string, tag = "4")]
    host: String,
    #[prost(string, tag = "5")]
    path_prefix: String,
    #[prost(string, tag = "6")]
    mount_id: String,
    #[prost(uint64, tag = "7")]
    mount_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GatewayMountRoute {
    kind: &'static str,
    host: String,
    path_prefix: String,
    mount_id: String,
}

pub async fn put_gateway_mount_record(
    storage: &Storage,
    mut record: GatewayMountRecord,
    expected_generation: Option<u64>,
) -> Result<u64> {
    let ref_name = gateway_mount_ref_name(&record)?;
    let tuple_key = gateway_metadata_tuple_key(GATEWAY_ROW_MOUNT, &ref_name)?;
    let store = CoreStore::new(storage.clone()).await?;
    let current_payload =
        store.read_coremeta_row(CF_REGISTRY, TABLE_GATEWAY_METADATA_ROW, &tuple_key)?;
    let current = current_payload
        .as_deref()
        .map(|payload| {
            decode_gateway_metadata_row::<GatewayMountRecord>(GATEWAY_ROW_MOUNT, &ref_name, payload)
        })
        .transpose()?;

    match (expected_generation, current.as_ref()) {
        (None, Some(_)) => bail!("gateway mount already exists"),
        (Some(expected), Some(current)) if current.generation == expected => {}
        (Some(_), Some(_)) => bail!("gateway mount generation mismatch"),
        (Some(_), None) => bail!("gateway mount does not exist"),
        (None, None) => {}
    }

    record.record_hash.clear();
    record.generation = current
        .as_ref()
        .map(|current| current.generation.saturating_add(1))
        .unwrap_or(1);
    validate_mount_record_shape(&record)?;
    record.record_hash = hash_record(&record)?;

    let scope_partition = gateway_mount_scope_partition(&ref_name);
    let transaction_id = format!(
        "gateway-mount:{}:{}:{}",
        record.mount_id,
        record.generation,
        record.record_hash.trim_start_matches("sha256:")
    );
    let main_payload =
        encode_gateway_metadata_row(GATEWAY_ROW_MOUNT, &ref_name, record.generation, &record)?;
    let previous_payload_hash = current_payload
        .as_deref()
        .map(|payload| core_meta_payload_digest(TABLE_GATEWAY_METADATA_ROW, payload));

    let old_routes = current
        .as_ref()
        .map(|current| mount_routes(&current.record))
        .transpose()?
        .unwrap_or_default();
    let new_routes = mount_routes(&record)?;
    let old_keys = old_routes
        .iter()
        .map(|route| Ok((mount_route_key(route)?, route)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let new_keys = new_routes
        .iter()
        .map(|route| Ok((mount_route_key(route)?, route)))
        .collect::<Result<BTreeMap<_, _>>>()?;

    let mut operations = Vec::with_capacity(1 + old_keys.len() + new_keys.len());
    operations.push(CoreMutationOperation::CoreMetaPut {
        partition_id: scope_partition.clone(),
        cf: CF_REGISTRY.to_string(),
        table_id: TABLE_GATEWAY_METADATA_ROW,
        tuple_key: tuple_key.clone(),
        payload: main_payload,
    });
    for key in old_keys.keys().filter(|key| !new_keys.contains_key(*key)) {
        operations.push(CoreMutationOperation::CoreMetaDelete {
            partition_id: scope_partition.clone(),
            cf: CF_REGISTRY.to_string(),
            table_id: TABLE_GATEWAY_MOUNT_ROUTE_ROW,
            tuple_key: key.clone(),
        });
    }
    for (key, route) in new_keys {
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: scope_partition.clone(),
            cf: CF_REGISTRY.to_string(),
            table_id: TABLE_GATEWAY_MOUNT_ROUTE_ROW,
            tuple_key: key,
            payload: encode_mount_route_row(&record, &ref_name, route)?,
        });
    }

    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: scope_partition.clone(),
            committed_by_principal: "gateway-mount-registry".to_string(),
            root_publications: vec![crate::core_store::CoreMutationRootPublication {
                root_anchor_key: scope_partition,
                writer_families: vec![
                    crate::formats::writer::WriterFamily::CoreControl
                        .as_str()
                        .to_string(),
                    crate::formats::writer::WriterFamily::Registry
                        .as_str()
                        .to_string(),
                ],
                transaction_coordinator: true,
            }],
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: CF_REGISTRY.to_string(),
                table_id: TABLE_GATEWAY_METADATA_ROW,
                tuple_key,
                expected_payload_hash: previous_payload_hash.clone(),
                require_absent: previous_payload_hash.is_none(),
                require_present: previous_payload_hash.is_some(),
            }],
            operations,
        })
        .await?;
    Ok(record.generation)
}

pub async fn resolve_gateway_mount(
    storage: &Storage,
    host: &str,
    path: &str,
) -> Result<Option<GatewayMountResolution>> {
    let host = normalize_gateway_host(host)?;
    let path = normalize_gateway_path(path)?;
    let store = CoreStore::new(storage.clone()).await?;

    for prefix in exact_path_candidates(&path) {
        if let Some(resolution) = resolve_route(
            storage,
            &store,
            ROUTE_KIND_EXACT_HOST,
            GatewayMountMatchKind::ExactHostAlias,
            &host,
            &prefix,
        )
        .await?
        {
            return Ok(Some(resolution));
        }
    }

    if let Some(resolution) = resolve_route(
        storage,
        &store,
        ROUTE_KIND_VIRTUAL_HOST,
        GatewayMountMatchKind::VirtualHostRegional,
        &host,
        "/",
    )
    .await?
    {
        return Ok(Some(resolution));
    }

    let Some(path_prefix) = path_style_candidate(&path) else {
        return Ok(None);
    };
    resolve_route(
        storage,
        &store,
        ROUTE_KIND_PATH_STYLE,
        GatewayMountMatchKind::PathStyleRegional,
        &host,
        &path_prefix,
    )
    .await
}

async fn resolve_route(
    storage: &Storage,
    store: &CoreStore,
    route_kind: &'static str,
    match_kind: GatewayMountMatchKind,
    host: &str,
    path_prefix: &str,
) -> Result<Option<GatewayMountResolution>> {
    let prefix = mount_route_prefix(route_kind, host, path_prefix)?;
    let rows = store.scan_coremeta_prefix_page(
        CF_REGISTRY,
        TABLE_GATEWAY_MOUNT_ROUTE_ROW,
        &prefix,
        None,
        MAX_ROUTE_MATCHES,
    )?;
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() > 1 {
        bail!(
            "gateway mount route is ambiguous for kind={route_kind} host={host} path_prefix={path_prefix}"
        );
    }
    let row = decode_mount_route_row(&rows[0].payload)?;
    if row.route_kind != route_kind || row.host != host || row.path_prefix != path_prefix {
        bail!("gateway mount route projection scope mismatch");
    }
    let Some((record, handle)) = read_gateway_mount_record(storage, &row.mount_id).await? else {
        bail!("gateway mount route points to a missing mount");
    };
    if handle.generation != row.mount_generation
        || record.generation != row.mount_generation
        || record.state != GatewayMountState::Active
        || !mount_routes(&record)?.contains(&GatewayMountRoute {
            kind: route_kind,
            host: host.to_string(),
            path_prefix: path_prefix.to_string(),
            mount_id: row.mount_id.clone(),
        })
    {
        bail!("gateway mount route projection is stale or inconsistent");
    }
    Ok(Some(GatewayMountResolution {
        record,
        row_generation: handle.generation,
        matched_host: host.to_string(),
        matched_path_prefix: path_prefix.to_string(),
        match_kind,
    }))
}

fn mount_routes(record: &GatewayMountRecord) -> Result<BTreeSet<GatewayMountRoute>> {
    let mut routes = BTreeSet::new();
    if record.state != GatewayMountState::Active {
        return Ok(routes);
    }
    for host in &record.hosts {
        for path_prefix in &record.path_prefixes {
            routes.insert(GatewayMountRoute {
                kind: ROUTE_KIND_EXACT_HOST,
                host: host.clone(),
                path_prefix: path_prefix.clone(),
                mount_id: record.mount_id.clone(),
            });
        }
    }
    routes.insert(GatewayMountRoute {
        kind: ROUTE_KIND_VIRTUAL_HOST,
        host: virtual_host_regional_name(record),
        path_prefix: "/".to_string(),
        mount_id: record.mount_id.clone(),
    });
    routes.insert(GatewayMountRoute {
        kind: ROUTE_KIND_PATH_STYLE,
        host: regional_gateway_host(record),
        path_prefix: path_style_gateway_prefix(record),
        mount_id: record.mount_id.clone(),
    });
    Ok(routes)
}

fn encode_mount_route_row(
    record: &GatewayMountRecord,
    ref_name: &str,
    route: &GatewayMountRoute,
) -> Result<Vec<u8>> {
    let row = GatewayMountRouteRowProto {
        common: Some(core_meta_committed_row_common(
            gateway_realm_id(record),
            gateway_metadata_root_key_hash(GATEWAY_ROW_MOUNT, ref_name),
            GATEWAY_METADATA_CANDIDATE_GENERATION,
            GATEWAY_METADATA_CANDIDATE_TRANSACTION_ID,
            0,
        )),
        schema: GATEWAY_MOUNT_ROUTE_SCHEMA.to_string(),
        route_kind: route.kind.to_string(),
        host: route.host.clone(),
        path_prefix: route.path_prefix.clone(),
        mount_id: route.mount_id.clone(),
        mount_generation: record.generation,
    };
    Ok(encode_deterministic_proto(&row))
}

fn decode_mount_route_row(payload: &[u8]) -> Result<GatewayMountRouteRowProto> {
    let row = decode_deterministic_proto::<GatewayMountRouteRowProto>(
        payload,
        "gateway mount route row",
    )?;
    if row.schema != GATEWAY_MOUNT_ROUTE_SCHEMA
        || row.mount_generation == 0
        || !matches!(
            row.route_kind.as_str(),
            ROUTE_KIND_EXACT_HOST | ROUTE_KIND_VIRTUAL_HOST | ROUTE_KIND_PATH_STYLE
        )
    {
        bail!("gateway mount route row is invalid");
    }
    normalize_gateway_host(&row.host)?;
    validate_gateway_path_prefix(&row.path_prefix)?;
    normalize_gateway_identifier(&row.mount_id, "mount id")?;
    Ok(row)
}

fn gateway_mount_scope_partition(ref_name: &str) -> String {
    format!("gateway/{GATEWAY_ROW_MOUNT}/{ref_name}")
}

fn mount_route_key(route: &GatewayMountRoute) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("gateway-mount-route"),
        CoreMetaTuplePart::Utf8(route.kind),
        CoreMetaTuplePart::Utf8(&route.host),
        CoreMetaTuplePart::Utf8(&route.path_prefix),
        CoreMetaTuplePart::Utf8(&route.mount_id),
    ])
}

fn mount_route_prefix(route_kind: &str, host: &str, path_prefix: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("gateway-mount-route"),
        CoreMetaTuplePart::Utf8(route_kind),
        CoreMetaTuplePart::Utf8(host),
        CoreMetaTuplePart::Utf8(path_prefix),
    ])
}

fn exact_path_candidates(path: &str) -> Vec<String> {
    let mut prefixes = vec!["/".to_string()];
    for (offset, byte) in path.bytes().enumerate().skip(1) {
        if byte == b'/' {
            prefixes.push(path[..=offset].to_string());
        }
    }
    prefixes.sort_by_key(|prefix| std::cmp::Reverse(prefix.len()));
    prefixes.dedup();
    prefixes
}

fn path_style_candidate(path: &str) -> Option<String> {
    let mut segments = path.split('/').filter(|segment| !segment.is_empty());
    let tenant = segments.next()?;
    if segments.next()? != "_gateway" {
        return None;
    }
    let gateway = segments.next()?;
    let registry = segments.next()?;
    Some(format!("/{tenant}/_gateway/{gateway}/{registry}/"))
}

fn virtual_host_regional_name(record: &GatewayMountRecord) -> String {
    format!(
        "{}.{}.{}{}",
        record.registry_instance_id, record.tenant_id, record.region, REGIONAL_GATEWAY_SUFFIX
    )
}

fn regional_gateway_host(record: &GatewayMountRecord) -> String {
    format!("{}{}", record.region, REGIONAL_GATEWAY_SUFFIX)
}

fn path_style_gateway_prefix(record: &GatewayMountRecord) -> String {
    format!(
        "/{}/_gateway/{}/{}/",
        record.tenant_id, record.gateway, record.registry_instance_id
    )
}
