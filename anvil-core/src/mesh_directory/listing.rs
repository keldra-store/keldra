use super::*;
use crate::core_store::core_meta_record_tuple_key;
use anyhow::anyhow;

const ROUTING_RECORD_PAGE_MAX: usize = 1_000;

#[derive(Debug, Clone)]
pub struct RoutingRecordPage {
    pub records: Vec<RoutingRecordDescriptor>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct BucketLocatorPage {
    pub locators: Vec<BucketLocatorDescriptor>,
    pub next_tuple_key: Option<Vec<u8>>,
}

struct RoutingRecordSourcePage {
    records: Vec<RoutingRecordSource>,
    next_tuple_key: Option<Vec<u8>>,
}

pub async fn page_projected_routing_records(
    storage: &Storage,
    family: RoutingRecordFamily,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> MeshDirectoryResult<RoutingRecordPage> {
    let page =
        page_projected_routing_record_sources(storage, family, after_tuple_key, page_size).await?;
    Ok(RoutingRecordPage {
        records: page
            .records
            .into_iter()
            .map(|source| source.descriptor)
            .collect(),
        next_tuple_key: page.next_tuple_key,
    })
}

pub async fn page_bucket_locators(
    storage: &Storage,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> MeshDirectoryResult<BucketLocatorPage> {
    let page = page_projected_routing_record_sources(
        storage,
        RoutingRecordFamily::BucketLocator,
        after_tuple_key,
        page_size,
    )
    .await?;
    let mut locators = Vec::with_capacity(page.records.len());
    for source in page.records {
        let locator: BucketLocatorDescriptor =
            record_proto::decode_typed_routing_descriptor(&source.payload_proto)?;
        if locator.routing_record_key() != source.descriptor.record_key {
            return Err(MeshDirectoryError::InvalidIdentifier {
                field: "bucket locator record key",
                value: format!(
                    "expected {}, got {}",
                    source.descriptor.record_key,
                    locator.routing_record_key()
                ),
            });
        }
        locators.push(locator);
    }
    Ok(BucketLocatorPage {
        locators,
        next_tuple_key: page.next_tuple_key,
    })
}

async fn page_projected_routing_record_sources(
    storage: &Storage,
    family: RoutingRecordFamily,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> MeshDirectoryResult<RoutingRecordSourcePage> {
    if !(1..=ROUTING_RECORD_PAGE_MAX).contains(&page_size) {
        return Err(anyhow!(
            "routing record page size must be between 1 and {ROUTING_RECORD_PAGE_MAX}"
        )
        .into());
    }
    let prefix = routing_projection_row_prefix(family)?;
    if after_tuple_key
        .is_some_and(|cursor| cursor.len() <= prefix.len() || !cursor.starts_with(&prefix))
    {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field: "routing record page cursor",
            value: "cursor is outside the requested family".to_string(),
        });
    }
    let store = CoreStore::new(storage.clone()).await?;
    let mut rows = store.scan_coremeta_prefix_page(
        CF_MESH,
        TABLE_MESH_PARTITION_ROW,
        &prefix,
        after_tuple_key,
        page_size + 1,
    )?;
    let has_more = rows.len() > page_size;
    if has_more {
        rows.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            core_meta_record_tuple_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("routing record page lost its final row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let projected = record_proto::decode_routing_projection_row(&row.payload)?;
        if projected.descriptor.family != family {
            return Err(MeshDirectoryError::InvalidIdentifier {
                field: "mesh directory projection family",
                value: format!("{:?}", projected.descriptor.family),
            });
        }
        records.push(RoutingRecordSource {
            descriptor: projected.descriptor,
            payload_proto: projected.payload_proto,
        });
    }
    Ok(RoutingRecordSourcePage {
        records,
        next_tuple_key,
    })
}
