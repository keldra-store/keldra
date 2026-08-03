//! Disposable discovery cache for ordinary index-definition objects.
//!
//! The cache is never authoritative. Its complete contents are replaced after
//! a successful fenced cluster scan; a restart or failed refresh merely makes
//! index discovery temporarily stale until the next scan succeeds.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use tonic::Status;

use crate::index_service::{
    IndexDefinitionLister, IndexDefinitionScan, IndexDefinitionScanPage, ListedIndexDefinition,
    StoredIndexDefinition, definition_path,
};

#[derive(Clone, Debug)]
pub(crate) struct CatalogDefinition {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) object_version: u64,
    pub(crate) stored: StoredIndexDefinition,
    pub(crate) encoded: Vec<u8>,
}

impl CatalogDefinition {
    pub(crate) fn validate(&self) -> Result<(), Status> {
        if self.tenant_id == 0 || self.bucket_id == 0 || self.object_version == 0 {
            return Err(Status::data_loss(
                "index definition cache entry has a zero stable identity",
            ));
        }
        let decoded = StoredIndexDefinition::decode(&self.encoded)?;
        if decoded != self.stored {
            return Err(Status::data_loss(
                "index definition cache bytes differ from their decoded value",
            ));
        }
        let expected = definition_path(&self.stored.name)?;
        if expected != format!("_anvil/indexes/definitions/{}", self.stored.name) {
            return Err(Status::data_loss(
                "index definition cache path is not canonical",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub(crate) struct IndexCatalog {
    inner: Arc<RwLock<BTreeMap<CatalogKey, CatalogDefinition>>>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CatalogKey {
    tenant_id: u64,
    bucket_id: u64,
    name: String,
}

impl IndexCatalog {
    pub(crate) fn replace(&self, definitions: Vec<CatalogDefinition>) -> Result<(), Status> {
        let mut replacement = BTreeMap::new();
        for definition in definitions {
            definition.validate()?;
            let key = CatalogKey {
                tenant_id: definition.tenant_id,
                bucket_id: definition.bucket_id,
                name: definition.stored.name.clone(),
            };
            if replacement.insert(key, definition).is_some() {
                return Err(Status::data_loss(
                    "cluster scan returned a duplicate index definition",
                ));
            }
        }
        *self
            .inner
            .write()
            .map_err(|_| Status::internal("index definition cache lock is poisoned"))? =
            replacement;
        Ok(())
    }

    pub(crate) fn all(&self) -> Result<Vec<CatalogDefinition>, Status> {
        Ok(self
            .inner
            .read()
            .map_err(|_| Status::internal("index definition cache lock is poisoned"))?
            .values()
            .cloned()
            .collect())
    }

    pub(crate) fn get(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        name: &str,
    ) -> Result<Option<CatalogDefinition>, Status> {
        Ok(self
            .inner
            .read()
            .map_err(|_| Status::internal("index definition cache lock is poisoned"))?
            .get(&CatalogKey {
                tenant_id,
                bucket_id,
                name: name.to_owned(),
            })
            .cloned())
    }
}

#[tonic::async_trait]
impl IndexDefinitionLister for IndexCatalog {
    async fn scan(&self, request: IndexDefinitionScan) -> Result<IndexDefinitionScanPage, Status> {
        if request.tenant_id == 0 || request.bucket_id == 0 || request.limit == 0 {
            return Err(Status::invalid_argument(
                "index definition scan identity and limit must be non-zero",
            ));
        }
        let catalog = self
            .inner
            .read()
            .map_err(|_| Status::internal("index definition cache lock is poisoned"))?;
        let mut selected = catalog
            .range(
                CatalogKey {
                    tenant_id: request.tenant_id,
                    bucket_id: request.bucket_id,
                    name: String::new(),
                }..,
            )
            .take_while(|(key, _)| {
                key.tenant_id == request.tenant_id && key.bucket_id == request.bucket_id
            })
            .filter(|(key, value)| {
                value.stored.tenant == request.tenant
                    && value.stored.bucket == request.bucket
                    && request
                        .start_after_name
                        .as_ref()
                        .is_none_or(|after| key.name > *after)
            })
            .take(request.limit.saturating_add(1))
            .map(|(key, value)| ListedIndexDefinition {
                name: key.name.clone(),
                version: value.object_version,
                bytes: value.encoded.clone(),
            })
            .collect::<Vec<_>>();
        let has_more = selected.len() > request.limit;
        selected.truncate(request.limit);
        Ok(IndexDefinitionScanPage {
            definitions: selected,
            has_more,
        })
    }
}

#[cfg(test)]
mod tests {
    use anvil_api::v1::{CreateIndexRequest, IndexSpecification, PathIndexSpec};

    use super::*;

    fn definition(name: &str, version: u64) -> CatalogDefinition {
        let stored = StoredIndexDefinition::create(
            "tenant".into(),
            CreateIndexRequest {
                bucket: "bucket".into(),
                name: name.into(),
                path_prefix: String::new(),
                content_type: String::new(),
                specification: Some(IndexSpecification {
                    specification: Some(anvil_api::v1::index_specification::Specification::Path(
                        PathIndexSpec {},
                    )),
                }),
                command_id: format!("create-{name}"),
            },
            version + 100,
        )
        .unwrap();
        CatalogDefinition {
            tenant_id: 1,
            bucket_id: 2,
            object_version: version,
            encoded: stored.encode().unwrap(),
            stored,
        }
    }

    #[tokio::test]
    async fn listing_is_sorted_unbounded_across_pages() {
        let catalog = IndexCatalog::default();
        catalog
            .replace(vec![
                definition("c", 3),
                definition("a", 1),
                definition("b", 2),
            ])
            .unwrap();
        let page = catalog
            .scan(IndexDefinitionScan {
                tenant: "tenant".into(),
                bucket: "bucket".into(),
                tenant_id: 1,
                bucket_id: 2,
                start_after_name: Some("a".into()),
                limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(page.definitions[0].name, "b");
        assert!(page.has_more);
    }

    #[tokio::test]
    async fn listing_has_a_page_limit_but_no_total_result_ceiling() {
        const TOTAL: usize = 1_205;
        let catalog = IndexCatalog::default();
        catalog
            .replace(
                (0..TOTAL)
                    .map(|index| definition(&format!("index-{index:04}"), index as u64 + 1))
                    .collect(),
            )
            .unwrap();

        let mut after = None;
        let mut names = Vec::new();
        loop {
            let page = catalog
                .scan(IndexDefinitionScan {
                    tenant: "tenant".into(),
                    bucket: "bucket".into(),
                    tenant_id: 1,
                    bucket_id: 2,
                    start_after_name: after.clone(),
                    limit: 137,
                })
                .await
                .unwrap();
            names.extend(
                page.definitions
                    .iter()
                    .map(|definition| definition.name.clone()),
            );
            if !page.has_more {
                break;
            }
            after = page
                .definitions
                .last()
                .map(|definition| definition.name.clone());
        }

        assert_eq!(names.len(), TOTAL);
        assert_eq!(names.first().unwrap(), "index-0000");
        assert_eq!(names.last().unwrap(), "index-1204");
    }
}
