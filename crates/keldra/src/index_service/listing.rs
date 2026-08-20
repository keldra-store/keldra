//! Scoped definition-name listing over the ordinary distributed object path.

use keldra_store::INDEX_DEFINITION_PREFIX;
use tonic::Status;

use crate::distributed_list::DistributedObjectLister;

use super::{
    IndexDefinitionLister, IndexDefinitionScan, IndexDefinitionScanPage, ListedIndexDefinition,
    definition_name, definition_path,
};

#[derive(Clone)]
pub(crate) struct DistributedIndexDefinitionLister {
    objects: DistributedObjectLister,
}

impl DistributedIndexDefinitionLister {
    pub(crate) fn new(objects: DistributedObjectLister) -> Self {
        Self { objects }
    }
}

#[tonic::async_trait]
impl IndexDefinitionLister for DistributedIndexDefinitionLister {
    async fn scan(&self, request: IndexDefinitionScan) -> Result<IndexDefinitionScanPage, Status> {
        let start_after = request
            .start_after_name
            .as_deref()
            .map(definition_path)
            .transpose()?;
        let page = self
            .objects
            .list_index_definitions(
                request.bearer,
                &request.tenant,
                &request.bucket,
                request.tenant_id,
                request.bucket_id,
                INDEX_DEFINITION_PREFIX,
                start_after.as_deref(),
                request.limit,
            )
            .await?;
        let definitions = page
            .paths
            .into_iter()
            .map(|path| {
                let name = definition_name(&path).ok_or_else(|| {
                    Status::data_loss("definition listing returned a path outside its scope")
                })?;
                Ok(ListedIndexDefinition {
                    name: name.to_owned(),
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(IndexDefinitionScanPage {
            definitions,
            has_more: page.has_more,
        })
    }
}
