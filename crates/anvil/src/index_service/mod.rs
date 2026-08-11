//! Public lifecycle and query surface for ordinary-object-backed indexes.

mod authorized_page;
mod boundary;
mod definition;
mod listing;
mod pagination;
mod service;

pub(crate) use authorized_page::collect_authorized_page;
pub(crate) use boundary::{
    ExecuteIndexQuery, ExecutedIndexQuery, IndexAuthorization, IndexAuthorizationEvidence,
    IndexDefinitionLister, IndexDefinitionReader, IndexDefinitionScan, IndexDefinitionScanPage,
    IndexPageCursor, IndexQueryExecutor, IndexServiceDependencies, ListedIndexDefinition,
};

pub(crate) use definition::{
    StoredIndexDefinition, definition_path, derive_index_id, path_matches_prefix,
    validate_command_id, validate_create_definition, validate_update_definition,
};
pub(crate) use listing::DistributedIndexDefinitionLister;
pub(crate) use pagination::{
    INDEX_PAGE_TOKEN_AUDIENCE, INDEX_PAGE_TOKEN_PURPOSE, IndexPageTokenClaims,
};
pub(crate) use service::IndexServiceImpl;
