//! Public lifecycle and query surface for ordinary-object-backed indexes.

mod boundary;
mod candidate_visibility;
mod definition;
mod listing;
mod pagination;
mod service;

pub(crate) use boundary::{
    ExecuteIndexQuery, ExecutedIndexQuery, IndexAuthorization, IndexAuthorizationEvidence,
    IndexDefinitionLister, IndexDefinitionScan, IndexDefinitionScanPage, IndexLiveVersionReader,
    IndexPageCursor, IndexQueryExecutor, IndexServiceDependencies, ListedIndexDefinition,
};
pub(crate) use candidate_visibility::{
    AuthorizedCurrentCandidates, CandidateVisibilityEvidence, IndexCandidateIdentity,
    IndexCandidateVisibility,
};

pub(crate) use definition::{
    StoredIndexDefinition, definition_name, definition_path, derive_index_id, path_matches_prefix,
    validate_command_id, validate_create_definition, validate_update_definition,
};
pub(crate) use listing::DistributedIndexDefinitionLister;
pub(crate) use pagination::{
    INDEX_PAGE_TOKEN_AUDIENCE, INDEX_PAGE_TOKEN_PURPOSE, IndexPageTokenClaims,
};
pub(crate) use service::IndexServiceImpl;
