//! Private data-peer wire and admission bounds.

use keldra_store::MAX_OBJECT_RECORD_EXPORT_BYTES;
use tonic::Status;

use super::wire;

pub(super) type ContentStream =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<wire::ContentFrame, Status>> + Send>>;
pub(super) type AuthzRealmStream = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<wire::AuthzRealmFrame, Status>> + Send>,
>;

pub(crate) const DATA_PEER_SCHEMA_VERSION: u32 = 3;
pub(crate) const DATA_PEER_FRAME_BYTES: usize = 64 * 1024;
pub(super) const MAX_TYPED_MUTATION_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_OBJECT_MUTATION_BATCH_ITEMS: usize = 1_000;
pub(crate) const MAX_OBJECT_MUTATION_BATCH_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_OBJECT_SNAPSHOT_BYTES: usize = MAX_OBJECT_RECORD_EXPORT_BYTES as usize;
pub(super) const MAX_DATA_PEER_MESSAGE_BYTES: usize = MAX_OBJECT_SNAPSHOT_BYTES + 1024;
