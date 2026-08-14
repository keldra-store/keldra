use std::future::Future;

use crate::{IndexError, IndexFileRead};

use super::codec::materialize_component_payload;
use super::{
    ArtifactDescriptor, ComponentHeader, ComponentKind, SegmentIdentity, decode_component,
};

/// Storage-neutral access to one exact ordinary-object artifact reference.
///
/// Anvil's implementation verifies the referenced object path, version,
/// content hash, and length before opening the descriptor's checked range.
/// The index crate therefore owns codecs and traversal without owning storage,
/// placement, caching, or authority.
pub trait ArtifactDirectoryRead: Send + Sync {
    type File: IndexFileRead;

    fn open_artifact(
        &self,
        descriptor: &ArtifactDescriptor,
    ) -> impl Future<Output = Result<Self::File, IndexError>> + Send;

    /// Execute one finite owned CPU chunk after asynchronous artifact reads
    /// complete. Production implementations route this to Anvil's one
    /// process-owned CPU pool; tests and simple embedders may run inline.
    fn run_query_cpu<T, F>(&self, work: F) -> impl Future<Output = Result<T, IndexError>> + Send
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, IndexError> + Send + 'static,
    {
        async move { work() }
    }
}

/// One fully checked component whose payload is bounded by the format-v4
/// decode ceiling. Owning the payload keeps the async storage handle out of
/// planner and iterator state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedComponent {
    pub header: ComponentHeader,
    pub payload: Vec<u8>,
}

pub async fn read_artifact_component<D: ArtifactDirectoryRead>(
    directory: &D,
    identity: SegmentIdentity,
    descriptor: &ArtifactDescriptor,
    expected_kind: ComponentKind,
) -> Result<LoadedComponent, IndexError> {
    descriptor.validate(identity.index_id)?;
    if descriptor.component_kind != expected_kind {
        return Err(IndexError::InvalidFormat(
            "format-v4 artifact has the wrong component kind",
        ));
    }
    let length =
        usize::try_from(descriptor.encoded_length).map_err(|_| IndexError::OffsetOverflow)?;
    let file = directory.open_artifact(descriptor).await?;
    let bytes = read_exact_at(&file, 0, length).await?;
    let codec_version = descriptor.codec_version;
    let logical_length = descriptor.logical_length;
    let checksum = descriptor.checksum;
    directory
        .run_query_cpu(move || {
            let decoded = decode_component(&bytes, identity, expected_kind, codec_version)?;
            if decoded.header.logical_length != logical_length
                || decoded.header.payload_checksum != checksum
            {
                return Err(IndexError::Integrity);
            }
            let header = decoded.header;
            let payload = materialize_component_payload(header, decoded.payload)?;
            Ok(LoadedComponent { header, payload })
        })
        .await
}

async fn read_exact_at<F: IndexFileRead>(
    file: &F,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, IndexError> {
    let expected_end = offset
        .checked_add(u64::try_from(length).map_err(|_| IndexError::OffsetOverflow)?)
        .ok_or(IndexError::OffsetOverflow)?;
    let mut output = Vec::with_capacity(length);
    let mut cursor = offset;
    while output.len() < length {
        let remaining = length - output.len();
        let slice = file.read_at(cursor, remaining).await?;
        let bytes = slice.as_ref();
        if bytes.is_empty() {
            return Err(IndexError::UnexpectedEof {
                expected: expected_end,
                actual: cursor,
            });
        }
        if bytes.len() > remaining {
            return Err(IndexError::InvalidFormat(
                "invalid file reader slice length",
            ));
        }
        output.extend_from_slice(bytes);
        cursor = cursor
            .checked_add(u64::try_from(bytes.len()).map_err(|_| IndexError::OffsetOverflow)?)
            .ok_or(IndexError::OffsetOverflow)?;
    }
    Ok(output)
}
