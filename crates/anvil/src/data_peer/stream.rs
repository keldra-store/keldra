use std::time::Duration;

use anvil_store::{BlobReader, BlobRef};
use tonic::{Status, Streaming};

use super::{ContentStream, DATA_PEER_FRAME_BYTES, content_end, content_frame};

pub(super) fn stream_blob(mut reader: BlobReader) -> ContentStream {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    tokio::spawn(async move {
        let mut offset = 0_u64;
        let mut buffer = vec![0_u8; DATA_PEER_FRAME_BYTES];
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => {
                    let _ = sender.send(Ok(content_end(offset))).await;
                    break;
                }
                Ok(read) => {
                    let frame = content_frame(offset, buffer[..read].to_vec());
                    offset += read as u64;
                    if sender.send(Ok(frame)).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(Status::data_loss(error.to_string()))).await;
                    break;
                }
            }
        }
    });
    Box::pin(tokio_stream::wrappers::ReceiverStream::new(receiver))
}

pub(super) async fn next_stream_message<T>(
    stream: &mut Streaming<T>,
    idle: Duration,
    operation: &'static str,
) -> Result<T, Status>
where
    T: prost::Message + Default,
{
    tokio::time::timeout(idle, stream.message())
        .await
        .map_err(|_| Status::deadline_exceeded(format!("{operation} made no progress")))??
        .ok_or_else(|| Status::invalid_argument(format!("{operation} ended without end frame")))
}

pub(super) fn validate_stream_frame(
    expected_offset: u64,
    content: &[u8],
    actual_offset: u64,
    end: bool,
) -> Result<(), Status> {
    if content.len() > DATA_PEER_FRAME_BYTES {
        return Err(Status::resource_exhausted("peer frame exceeds 64 KiB"));
    }
    if actual_offset != expected_offset {
        return Err(Status::invalid_argument(
            "peer frame offset is not contiguous",
        ));
    }
    if content.is_empty() && !end {
        return Err(Status::invalid_argument(
            "an empty peer frame must terminate its stream",
        ));
    }
    Ok(())
}

pub(super) fn require_large_blob(reference: &BlobRef, max_blob_bytes: u64) -> Result<(), Status> {
    if reference.length <= anvil_store::SMALL_BLOB_MAX_BYTES as u64 {
        return Err(Status::invalid_argument(
            "operation requires content larger than 64 KiB",
        ));
    }
    if reference.length > max_blob_bytes {
        return Err(Status::resource_exhausted(
            "content identity exceeds the configured maximum blob size",
        ));
    }
    Ok(())
}
