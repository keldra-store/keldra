use std::io::Read;

use anvil_store::{ObjectKey, ObjectPathSnapshot, VersionId};

use super::*;
use crate::cluster_object_read::{ClusterObjectReader, ClusterOpenedObject};

pub(super) fn get_object_response(
    selected: Option<ClusterOpenedObject>,
    exact_version: bool,
) -> Result<Response<GetObjectStream>, Status> {
    if selected.is_none() && exact_version {
        return Err(Status::not_found("requested version was not found"));
    }
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    tokio::task::spawn_blocking(move || stream_opened_object(sender, selected));
    Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
}

fn stream_opened_object(
    sender: tokio::sync::mpsc::Sender<Result<ObjectChunk, Status>>,
    selected: Option<ClusterOpenedObject>,
) {
    let (head, payload) = match selected {
        Some(object) => {
            let head = match api_head(&object.version) {
                Ok(head) => head,
                Err(error) => {
                    let _ = sender.blocking_send(Err(error));
                    return;
                }
            };
            (head, object.payload)
        }
        None => (never_existed(), None),
    };
    if sender
        .blocking_send(Ok(ObjectChunk {
            value: Some(ObjectChunkValue::Head(head)),
        }))
        .is_err()
    {
        return;
    }
    let Some(payload) = payload else {
        return;
    };
    let mut spool = payload.into_spool();
    let mut bytes = vec![0_u8; OBJECT_CHUNK_BYTES];
    loop {
        let read = match spool.read(&mut bytes) {
            Ok(0) => return,
            Ok(read) => read,
            Err(error) => {
                let _ = sender.blocking_send(Err(internal(error)));
                return;
            }
        };
        if sender
            .blocking_send(Ok(ObjectChunk {
                value: Some(ObjectChunkValue::Bytes(bytes[..read].to_vec())),
            }))
            .is_err()
        {
            return;
        }
    }
}

pub(super) fn list_object_versions_response(
    snapshot: Option<ObjectPathSnapshot>,
    key: &ObjectKey,
) -> Result<Response<ListObjectVersionsStream>, Status> {
    let versions = match snapshot {
        Some(snapshot) => {
            validate_snapshot(&snapshot, key)?;
            snapshot.versions
        }
        None => Vec::new(),
    };
    let (sender, receiver) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        for version in versions {
            let version = match api_object_version(&version) {
                Ok(version) => version,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };
            if sender.send(Ok(version)).await.is_err() {
                return;
            }
        }
    });
    Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
}

pub(super) fn declared_payload_length(
    snapshot: Option<&ObjectPathSnapshot>,
    key: &ObjectKey,
    requested_version: Option<VersionId>,
) -> Result<u64, Status> {
    let Some(snapshot) = snapshot else {
        return Ok(0);
    };
    validate_snapshot(snapshot, key)?;
    let selected_id = requested_version.unwrap_or(snapshot.head.version);
    let Some(version) = snapshot
        .versions
        .iter()
        .find(|version| version.id == selected_id)
    else {
        return Ok(0);
    };
    match (&version.blob, version.deleted) {
        (Some(blob), false) => Ok(blob.length),
        (None, true) => Ok(0),
        _ => Err(Status::data_loss("version has an invalid payload shape")),
    }
}

pub(super) async fn read_batch_result(
    reader: &ClusterObjectReader,
    key: &ObjectKey,
    requested_version: Option<VersionId>,
) -> Result<(BatchGetResult, u64), Status> {
    let Some(object) = reader.open(key, requested_version).await? else {
        return if requested_version.is_some() {
            Ok((
                BatchGetResult::Failure(ReadFailure {
                    code: ReadFailureCode::VersionNotFound as i32,
                    message: "requested version was not found".into(),
                }),
                0,
            ))
        } else {
            Ok((
                BatchGetResult::Object(BatchGetObject {
                    head: Some(never_existed()),
                    bytes: Vec::new(),
                }),
                0,
            ))
        };
    };
    let head = api_head(&object.version)?;
    let declared_length = object
        .version
        .blob
        .as_ref()
        .map_or(0, |reference| reference.length);
    let bytes = match object.payload {
        Some(payload) => tokio::task::spawn_blocking(move || {
            let mut bytes = Vec::new();
            payload.into_spool().read_to_end(&mut bytes).map(|_| bytes)
        })
        .await
        .map_err(|error| internal(format!("batch payload worker failed: {error}")))?
        .map_err(internal)?,
        None => Vec::new(),
    };
    Ok((
        BatchGetResult::Object(BatchGetObject {
            head: Some(head),
            bytes,
        }),
        declared_length,
    ))
}

pub(super) fn status_failure(error: Status) -> ReadFailure {
    let code = match error.code() {
        tonic::Code::InvalidArgument => ReadFailureCode::Invalid,
        tonic::Code::ResourceExhausted => ReadFailureCode::ResourceLimit,
        tonic::Code::DataLoss => ReadFailureCode::DataLoss,
        _ => ReadFailureCode::Internal,
    };
    ReadFailure {
        code: code as i32,
        message: error.message().to_owned(),
    }
}

fn validate_snapshot(snapshot: &ObjectPathSnapshot, key: &ObjectKey) -> Result<(), Status> {
    snapshot
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if snapshot.exact_path != key.path() {
        return Err(Status::data_loss(
            "object quorum returned another exact path",
        ));
    }
    Ok(())
}
