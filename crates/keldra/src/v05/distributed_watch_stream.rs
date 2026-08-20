use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use keldra_api::v1::watch_message::Message as WatchMessageValue;
use keldra_api::v1::{WatchCheckpoint, WatchMessage};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

use crate::distributed_list::OriginalBearer;
use crate::distributed_watch::{DistributedWatch, DistributedWatchError, DistributedWatchScope};

use super::api_watch_invalidation;

const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub(super) type ClusterWatchStream =
    Pin<Box<dyn Stream<Item = Result<WatchMessage, Status>> + Send + Sync + 'static>>;

pub(super) enum ClusterWatchStart {
    Now,
    RetainedBeginning,
    Resume(Vec<u8>),
}

pub(super) async fn response(
    watch: Arc<DistributedWatch>,
    scope: DistributedWatchScope,
    bearer: OriginalBearer,
    start: ClusterWatchStart,
) -> Result<Response<ClusterWatchStream>, Status> {
    let (checkpoint, authenticated) = match start {
        ClusterWatchStart::Now => (
            watch
                .start_now(scope.clone(), bearer.clone())
                .await
                .map_err(watch_status)?,
            true,
        ),
        ClusterWatchStart::RetainedBeginning => (
            watch
                .start_retained_beginning(scope.clone(), bearer.clone())
                .await
                .map_err(watch_status)?,
            true,
        ),
        ClusterWatchStart::Resume(checkpoint) => (checkpoint, false),
    };

    let (sender, receiver) = tokio::sync::mpsc::channel(32);
    tokio::spawn(run_watch(
        watch,
        scope,
        bearer,
        checkpoint,
        authenticated,
        sender,
    ));
    Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
}

async fn run_watch(
    watch: Arc<DistributedWatch>,
    scope: DistributedWatchScope,
    bearer: OriginalBearer,
    mut checkpoint: Vec<u8>,
    mut authenticated: bool,
    sender: tokio::sync::mpsc::Sender<Result<WatchMessage, Status>>,
) {
    if authenticated && send_checkpoint(&sender, checkpoint.clone()).await.is_err() {
        return;
    }
    loop {
        let batch = match watch
            .poll_once(scope.clone(), &checkpoint, bearer.clone())
            .await
        {
            Ok(batch) => batch,
            Err(error) => {
                let _ = sender.send(Err(watch_status(error))).await;
                return;
            }
        };
        let had_invalidations = !batch.invalidations.is_empty();
        for invalidation in batch.invalidations {
            if sender
                .send(Ok(WatchMessage {
                    message: Some(WatchMessageValue::Invalidation(api_watch_invalidation(
                        invalidation,
                    ))),
                }))
                .await
                .is_err()
            {
                return;
            }
        }
        let checkpoint_changed = batch.checkpoint != checkpoint;
        checkpoint = batch.checkpoint;
        if (!authenticated || had_invalidations || checkpoint_changed)
            && send_checkpoint(&sender, checkpoint.clone()).await.is_err()
        {
            return;
        }
        authenticated = true;
        if !had_invalidations && !checkpoint_changed {
            tokio::select! {
                () = tokio::time::sleep(IDLE_POLL_INTERVAL) => {}
                () = sender.closed() => return,
            }
        }
    }
}

async fn send_checkpoint(
    sender: &tokio::sync::mpsc::Sender<Result<WatchMessage, Status>>,
    resume_token: Vec<u8>,
) -> Result<(), ()> {
    sender
        .send(Ok(WatchMessage {
            message: Some(WatchMessageValue::Checkpoint(WatchCheckpoint {
                resume_token,
            })),
        }))
        .await
        .map_err(|_| ())
}

fn watch_status(error: DistributedWatchError) -> Status {
    match error {
        DistributedWatchError::InvalidScope(message) => Status::invalid_argument(message),
        DistributedWatchError::InvalidCheckpoint => {
            Status::invalid_argument("invalid watch checkpoint")
        }
        DistributedWatchError::ResumeExpired => Status::failed_precondition("RESUME_EXPIRED"),
        DistributedWatchError::Placement(message) => Status::unavailable(message),
        DistributedWatchError::SourceUnavailable { message, .. }
        | DistributedWatchError::InvalidSource { message, .. } => Status::unavailable(message),
        DistributedWatchError::MembershipChanged => {
            Status::unavailable("watch membership changed while polling")
        }
        DistributedWatchError::CheckpointCodec(message) => Status::internal(message),
    }
}
