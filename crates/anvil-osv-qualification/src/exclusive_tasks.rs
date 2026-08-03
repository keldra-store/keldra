use std::future::Future;

use anvil_api::v1::object_service_client::ObjectServiceClient;
use anyhow::{Context, Result};
use tokio::task::{JoinError, JoinSet};
use tonic::transport::{Channel, Endpoint};

pub(super) async fn connect_object_clients(
    endpoints: &[String],
    message_bytes: usize,
) -> Result<Vec<ObjectServiceClient<Channel>>> {
    let mut clients = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let channel = Endpoint::from_shared(endpoint.clone())?
            .connect()
            .await
            .with_context(|| format!("connect to write endpoint {endpoint}"))?;
        clients.push(
            ObjectServiceClient::new(channel)
                .max_encoding_message_size(message_bytes)
                .max_decoding_message_size(message_bytes),
        );
    }
    Ok(clients)
}

/// Runs at most one task for each resource, returning the resource only after
/// its task completes.
pub(super) struct ExclusiveTasks<R, T>
where
    R: Send + 'static,
    T: Send + 'static,
{
    available: Vec<R>,
    active: JoinSet<(R, T)>,
}

impl<R, T> ExclusiveTasks<R, T>
where
    R: Send + 'static,
    T: Send + 'static,
{
    pub(super) fn new(resources: Vec<R>) -> Self {
        Self {
            available: resources,
            active: JoinSet::new(),
        }
    }

    /// Returns an idle resource immediately, or the resource from whichever
    /// active task finishes first along with that task's output.
    pub(super) async fn acquire(&mut self) -> Option<Result<(R, Option<T>), JoinError>> {
        if let Some(resource) = self.available.pop() {
            return Some(Ok((resource, None)));
        }
        self.active
            .join_next()
            .await
            .map(|completed| completed.map(|(resource, output)| (resource, Some(output))))
    }

    pub(super) fn spawn<F>(&mut self, resource: R, task: F)
    where
        F: Future<Output = T> + Send + 'static,
    {
        self.active.spawn(async move { (resource, task.await) });
    }

    pub(super) async fn join_next(&mut self) -> Option<Result<(R, T), JoinError>> {
        self.active.join_next().await
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn reuses_whichever_resource_finishes_first() {
        let mut tasks = ExclusiveTasks::new(vec!["first", "second"]);
        let (first, first_output) = tasks.acquire().await.unwrap().unwrap();
        let (second, second_output) = tasks.acquire().await.unwrap().unwrap();
        assert!(first_output.is_none());
        assert!(second_output.is_none());

        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        tasks.spawn(first, async move {
            first_rx.await.unwrap();
            1
        });
        tasks.spawn(second, async move {
            second_rx.await.unwrap();
            2
        });

        second_tx.send(()).unwrap();
        let (reused, output) = tasks.acquire().await.unwrap().unwrap();
        assert_eq!(reused, second);
        assert_eq!(output, Some(2));

        first_tx.send(()).unwrap();
        let (remaining, output) = tasks.join_next().await.unwrap().unwrap();
        assert_eq!(remaining, first);
        assert_eq!(output, 1);
    }

    #[tokio::test]
    async fn empty_pool_has_no_resource_to_acquire() {
        let mut tasks = ExclusiveTasks::<(), ()>::new(Vec::new());
        assert!(tasks.acquire().await.is_none());
    }
}
