use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::Router;

pub(crate) struct HttpGatewayServer {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<()>>,
    completed: bool,
}

impl HttpGatewayServer {
    pub(crate) async fn start(address: SocketAddr, router: Router) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("bind HTTP gateway listener at {address}"))?;
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await
                .context("serve HTTP gateway listener")
        });
        tracing::info!(%address, "Anvil HTTP gateway listening");
        Ok(Self {
            shutdown: Some(shutdown),
            task,
            completed: false,
        })
    }

    pub(crate) fn task_mut(&mut self) -> &mut tokio::task::JoinHandle<Result<()>> {
        &mut self.task
    }

    pub(crate) fn record_completed(&mut self) {
        self.completed = true;
    }

    pub(crate) async fn shutdown(mut self) -> Result<()> {
        if self.completed {
            return Ok(());
        }
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.await.context("join HTTP gateway task")?
    }
}
