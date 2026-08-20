use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Method, Version};
use axum::{Router, ServiceExt as _, extract::Request};
use tower::{ServiceExt as _, service_fn};

const GRPC_CONTENT_TYPE: &[u8] = b"application/grpc";
const GRPC_SERVICE_PATHS: &[&str] = &[
    "/anvil.v1.ObjectService/",
    "/anvil.v1.AuthzService/",
    "/anvil.v1.IndexService/",
    "/anvil.v1.AccountingService/",
    "/anvil.v1.CredentialService/",
    "/anvil.v1.AdministrationService/",
    "/anvil.v1.PersonalDbService/",
];

pub(crate) struct PublicServer {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<()>>,
    completed: bool,
}

impl PublicServer {
    pub(crate) async fn start<F>(
        address: SocketAddr,
        grpc_router: Router,
        gateway_router: Router,
        before_serving: F,
    ) -> Result<Self>
    where
        F: FnOnce(),
    {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("bind public listener at {address}"))?;
        // Nothing can accept from the bound socket until the task below is
        // spawned. This is the exact initialization/serving boundary used by
        // startup evidence.
        before_serving();
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let public = service_fn(move |request: Request| {
            let grpc_router = grpc_router.clone();
            let gateway_router = gateway_router.clone();
            async move {
                if is_grpc_request(&request) {
                    grpc_router.oneshot(request).await
                } else {
                    gateway_router.oneshot(request).await
                }
            }
        });
        let task = tokio::spawn(async move {
            axum::serve(listener, public.into_make_service())
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await
                .context("serve public listener")
        });
        tracing::info!(%address, "Keldra public gRPC and HTTP gateway listener started");
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
        self.task.await.context("join public server task")?
    }
}

fn is_grpc_request(request: &Request) -> bool {
    request.method() == Method::POST
        && request.version() == Version::HTTP_2
        && GRPC_SERVICE_PATHS
            .iter()
            .any(|prefix| request.uri().path().starts_with(prefix))
        && request
            .headers()
            .get(CONTENT_TYPE)
            .is_some_and(|value| is_grpc_content_type(value.as_bytes()))
}

fn is_grpc_content_type(value: &[u8]) -> bool {
    let Some(prefix) = value.get(..GRPC_CONTENT_TYPE.len()) else {
        return false;
    };
    prefix.eq_ignore_ascii_case(GRPC_CONTENT_TYPE)
        && matches!(
            value.get(GRPC_CONTENT_TYPE.len()),
            None | Some(b'+') | Some(b';')
        )
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Method, Request, Version};

    use super::is_grpc_request;

    fn request(method: Method, version: Version, path: &str, content_type: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .version(version)
            .uri(path)
            .header("content-type", content_type)
            .body(Body::empty())
            .unwrap()
    }

    #[test]
    fn grpc_wire_shape_selects_only_registered_services() {
        for content_type in [
            "application/grpc",
            "application/grpc+proto",
            "Application/Grpc; charset=utf-8",
        ] {
            let request = request(
                Method::POST,
                Version::HTTP_2,
                "/anvil.v1.ObjectService/GetObject",
                content_type,
            );
            assert!(is_grpc_request(&request), "{content_type}");
        }

        for content_type in [
            "application/grpcfoo",
            "application/json",
            "application/octet-stream",
            "text/plain",
        ] {
            let request = request(
                Method::POST,
                Version::HTTP_2,
                "/anvil.v1.ObjectService/GetObject",
                content_type,
            );
            assert!(!is_grpc_request(&request), "{content_type}");
        }

        assert!(!is_grpc_request(&request(
            Method::PUT,
            Version::HTTP_2,
            "/anvil.v1.ObjectService/GetObject",
            "application/grpc",
        )));
        assert!(!is_grpc_request(&request(
            Method::POST,
            Version::HTTP_11,
            "/anvil.v1.ObjectService/GetObject",
            "application/grpc",
        )));
        assert!(!is_grpc_request(&request(
            Method::POST,
            Version::HTTP_2,
            "/objects/grpc-payload.bin",
            "application/grpc",
        )));
    }
}
