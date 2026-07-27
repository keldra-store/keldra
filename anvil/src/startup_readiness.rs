use axum::body::Body;
use axum::http::{Response, StatusCode, header};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub(crate) struct PublicReadiness {
    system_realm_ready: Arc<AtomicBool>,
}

impl PublicReadiness {
    pub(crate) fn new(system_realm_ready: bool) -> Self {
        Self {
            system_realm_ready: Arc::new(AtomicBool::new(system_realm_ready)),
        }
    }

    pub(crate) fn mark_system_realm_ready(&self) {
        self.system_realm_ready.store(true, Ordering::Release);
    }

    pub(crate) fn public_api_ready(&self) -> bool {
        self.system_realm_ready.load(Ordering::Acquire)
    }

    pub(crate) fn cluster_ready(&self) -> bool {
        true
    }

    pub(crate) async fn wait_until_ready(&self) {
        while !self.public_api_ready() {
            tokio::time::sleep(READINESS_POLL_INTERVAL).await;
        }
    }
}

pub(crate) fn is_bootstrap_internal_rpc(path: &str) -> bool {
    [
        "/anvil.BlockStoreInternal/",
        "/anvil.AntiEntropyInternal/",
        "/anvil.CrossRegionProxyInternal/",
        // Consensus must elect a leader before the system realm can become
        // ready. Both services authenticate their long-lived node session
        // before accepting frames, so exposing them here does not expose a
        // public product API during bootstrap.
        "/anvil.ConsensusTransport/",
        "/anvil.ReplicationService/",
    ]
    .iter()
    .any(|prefix| path.starts_with(prefix))
}

pub(crate) fn is_readiness_probe(path: &str) -> bool {
    path == "/ready"
}

pub(crate) fn may_bypass_public_readiness(path: &str, cluster_ready: bool) -> bool {
    is_bootstrap_internal_rpc(path) || (is_readiness_probe(path) && !cluster_ready)
}

pub(crate) fn unavailable_response(grpc: bool) -> Response<Body> {
    if grpc {
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/grpc")
            .header("grpc-status", "14")
            .header("grpc-message", "Anvil startup is not ready")
            .body(Body::empty())
            .expect("static gRPC recovery response is valid")
    } else {
        Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(header::RETRY_AFTER, "1")
            .body(Body::from("Anvil startup is not ready"))
            .expect("static HTTP recovery response is valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_internal_bootstrap_services_bypass_public_readiness() {
        assert!(is_bootstrap_internal_rpc(
            "/anvil.BlockStoreInternal/GetShard"
        ));
        assert!(is_bootstrap_internal_rpc(
            "/anvil.AntiEntropyInternal/ExchangeInventory"
        ));
        assert!(is_bootstrap_internal_rpc(
            "/anvil.ConsensusTransport/Exchange"
        ));
        assert!(is_bootstrap_internal_rpc(
            "/anvil.ReplicationService/Replicate"
        ));
        assert!(!is_bootstrap_internal_rpc(
            "/anvil.RootRegisterInternal/ReadRoot"
        ));
        assert!(!is_bootstrap_internal_rpc(
            "/anvil.CoreMetaReplicationInternal/ExchangeCoreMetaInventory"
        ));
        assert!(!is_bootstrap_internal_rpc("/anvil.ObjectService/GetObject"));
        assert!(!is_bootstrap_internal_rpc(
            "/anvil.InternalProxyService/ProxyNative"
        ));
    }

    #[test]
    fn readiness_probe_remains_observable_while_recovery_is_incomplete() {
        assert!(is_readiness_probe("/ready"));
        assert!(!is_readiness_probe("/health"));
        assert!(!is_readiness_probe("/ready/extra"));
        assert!(may_bypass_public_readiness("/ready", false));
        assert!(!may_bypass_public_readiness("/ready", true));
    }

    #[test]
    fn unavailable_grpc_response_uses_unavailable_status() {
        let response = unavailable_response(true);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["grpc-status"], "14");
    }
}
