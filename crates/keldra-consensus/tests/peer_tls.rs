use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use keldra_consensus::{
    ClusterId, CommittedPeerPinProvider, CommittedPeerPins, DEFAULT_PEER_TLS_HANDSHAKE_TIMEOUT,
    NodeId, PeerRpcKind, PeerSpkiSha256, PeerTlsAcceptor, PeerTlsConfig, PeerTlsConnector,
    PeerTlsError, PeerTlsIdentity, authorize_peer_rpc,
};
use rustls::HandshakeKind;
use tokio::net::TcpListener;

const CERT_ONE: &[u8] = include_bytes!("fixtures/peer-one.cert.pem");
const KEY_ONE: &[u8] = include_bytes!("fixtures/peer-one.key.pem");
const CERT_TWO: &[u8] = include_bytes!("fixtures/peer-two.cert.pem");
const KEY_TWO: &[u8] = include_bytes!("fixtures/peer-two.key.pem");
const TEST_CLUSTER_ID: ClusterId = ClusterId([9; 16]);

#[derive(Default)]
struct TestPins {
    entries: RwLock<BTreeMap<NodeId, (CommittedPeerPins, bool)>>,
}

impl TestPins {
    fn set(&self, node_id: NodeId, pins: CommittedPeerPins, authorized: bool) {
        self.entries
            .write()
            .unwrap()
            .insert(node_id, (pins, authorized));
    }
}

impl CommittedPeerPinProvider for TestPins {
    fn connection_pins(&self, node_id: NodeId) -> Option<CommittedPeerPins> {
        self.entries
            .read()
            .unwrap()
            .get(&node_id)
            .map(|(pins, _)| *pins)
    }

    fn authorized_rpc_pins(
        &self,
        cluster_id: ClusterId,
        node_id: NodeId,
        _kind: PeerRpcKind,
    ) -> Option<CommittedPeerPins> {
        if cluster_id != TEST_CLUSTER_ID {
            return None;
        }
        self.entries
            .read()
            .unwrap()
            .get(&node_id)
            .and_then(|(pins, authorized)| authorized.then_some(*pins))
    }
}

fn identity_one() -> Arc<PeerTlsIdentity> {
    Arc::new(PeerTlsIdentity::from_pem(CERT_ONE, KEY_ONE).unwrap())
}

fn identity_two() -> Arc<PeerTlsIdentity> {
    Arc::new(PeerTlsIdentity::from_pem(CERT_TWO, KEY_TWO).unwrap())
}

#[tokio::test]
async fn accepts_mutual_tls_and_exposes_the_client_pin() {
    let first = identity_one();
    let second = identity_two();
    let pins = Arc::new(TestPins::default());
    pins.set(
        NodeId(1),
        CommittedPeerPins {
            current: first.spki_sha256(),
            overlap: None,
        },
        true,
    );
    pins.set(
        NodeId(2),
        CommittedPeerPins {
            current: second.spki_sha256(),
            overlap: None,
        },
        true,
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = PeerTlsAcceptor::new(&second, PeerTlsConfig::default()).unwrap();
    let server_pins = pins.clone();
    let expected_first_pin = first.spki_sha256();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let accepted = acceptor.accept(tcp).await.unwrap();
        assert_eq!(
            accepted.stream.get_ref().1.handshake_kind(),
            Some(HandshakeKind::Full)
        );
        assert_eq!(accepted.presented_spki_sha256, expected_first_pin);
        authorize_peer_rpc(
            server_pins.as_ref(),
            TEST_CLUSTER_ID,
            NodeId(1),
            PeerRpcKind::Vote,
            accepted.presented_spki_sha256,
        )
        .unwrap()
    });

    let connected = PeerTlsConnector::new(first, pins, PeerTlsConfig::default())
        .unwrap()
        .connect(NodeId(2), &address.to_string())
        .await
        .unwrap();
    assert_eq!(connected.presented_spki_sha256, second.spki_sha256());
    assert_eq!(connected.peer_node_id, NodeId(2));
    assert_eq!(
        connected.stream.get_ref().1.handshake_kind(),
        Some(HandshakeKind::Full)
    );
    assert_eq!(server.await.unwrap().node_id, NodeId(1));
}

#[tokio::test]
async fn repeated_connections_do_not_resume_tls_sessions() {
    let first = identity_one();
    let second = identity_two();
    let pins = Arc::new(TestPins::default());
    pins.set(
        NodeId(2),
        CommittedPeerPins {
            current: second.spki_sha256(),
            overlap: None,
        },
        true,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = PeerTlsAcceptor::new(&second, PeerTlsConfig::default()).unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (tcp, _) = listener.accept().await.unwrap();
            let accepted = acceptor.accept(tcp).await.unwrap();
            assert_eq!(
                accepted.stream.get_ref().1.handshake_kind(),
                Some(HandshakeKind::Full)
            );
        }
    });
    let connector = PeerTlsConnector::new(first, pins, PeerTlsConfig::default()).unwrap();
    for _ in 0..2 {
        let connected = connector
            .connect(NodeId(2), &address.to_string())
            .await
            .unwrap();
        assert_eq!(
            connected.stream.get_ref().1.handshake_kind(),
            Some(HandshakeKind::Full)
        );
    }
    server.await.unwrap();
}

#[tokio::test]
async fn connector_rejects_a_server_pin_not_in_committed_state() {
    let first = identity_one();
    let second = identity_two();
    let pins = Arc::new(TestPins::default());
    pins.set(
        NodeId(2),
        CommittedPeerPins {
            current: PeerSpkiSha256([9; 32]),
            overlap: None,
        },
        true,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = PeerTlsAcceptor::new(&second, PeerTlsConfig::default()).unwrap();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        assert!(acceptor.accept(tcp).await.is_err());
    });

    let rejected = PeerTlsConnector::new(first, pins, PeerTlsConfig::default())
        .unwrap()
        .connect(NodeId(2), &address.to_string())
        .await;
    assert!(rejected.is_err());
    server.await.unwrap();
}

#[test]
fn every_rpc_rechecks_state_and_accepts_exactly_one_overlap() {
    let first = identity_one();
    let pins = TestPins::default();
    let node = NodeId(1);
    let retired = PeerSpkiSha256([7; 32]);
    pins.set(
        node,
        CommittedPeerPins {
            current: retired,
            overlap: Some(first.spki_sha256()),
        },
        true,
    );
    assert!(
        authorize_peer_rpc(
            &pins,
            TEST_CLUSTER_ID,
            node,
            PeerRpcKind::AppendEntries,
            first.spki_sha256(),
        )
        .is_ok()
    );

    pins.set(
        node,
        CommittedPeerPins {
            current: retired,
            overlap: Some(first.spki_sha256()),
        },
        false,
    );
    assert!(matches!(
        authorize_peer_rpc(
            &pins,
            TEST_CLUSTER_ID,
            node,
            PeerRpcKind::AppendEntries,
            first.spki_sha256(),
        ),
        Err(PeerTlsError::Unauthorized)
    ));
}

#[test]
fn mismatched_private_key_is_rejected_before_listening() {
    let mismatched = PeerTlsIdentity::from_pem(CERT_ONE, KEY_TWO).unwrap();
    assert!(matches!(
        PeerTlsAcceptor::new(&mismatched, PeerTlsConfig::default()),
        Err(PeerTlsError::Configuration(_))
    ));
}

#[test]
fn identity_rejects_a_certificate_chain() {
    let mut chain = CERT_ONE.to_vec();
    chain.extend_from_slice(CERT_TWO);
    assert!(matches!(
        PeerTlsIdentity::from_pem(&chain, KEY_ONE),
        Err(PeerTlsError::InvalidIdentity(_))
    ));
}

#[test]
fn zero_handshake_timeout_is_rejected_at_startup() {
    assert_eq!(
        DEFAULT_PEER_TLS_HANDSHAKE_TIMEOUT,
        std::time::Duration::from_secs(30)
    );
    let identity = identity_one();
    assert!(matches!(
        PeerTlsAcceptor::new(
            &identity,
            PeerTlsConfig {
                handshake_timeout: std::time::Duration::ZERO,
            },
        ),
        Err(PeerTlsError::Configuration(_))
    ));
}
