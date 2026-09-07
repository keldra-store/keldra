use super::*;

pub(super) struct TestPins {
    pub(super) cluster_id: ClusterId,
    nodes: RwLock<BTreeMap<NodeId, (CommittedPeerPins, NodeState)>>,
}

impl TestPins {
    pub(super) fn new(cluster_id: ClusterId) -> Self {
        Self {
            cluster_id,
            nodes: RwLock::new(BTreeMap::new()),
        }
    }

    pub(super) fn install(&self, node_id: NodeId, pin: PeerSpkiSha256, state: NodeState) {
        self.nodes.write().unwrap().insert(
            node_id,
            (
                CommittedPeerPins {
                    current: pin,
                    overlap: None,
                },
                state,
            ),
        );
    }

    pub(super) fn set_state(&self, node_id: NodeId, state: NodeState) {
        self.nodes.write().unwrap().get_mut(&node_id).unwrap().1 = state;
    }

    pub(super) fn remove(&self, node_id: NodeId) {
        self.nodes.write().unwrap().remove(&node_id);
    }
}

impl CommittedPeerPinProvider for TestPins {
    fn connection_pins(&self, node_id: NodeId) -> Option<CommittedPeerPins> {
        self.nodes.read().ok()?.get(&node_id).map(|(pins, _)| *pins)
    }

    fn authorized_rpc_pins(
        &self,
        cluster_id: ClusterId,
        node_id: NodeId,
        kind: PeerRpcKind,
    ) -> Option<CommittedPeerPins> {
        if cluster_id != self.cluster_id {
            return None;
        }
        let nodes = self.nodes.read().ok()?;
        let (pins, state) = nodes.get(&node_id)?;
        let allowed = match kind {
            PeerRpcKind::JoinControl => matches!(state, NodeState::Active | NodeState::Joining),
            _ => *state == NodeState::Active,
        };
        allowed.then_some(*pins)
    }
}

pub(super) fn identity(cluster_id: ClusterId, node_id: NodeId) -> Arc<PeerTlsIdentity> {
    let identity = node_identity::generate(cluster_id, node_id).unwrap();
    let peer = identity.presented_peer_identity();
    Arc::new(
        PeerTlsIdentity::from_pem(
            peer.certificate_pem().as_bytes(),
            peer.private_key_pem().as_bytes(),
        )
        .unwrap(),
    )
}

pub(super) async fn start_server(
    identity: Arc<PeerTlsIdentity>,
    pins: Arc<TestPins>,
    store: Store,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = PeerTlsAcceptor::new(&identity, PeerTlsConfig::default()).unwrap();
    let incoming = TcpIncoming::from(listener)
        .then(move |stream| {
            let acceptor = acceptor.clone();
            async move {
                let stream = stream.map_err(PeerTlsError::Io)?;
                acceptor.accept(stream).await
            }
        })
        .filter_map(|result| result.ok().map(Ok::<_, std::io::Error>));
    let service = DataPeerService::new_test(
        store,
        pins.clone(),
        pins.cluster_id,
        NodeId(1),
        [NodeId(1), NodeId(2)],
        ErasureProfile::default(),
        Duration::from_secs(30),
        16 * 1024 * 1024,
    )
    .unwrap()
    .into_server();
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    (address, shutdown, task)
}

pub(super) async fn collect_content(mut stream: Streaming<wire::ContentFrame>) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(frame) = stream.message().await.unwrap() {
        assert_eq!(frame.schema_version, DATA_PEER_SCHEMA_VERSION);
        assert_eq!(frame.offset, bytes.len() as u64);
        bytes.extend_from_slice(&frame.content);
        if frame.end {
            return bytes;
        }
    }
    panic!("peer content stream ended without an end frame");
}

pub(super) fn shard_frame(
    transport: &DataPeerTransport,
    identity: &ShardIdentity,
    offset: u64,
    content: &[u8],
    end: bool,
) -> wire::ShardPutFrame {
    wire::ShardPutFrame {
        shard: Some(wire_shard(transport.context(), identity)),
        offset,
        content: content.to_vec(),
        end,
    }
}
