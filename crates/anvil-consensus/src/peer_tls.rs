//! Mandatory mutual TLS mechanics for the private Tonic peer protocol.
//!
//! This module does not define an RPC wire format. It accepts or connects one
//! TLS stream which the internal Tonic transport can own. TLS proves private
//! key possession; each Tonic RPC must then call [`authorize_peer_rpc`] with
//! its claimed node ID, RPC kind, and the connection's presented SPKI pin.

use std::fmt;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tonic::transport::server::Connected;

use crate::peer::PeerRpcKind;
use crate::types::{ClusterId, NodeId, PeerSpkiSha256};

const PEER_ALPN: &[u8] = b"anvil-peer/1";
pub const DEFAULT_PEER_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// Startup configuration shared by peer TLS accept and connect paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerTlsConfig {
    pub handshake_timeout: Duration,
}

impl Default for PeerTlsConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: DEFAULT_PEER_TLS_HANDSHAKE_TIMEOUT,
        }
    }
}

impl PeerTlsConfig {
    fn validate(self) -> Result<Self, PeerTlsError> {
        if self.handshake_timeout.is_zero() {
            return Err(PeerTlsError::Configuration(
                "peer TLS handshake timeout must be greater than zero".into(),
            ));
        }
        Ok(self)
    }
}

/// The only two committed public-key pins accepted during key rotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedPeerPins {
    pub current: PeerSpkiSha256,
    pub overlap: Option<PeerSpkiSha256>,
}

impl CommittedPeerPins {
    pub fn contains(self, pin: PeerSpkiSha256) -> bool {
        self.current == pin || self.overlap == Some(pin)
    }
}

/// Reads peer authorization from the latest locally applied committed state.
///
/// Implementations own cluster identity, applied-index, membership-state, and
/// RPC-class policy. `authorized_rpc_pins` must reject a different cluster ID
/// and read those facts and both pins from one committed-state snapshot.
/// Returning `None` denies the RPC.
pub trait CommittedPeerPinProvider: Send + Sync + 'static {
    fn connection_pins(&self, node_id: NodeId) -> Option<CommittedPeerPins>;

    fn authorized_rpc_pins(
        &self,
        cluster_id: ClusterId,
        node_id: NodeId,
        kind: PeerRpcKind,
    ) -> Option<CommittedPeerPins>;
}

/// Identity attached to a Tonic request after its per-RPC committed check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticatedPeer {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub spki_sha256: PeerSpkiSha256,
}

/// Perform the mandatory check for one RPC on an already-authenticated stream.
///
/// Calling this once per connection is insufficient: removal and pin
/// retirement must revoke authority without waiting for that connection to
/// close.
pub fn authorize_peer_rpc(
    provider: &dyn CommittedPeerPinProvider,
    claimed_cluster_id: ClusterId,
    claimed_node_id: NodeId,
    kind: PeerRpcKind,
    presented_spki_sha256: PeerSpkiSha256,
) -> Result<AuthenticatedPeer, PeerTlsError> {
    let authorized = provider
        .authorized_rpc_pins(claimed_cluster_id, claimed_node_id, kind)
        .is_some_and(|pins| pins.contains(presented_spki_sha256));
    if !authorized {
        return Err(PeerTlsError::Unauthorized);
    }
    Ok(AuthenticatedPeer {
        cluster_id: claimed_cluster_id,
        node_id: claimed_node_id,
        spki_sha256: presented_spki_sha256,
    })
}

/// One node's leaf certificate chain and matching private key.
pub struct PeerTlsIdentity {
    certificates: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    spki_sha256: PeerSpkiSha256,
}

impl fmt::Debug for PeerTlsIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerTlsIdentity")
            .field("certificate_count", &self.certificates.len())
            .field("spki_sha256", &self.spki_sha256)
            .field("private_key", &"<redacted>")
            .finish()
    }
}

impl PeerTlsIdentity {
    pub fn from_der(
        certificates: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
    ) -> Result<Self, PeerTlsError> {
        if certificates.len() != 1 {
            return Err(PeerTlsError::InvalidIdentity(
                "peer identity must contain exactly one self-signed leaf certificate".into(),
            ));
        }
        let leaf = &certificates[0];
        let spki_sha256 = peer_spki_sha256(leaf)?;
        Ok(Self {
            certificates,
            private_key,
            spki_sha256,
        })
    }

    pub fn from_pem(certificate_pem: &[u8], private_key_pem: &[u8]) -> Result<Self, PeerTlsError> {
        let certificates = CertificateDer::pem_slice_iter(certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| PeerTlsError::InvalidIdentity(error.to_string()))?;
        let private_key = PrivateKeyDer::from_pem_slice(private_key_pem)
            .map_err(|error| PeerTlsError::InvalidIdentity(error.to_string()))?;
        Self::from_der(certificates, private_key)
    }

    pub fn spki_sha256(&self) -> PeerSpkiSha256 {
        self.spki_sha256
    }
}

#[derive(Debug, Error)]
pub enum PeerTlsError {
    #[error("invalid peer TLS identity: {0}")]
    InvalidIdentity(String),
    #[error("peer TLS configuration failed: {0}")]
    Configuration(String),
    #[error("peer TLS I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("peer TLS protocol failed: {0}")]
    Protocol(String),
    #[error("peer is not authorized by current committed cluster state")]
    Unauthorized,
}

/// SHA-256 over the leaf certificate's DER-encoded SubjectPublicKeyInfo.
pub fn peer_spki_sha256(certificate: &CertificateDer<'_>) -> Result<PeerSpkiSha256, PeerTlsError> {
    let parsed = rustls::server::ParsedCertificate::try_from(certificate)
        .map_err(|error| PeerTlsError::InvalidIdentity(error.to_string()))?;
    let digest = Sha256::digest(parsed.subject_public_key_info().as_ref());
    Ok(PeerSpkiSha256(digest.into()))
}

/// A server-side TLS stream plus the leaf pin Tonic must attach to requests.
pub struct AcceptedPeerTls {
    pub stream: tokio_rustls::server::TlsStream<TcpStream>,
    pub presented_spki_sha256: PeerSpkiSha256,
}

impl Connected for AcceptedPeerTls {
    type ConnectInfo = PeerSpkiSha256;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.presented_spki_sha256
    }
}

impl AsyncRead for AcceptedPeerTls {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for AcceptedPeerTls {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.stream).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(context)
    }
}

/// Mandatory-mTLS acceptor for a Tonic `serve_with_incoming` adapter.
#[derive(Clone)]
pub struct PeerTlsAcceptor {
    inner: TlsAcceptor,
    handshake_timeout: Duration,
}

impl PeerTlsAcceptor {
    pub fn new(identity: &PeerTlsIdentity, config: PeerTlsConfig) -> Result<Self, PeerTlsError> {
        let config = config.validate()?;
        Ok(Self {
            inner: TlsAcceptor::from(Arc::new(server_config(identity)?)),
            handshake_timeout: config.handshake_timeout,
        })
    }

    pub async fn accept(&self, stream: TcpStream) -> Result<AcceptedPeerTls, PeerTlsError> {
        let stream = timed(self.handshake_timeout, self.inner.accept(stream)).await?;
        require_alpn(stream.get_ref().1.alpn_protocol())?;
        let certificate = peer_leaf(stream.get_ref().1.peer_certificates())?;
        let presented_spki_sha256 = peer_spki_sha256(certificate)?;
        Ok(AcceptedPeerTls {
            stream,
            presented_spki_sha256,
        })
    }
}

/// A client-side TLS stream whose server pin was checked after the handshake.
pub struct ConnectedPeerTls {
    pub stream: tokio_rustls::client::TlsStream<TcpStream>,
    pub peer_node_id: NodeId,
    pub presented_spki_sha256: PeerSpkiSha256,
}

/// Mandatory-mTLS connector for a Tonic `connect_with_connector` adapter.
#[derive(Clone)]
pub struct PeerTlsConnector {
    identity: Arc<PeerTlsIdentity>,
    pins: Arc<dyn CommittedPeerPinProvider>,
    handshake_timeout: Duration,
}

impl PeerTlsConnector {
    pub fn new(
        identity: Arc<PeerTlsIdentity>,
        pins: Arc<dyn CommittedPeerPinProvider>,
        config: PeerTlsConfig,
    ) -> Result<Self, PeerTlsError> {
        let config = config.validate()?;
        Ok(Self {
            identity,
            pins,
            handshake_timeout: config.handshake_timeout,
        })
    }

    pub async fn connect(
        &self,
        target: NodeId,
        address: &str,
    ) -> Result<ConnectedPeerTls, PeerTlsError> {
        let config = client_config(target, &self.identity, self.pins.clone())?;
        let stream = timed(self.handshake_timeout, TcpStream::connect(address)).await?;
        let server_name = ServerName::try_from("anvil-peer.invalid")
            .map_err(|error| PeerTlsError::Configuration(error.to_string()))?;
        let stream = timed(
            self.handshake_timeout,
            TlsConnector::from(Arc::new(config)).connect(server_name, stream),
        )
        .await?;
        require_alpn(stream.get_ref().1.alpn_protocol())?;
        let certificate = peer_leaf(stream.get_ref().1.peer_certificates())?;
        let presented_spki_sha256 = peer_spki_sha256(certificate)?;

        let still_committed = self
            .pins
            .connection_pins(target)
            .is_some_and(|pins| pins.contains(presented_spki_sha256));
        if !still_committed {
            return Err(PeerTlsError::Unauthorized);
        }
        Ok(ConnectedPeerTls {
            stream,
            peer_node_id: target,
            presented_spki_sha256,
        })
    }
}

fn server_config(identity: &PeerTlsIdentity) -> Result<ServerConfig, PeerTlsError> {
    let provider = crypto_provider();
    let verifier = Arc::new(PossessionClientVerifier {
        algorithms: provider.signature_verification_algorithms,
    });
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(configuration_error)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            identity.certificates.clone(),
            identity.private_key.clone_key(),
        )
        .map_err(configuration_error)?;
    config.alpn_protocols = vec![PEER_ALPN.to_vec()];
    config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    Ok(config)
}

fn client_config(
    target: NodeId,
    identity: &PeerTlsIdentity,
    pins: Arc<dyn CommittedPeerPinProvider>,
) -> Result<ClientConfig, PeerTlsError> {
    let provider = crypto_provider();
    let verifier = Arc::new(PinnedServerVerifier {
        target,
        pins,
        algorithms: provider.signature_verification_algorithms,
    });
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(configuration_error)?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(
            identity.certificates.clone(),
            identity.private_key.clone_key(),
        )
        .map_err(configuration_error)?;
    config.alpn_protocols = vec![PEER_ALPN.to_vec()];
    config.resumption = rustls::client::Resumption::disabled();
    Ok(config)
}

fn crypto_provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

struct PinnedServerVerifier {
    target: NodeId,
    pins: Arc<dyn CommittedPeerPinProvider>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl fmt::Debug for PinnedServerVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PinnedServerVerifier")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl ServerCertVerifier for PinnedServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let pin = peer_spki_sha256(end_entity).map_err(bad_certificate)?;
        let accepted = self
            .pins
            .connection_pins(self.target)
            .is_some_and(|committed| committed.contains(pin));
        if !accepted {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer,
            ));
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, certificate, signature, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, certificate, signature, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

struct PossessionClientVerifier {
    algorithms: WebPkiSupportedAlgorithms,
}

impl fmt::Debug for PossessionClientVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PossessionClientVerifier")
    }
}

impl ClientCertVerifier for PossessionClientVerifier {
    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        rustls::server::ParsedCertificate::try_from(end_entity)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, certificate, signature, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, certificate, signature, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

async fn timed<T>(
    handshake_timeout: Duration,
    future: impl Future<Output = io::Result<T>>,
) -> Result<T, PeerTlsError> {
    timeout(handshake_timeout, future)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer TLS handshake timed out"))?
        .map_err(PeerTlsError::Io)
}

fn peer_leaf<'a>(
    certificates: Option<&'a [CertificateDer<'static>]>,
) -> Result<&'a CertificateDer<'static>, PeerTlsError> {
    certificates
        .and_then(|chain| chain.first())
        .ok_or_else(|| PeerTlsError::Protocol("peer did not present a certificate".into()))
}

fn require_alpn(protocol: Option<&[u8]>) -> Result<(), PeerTlsError> {
    if protocol == Some(PEER_ALPN) {
        Ok(())
    } else {
        Err(PeerTlsError::Protocol(
            "peer did not negotiate anvil-peer/1".into(),
        ))
    }
}

fn bad_certificate(error: PeerTlsError) -> rustls::Error {
    match error {
        PeerTlsError::InvalidIdentity(_) => {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
        }
        _ => rustls::Error::General(error.to_string()),
    }
}

fn configuration_error(error: rustls::Error) -> PeerTlsError {
    PeerTlsError::Configuration(error.to_string())
}
