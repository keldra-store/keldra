//! Bounded, private persistence for one node's stable local identity.
//!
//! Startup, join, certificate generation, and rotation orchestration live
//! elsewhere. This module only creates, validates, reads, and atomically
//! replaces `${data_dir}/node-identity.json`.

pub(crate) mod rotation;

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use anvil_consensus::{
    ClusterId, NodeId, PeerSpkiSha256, PeerTlsAcceptor, PeerTlsConfig, PeerTlsIdentity,
};
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub(crate) const NODE_IDENTITY_FILE_NAME: &str = "node-identity.json";
const NODE_IDENTITY_FORMAT_VERSION: u16 = 1;
const MAX_NODE_ID: u64 = 1_023;
const MAX_PEM_BYTES: usize = 64 * 1024;
const MAX_IDENTITY_FILE_BYTES: u64 = 4 * MAX_PEM_BYTES as u64 + 4 * 1024;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PersistedPeerIdentity {
    certificate_pem: String,
    private_key_pem: String,
}

impl fmt::Debug for PersistedPeerIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistedPeerIdentity")
            .field("certificate_pem_bytes", &self.certificate_pem.len())
            .field("private_key_pem", &"<redacted>")
            .finish()
    }
}

impl PersistedPeerIdentity {
    pub(crate) fn new(
        certificate_pem: impl Into<String>,
        private_key_pem: impl Into<String>,
    ) -> Result<Self, NodeIdentityError> {
        let identity = Self {
            certificate_pem: certificate_pem.into(),
            private_key_pem: private_key_pem.into(),
        };
        validate_peer_identity(&identity)?;
        Ok(identity)
    }

    pub(crate) fn certificate_pem(&self) -> &str {
        &self.certificate_pem
    }

    pub(crate) fn private_key_pem(&self) -> &str {
        &self.private_key_pem
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct LocalNodeIdentity {
    cluster_id: ClusterId,
    node_id: NodeId,
    presented_peer_identity: PersistedPeerIdentity,
    overlap_peer_identity: Option<PersistedPeerIdentity>,
}

impl fmt::Debug for LocalNodeIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalNodeIdentity")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("presented_peer_identity", &self.presented_peer_identity)
            .field("overlap_peer_identity", &self.overlap_peer_identity)
            .finish()
    }
}

impl LocalNodeIdentity {
    pub(crate) fn new(
        cluster_id: ClusterId,
        node_id: NodeId,
        presented_peer_identity: PersistedPeerIdentity,
        overlap_peer_identity: Option<PersistedPeerIdentity>,
    ) -> Result<Self, NodeIdentityError> {
        let identity = Self {
            cluster_id,
            node_id,
            presented_peer_identity,
            overlap_peer_identity,
        };
        validate_identity(&identity)?;
        Ok(identity)
    }

    pub(crate) fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) fn presented_peer_identity(&self) -> &PersistedPeerIdentity {
        &self.presented_peer_identity
    }

    pub(crate) fn overlap_peer_identity(&self) -> Option<&PersistedPeerIdentity> {
        self.overlap_peer_identity.as_ref()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityDocument {
    format_version: u16,
    cluster_id: [u8; 16],
    node_id: u64,
    presented_peer_identity: PersistedPeerIdentity,
    overlap_peer_identity: Option<PersistedPeerIdentity>,
}

impl From<&LocalNodeIdentity> for IdentityDocument {
    fn from(identity: &LocalNodeIdentity) -> Self {
        Self {
            format_version: NODE_IDENTITY_FORMAT_VERSION,
            cluster_id: identity.cluster_id.0,
            node_id: identity.node_id.0,
            presented_peer_identity: identity.presented_peer_identity.clone(),
            overlap_peer_identity: identity.overlap_peer_identity.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum NodeIdentityError {
    #[error("local node identity already exists")]
    AlreadyExists,
    #[cfg(not(unix))]
    #[error("local node identity storage requires a Unix host")]
    UnsupportedPlatform,
    #[error("local node identity file is invalid: {0}")]
    Invalid(&'static str),
    #[error("unsupported local node identity format version {0}")]
    UnsupportedFormat(u16),
    #[error("peer identity generation failed: {0}")]
    Generation(String),
    #[error(
        "local node identity mismatch: expected cluster {expected_cluster:?} node {expected_node:?}, found cluster {found_cluster:?} node {found_node:?}"
    )]
    StableIdentityMismatch {
        expected_cluster: ClusterId,
        expected_node: NodeId,
        found_cluster: ClusterId,
        found_node: NodeId,
    },
    #[error("local node identity I/O failed: {0}")]
    Io(#[from] io::Error),
}

pub(crate) fn identity_path(data_dir: &Path) -> PathBuf {
    data_dir.join(NODE_IDENTITY_FILE_NAME)
}

/// Generate the cluster-managed self-signed identity for one admitted node.
pub(crate) fn generate(
    cluster_id: ClusterId,
    node_id: NodeId,
) -> Result<LocalNodeIdentity, NodeIdentityError> {
    if cluster_id.0 == [0; 16] || !(1..=MAX_NODE_ID).contains(&node_id.0) {
        return Err(NodeIdentityError::Invalid(
            "stable cluster and node IDs must be valid before certificate generation",
        ));
    }
    let mut parameters = CertificateParams::new(Vec::<String>::new())
        .map_err(|error| NodeIdentityError::Generation(error.to_string()))?;
    parameters.is_ca = IsCa::NoCa;
    parameters.distinguished_name = DistinguishedName::new();
    parameters.distinguished_name.push(
        DnType::CommonName,
        format!("anvil-peer-{}-{}", hex::encode(cluster_id.0), node_id.0),
    );
    parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    parameters.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let key_pair =
        KeyPair::generate().map_err(|error| NodeIdentityError::Generation(error.to_string()))?;
    let certificate = parameters
        .self_signed(&key_pair)
        .map_err(|error| NodeIdentityError::Generation(error.to_string()))?;
    let peer = PersistedPeerIdentity::new(certificate.pem(), key_pair.serialize_pem())?;
    LocalNodeIdentity::new(cluster_id, node_id, peer, None)
}

/// Create the final identity file directly and never replace an existing path.
pub(crate) fn create(
    data_dir: &Path,
    identity: &LocalNodeIdentity,
) -> Result<(), NodeIdentityError> {
    validate_identity(identity)?;
    let encoded = encode(identity)?;
    let path = identity_path(data_dir);
    let mut file = match create_private_file(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(NodeIdentityError::AlreadyExists);
        }
        Err(error) => return Err(error.into()),
    };
    file.write_all(&encoded)?;
    file.sync_all()?;
    sync_directory(data_dir)?;
    Ok(())
}

/// Load a restart identity and fail closed if its stable IDs differ.
pub(crate) fn load(
    data_dir: &Path,
    expected_cluster: ClusterId,
    expected_node: NodeId,
) -> Result<LocalNodeIdentity, NodeIdentityError> {
    let identity = decode(&read_private_file(&identity_path(data_dir))?)?;
    require_stable_identity(&identity, expected_cluster, expected_node)?;
    Ok(identity)
}

/// Load the stable node identity before the local Raft state is opened.
///
/// The configured node ID is still checked immediately. The caller must then
/// compare the returned cluster ID with the committed Raft cluster identity
/// before exposing either listener.
pub(crate) fn load_for_node(
    data_dir: &Path,
    expected_node: NodeId,
) -> Result<LocalNodeIdentity, NodeIdentityError> {
    let identity = decode(&read_private_file(&identity_path(data_dir))?)?;
    if identity.node_id != expected_node {
        return Err(NodeIdentityError::StableIdentityMismatch {
            expected_cluster: identity.cluster_id,
            expected_node,
            found_cluster: identity.cluster_id,
            found_node: identity.node_id,
        });
    }
    Ok(identity)
}

/// Atomically replace only peer material while preserving stable IDs.
pub(crate) fn replace(
    data_dir: &Path,
    expected_cluster: ClusterId,
    expected_node: NodeId,
    replacement: &LocalNodeIdentity,
) -> Result<(), NodeIdentityError> {
    load(data_dir, expected_cluster, expected_node)?;
    require_stable_identity(replacement, expected_cluster, expected_node)?;
    validate_identity(replacement)?;

    let encoded = encode(replacement)?;
    let final_path = identity_path(data_dir);
    let temporary_path = temporary_path(data_dir);
    let mut temporary = TemporaryIdentity::create(temporary_path)?;
    temporary.file.write_all(&encoded)?;
    temporary.file.sync_all()?;
    fs::rename(&temporary.path, &final_path)?;
    temporary.persisted = true;
    sync_directory(data_dir)?;
    Ok(())
}

fn validate_identity(identity: &LocalNodeIdentity) -> Result<(), NodeIdentityError> {
    if identity.cluster_id.0 == [0; 16] {
        return Err(NodeIdentityError::Invalid("cluster ID must not be zero"));
    }
    if !(1..=MAX_NODE_ID).contains(&identity.node_id.0) {
        return Err(NodeIdentityError::Invalid(
            "node ID must be in the range 1..=1023",
        ));
    }
    let presented_pin = validate_peer_identity(&identity.presented_peer_identity)?;
    if let Some(overlap) = &identity.overlap_peer_identity {
        let overlap_pin = validate_peer_identity(overlap)?;
        if overlap_pin == presented_pin {
            return Err(NodeIdentityError::Invalid(
                "presented and overlap peer identities must use different SPKI pins",
            ));
        }
    }
    Ok(())
}

fn validate_peer_identity(
    identity: &PersistedPeerIdentity,
) -> Result<PeerSpkiSha256, NodeIdentityError> {
    validate_pem_field(&identity.certificate_pem, "certificate PEM")?;
    validate_pem_field(&identity.private_key_pem, "private-key PEM")?;
    let parsed = PeerTlsIdentity::from_pem(
        identity.certificate_pem.as_bytes(),
        identity.private_key_pem.as_bytes(),
    )
    .map_err(|_| NodeIdentityError::Invalid("peer certificate or private-key PEM is invalid"))?;
    PeerTlsAcceptor::new(&parsed, PeerTlsConfig::default()).map_err(|_| {
        NodeIdentityError::Invalid("peer certificate and private key do not form one identity")
    })?;
    Ok(parsed.spki_sha256())
}

fn validate_pem_field(value: &str, name: &'static str) -> Result<(), NodeIdentityError> {
    if value.is_empty() {
        return Err(NodeIdentityError::Invalid(match name {
            "certificate PEM" => "certificate PEM must not be empty",
            _ => "private-key PEM must not be empty",
        }));
    }
    if value.len() > MAX_PEM_BYTES {
        return Err(NodeIdentityError::Invalid(match name {
            "certificate PEM" => "certificate PEM exceeds 64 KiB",
            _ => "private-key PEM exceeds 64 KiB",
        }));
    }
    if !value.is_ascii() {
        return Err(NodeIdentityError::Invalid(match name {
            "certificate PEM" => "certificate PEM must be ASCII",
            _ => "private-key PEM must be ASCII",
        }));
    }
    Ok(())
}

fn encode(identity: &LocalNodeIdentity) -> Result<Vec<u8>, NodeIdentityError> {
    let encoded = serde_json::to_vec(&IdentityDocument::from(identity))
        .map_err(|_| NodeIdentityError::Invalid("identity could not be encoded"))?;
    if encoded.len() as u64 > MAX_IDENTITY_FILE_BYTES {
        return Err(NodeIdentityError::Invalid(
            "identity file exceeds its bound",
        ));
    }
    Ok(encoded)
}

fn decode(encoded: &[u8]) -> Result<LocalNodeIdentity, NodeIdentityError> {
    let document: IdentityDocument = serde_json::from_slice(encoded)
        .map_err(|_| NodeIdentityError::Invalid("identity JSON is malformed"))?;
    if document.format_version != NODE_IDENTITY_FORMAT_VERSION {
        return Err(NodeIdentityError::UnsupportedFormat(
            document.format_version,
        ));
    }
    LocalNodeIdentity::new(
        ClusterId(document.cluster_id),
        NodeId(document.node_id),
        document.presented_peer_identity,
        document.overlap_peer_identity,
    )
}

fn require_stable_identity(
    identity: &LocalNodeIdentity,
    expected_cluster: ClusterId,
    expected_node: NodeId,
) -> Result<(), NodeIdentityError> {
    if identity.cluster_id != expected_cluster || identity.node_id != expected_node {
        return Err(NodeIdentityError::StableIdentityMismatch {
            expected_cluster,
            expected_node,
            found_cluster: identity.cluster_id,
            found_node: identity.node_id,
        });
    }
    Ok(())
}

#[cfg(unix)]
fn create_private_file(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

#[cfg(not(unix))]
fn create_private_file(_path: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        NodeIdentityError::UnsupportedPlatform,
    ))
}

#[cfg(unix)]
fn read_private_file(path: &Path) -> Result<Vec<u8>, NodeIdentityError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let linked = fs::symlink_metadata(path)?;
    if !linked.file_type().is_file() || linked.file_type().is_symlink() {
        return Err(NodeIdentityError::Invalid(
            "path must name a regular non-symlink file",
        ));
    }
    if linked.permissions().mode() & 0o7777 != 0o600 {
        return Err(NodeIdentityError::Invalid("file mode must be exactly 0600"));
    }
    if linked.len() > MAX_IDENTITY_FILE_BYTES {
        return Err(NodeIdentityError::Invalid(
            "identity file exceeds its bound",
        ));
    }

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let opened = file.metadata()?;
    if !opened.file_type().is_file()
        || opened.dev() != linked.dev()
        || opened.ino() != linked.ino()
        || opened.permissions().mode() & 0o7777 != 0o600
    {
        return Err(NodeIdentityError::Invalid(
            "identity file changed while it was opened",
        ));
    }
    let mut encoded = Vec::new();
    file.take(MAX_IDENTITY_FILE_BYTES + 1)
        .read_to_end(&mut encoded)?;
    if encoded.len() as u64 > MAX_IDENTITY_FILE_BYTES {
        return Err(NodeIdentityError::Invalid(
            "identity file exceeds its bound",
        ));
    }
    Ok(encoded)
}

#[cfg(not(unix))]
fn read_private_file(_path: &Path) -> Result<Vec<u8>, NodeIdentityError> {
    Err(NodeIdentityError::UnsupportedPlatform)
}

fn sync_directory(data_dir: &Path) -> Result<(), NodeIdentityError> {
    File::open(data_dir)?.sync_all()?;
    Ok(())
}

fn temporary_path(data_dir: &Path) -> PathBuf {
    data_dir.join(format!(
        ".{NODE_IDENTITY_FILE_NAME}.{}.tmp",
        Uuid::new_v4().simple()
    ))
}

struct TemporaryIdentity {
    path: PathBuf,
    file: File,
    persisted: bool,
}

impl TemporaryIdentity {
    fn create(path: PathBuf) -> Result<Self, NodeIdentityError> {
        let file = create_private_file(&path)?;
        Ok(Self {
            path,
            file,
            persisted: false,
        })
    }
}

impl Drop for TemporaryIdentity {
    fn drop(&mut self) {
        if !self.persisted {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CERT_ONE: &str = "-----BEGIN CERTIFICATE-----\nMIIBiDCCAS+gAwIBAgIUBpJAQ4cuYCJY3MUr3nrYZNGndFIwCgYIKoZIzj0EAwIw\nGTEXMBUGA1UEAwwOYW52aWwtcGVlci1vbmUwIBcNMjYwODAyMTc1ODAxWhgPMjEy\nNjA3MDkxNzU4MDFaMBkxFzAVBgNVBAMMDmFudmlsLXBlZXItb25lMFkwEwYHKoZI\nzj0CAQYIKoZIzj0DAQcDQgAEMigBrGBgRsaeRVKksJstx5sRCoGReN/oqwivQp5S\nM9WKp+IOG4xjnTxJ20px1RMoLNbD9OcfMFgi/rW0EKSEr6NTMFEwHQYDVR0OBBYE\nFKsE3oFrKlZbya3xw8tE2sdCyHd8MB8GA1UdIwQYMBaAFKsE3oFrKlZbya3xw8tE\n2sdCyHd8MA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDRwAwRAIgM90XVQOd\n8WbLrStdJaLcQEvCUbA09VhmnPtYE+2sHIwCIA/GyPPbWXoTKls9ftUrWdfvLzAf\nGJN03xvysAiHhk9S\n-----END CERTIFICATE-----\n";
    const KEY_ONE: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgIDVYsQiKeb+5igf2\nFWZoJxHoYIU35IJeWhaOYs7b8cGhRANCAAQyKAGsYGBGxp5FUqSwmy3HmxEKgZF4\n3+irCK9CnlIz1Yqn4g4bjGOdPEnbSnHVEygs1sP05x8wWCL+tbQQpISv\n-----END PRIVATE KEY-----\n";
    const CERT_TWO: &str = "-----BEGIN CERTIFICATE-----\nMIIBiDCCAS+gAwIBAgIUf5n6LrQEla3eKawcyn3sQOQ7QkswCgYIKoZIzj0EAwIw\nGTEXMBUGA1UEAwwOYW52aWwtcGVlci10d28wIBcNMjYwODAyMTc1ODAxWhgPMjEy\nNjA3MDkxNzU4MDFaMBkxFzAVBgNVBAMMDmFudmlsLXBlZXItdHdvMFkwEwYHKoZI\nzj0CAQYIKoZIzj0DAQcDQgAE93y1cLO+GxaXebXlyDlH4XrXK18VXiHr2x0jNrwE\nWA2BNo/KcAJlja4wH86uBViH1YXPjX2S+ouDq9/c6fNOqqNTMFEwHQYDVR0OBBYE\nFCjps4Bo9leCpW75eic8Xfg4FqdkMB8GA1UdIwQYMBaAFCjps4Bo9leCpW75eic8\nXfg4FqdkMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDRwAwRAIgY0qPNVtW\nuAu09bg/59VhvfoGAi9cltsqsmcubQoOz2oCICiOvfJpde/NOt7EI/SFDgv+2+TF\ntGXw1iSrAveGPYpF\n-----END CERTIFICATE-----\n";
    const KEY_TWO: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg8USeg2m+n4dza2J9\nFYK0tWK9mU+EGFbKroOxJxJRs6ShRANCAAT3fLVws74bFpd5teXIOUfhetcrXxVe\nIevbHSM2vARYDYE2j8pwAmWNrjAfzq4FWIfVhc+NfZL6i4Or39zp806q\n-----END PRIVATE KEY-----\n";

    fn peer_one() -> PersistedPeerIdentity {
        PersistedPeerIdentity::new(CERT_ONE, KEY_ONE).unwrap()
    }

    fn peer_two() -> PersistedPeerIdentity {
        PersistedPeerIdentity::new(CERT_TWO, KEY_TWO).unwrap()
    }

    fn identity(peer: PersistedPeerIdentity) -> LocalNodeIdentity {
        LocalNodeIdentity::new(ClusterId([7; 16]), NodeId(17), peer, None).unwrap()
    }

    #[cfg(unix)]
    fn write_private(path: &Path, bytes: &[u8]) {
        use std::os::unix::fs::PermissionsExt;

        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn create_is_private_and_restart_reads_the_exact_identity() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let expected = identity(peer_one());
        create(directory.path(), &expected).unwrap();
        assert_eq!(
            fs::metadata(identity_path(directory.path()))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
        let restarted = load(directory.path(), ClusterId([7; 16]), NodeId(17)).unwrap();
        assert_eq!(restarted, expected);
        assert_eq!(
            restarted.presented_peer_identity().certificate_pem(),
            CERT_ONE
        );
        assert!(restarted.presented_peer_identity().private_key_pem() == KEY_ONE);
    }

    #[cfg(unix)]
    #[test]
    fn startup_load_checks_node_before_raft_supplies_the_cluster_id() {
        let directory = tempfile::tempdir().unwrap();
        let expected = identity(peer_one());
        create(directory.path(), &expected).unwrap();

        assert_eq!(
            load_for_node(directory.path(), NodeId(17)).unwrap(),
            expected
        );
        assert!(matches!(
            load_for_node(directory.path(), NodeId(18)),
            Err(NodeIdentityError::StableIdentityMismatch {
                expected_node: NodeId(18),
                found_node: NodeId(17),
                ..
            })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn create_never_replaces_an_existing_identity() {
        let directory = tempfile::tempdir().unwrap();
        let first = identity(peer_one());
        create(directory.path(), &first).unwrap();
        assert!(matches!(
            create(directory.path(), &identity(peer_two())),
            Err(NodeIdentityError::AlreadyExists)
        ));
        assert_eq!(
            load(directory.path(), ClusterId([7; 16]), NodeId(17)).unwrap(),
            first
        );
    }

    #[cfg(unix)]
    #[test]
    fn replace_atomically_changes_peer_material_and_preserves_stable_ids() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        create(directory.path(), &identity(peer_one())).unwrap();
        let replacement =
            LocalNodeIdentity::new(ClusterId([7; 16]), NodeId(17), peer_two(), Some(peer_one()))
                .unwrap();
        replace(
            directory.path(),
            ClusterId([7; 16]),
            NodeId(17),
            &replacement,
        )
        .unwrap();
        let reopened = load(directory.path(), ClusterId([7; 16]), NodeId(17)).unwrap();
        assert_eq!(reopened, replacement);
        assert!(reopened.overlap_peer_identity().is_some());
        assert_eq!(reopened.cluster_id(), ClusterId([7; 16]));
        assert_eq!(reopened.node_id(), NodeId(17));
        assert_eq!(
            fs::metadata(identity_path(directory.path()))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
        assert!(fs::read_dir(directory.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
    }

    #[cfg(unix)]
    #[test]
    fn stable_identity_mismatch_fails_without_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let original = identity(peer_one());
        create(directory.path(), &original).unwrap();
        assert!(matches!(
            load(directory.path(), ClusterId([8; 16]), NodeId(17)),
            Err(NodeIdentityError::StableIdentityMismatch { .. })
        ));
        let wrong_node =
            LocalNodeIdentity::new(ClusterId([7; 16]), NodeId(18), peer_two(), None).unwrap();
        assert!(matches!(
            replace(
                directory.path(),
                ClusterId([7; 16]),
                NodeId(17),
                &wrong_node
            ),
            Err(NodeIdentityError::StableIdentityMismatch { .. })
        ));
        assert_eq!(
            load(directory.path(), ClusterId([7; 16]), NodeId(17)).unwrap(),
            original
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinks_non_regular_files_and_wrong_mode() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target.json");
        write_private(&target, &encode(&identity(peer_one())).unwrap());
        symlink(&target, identity_path(directory.path())).unwrap();
        assert!(matches!(
            load(directory.path(), ClusterId([7; 16]), NodeId(17)),
            Err(NodeIdentityError::Invalid(_))
        ));
        fs::remove_file(identity_path(directory.path())).unwrap();
        fs::create_dir(identity_path(directory.path())).unwrap();
        assert!(matches!(
            load(directory.path(), ClusterId([7; 16]), NodeId(17)),
            Err(NodeIdentityError::Invalid(_))
        ));
        fs::remove_dir(identity_path(directory.path())).unwrap();
        write_private(
            &identity_path(directory.path()),
            &encode(&identity(peer_one())).unwrap(),
        );
        fs::set_permissions(
            identity_path(directory.path()),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();
        assert!(matches!(
            load(directory.path(), ClusterId([7; 16]), NodeId(17)),
            Err(NodeIdentityError::Invalid(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_oversize_malformed_unknown_and_future_documents() {
        let malformed_cases = [
            b"not-json".as_slice(),
            br#"{"format_version":1,"unexpected":true}"#,
            br#"{"format_version":2,"cluster_id":[7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7],"node_id":17,"presented_peer_identity":{"certificate_pem":"x","private_key_pem":"y"},"overlap_peer_identity":null}"#,
        ];
        for malformed in malformed_cases {
            let directory = tempfile::tempdir().unwrap();
            write_private(&identity_path(directory.path()), malformed);
            assert!(load(directory.path(), ClusterId([7; 16]), NodeId(17)).is_err());
        }

        let directory = tempfile::tempdir().unwrap();
        write_private(
            &identity_path(directory.path()),
            &vec![b'x'; MAX_IDENTITY_FILE_BYTES as usize + 1],
        );
        assert!(matches!(
            load(directory.path(), ClusterId([7; 16]), NodeId(17)),
            Err(NodeIdentityError::Invalid(_))
        ));
    }

    #[test]
    fn invalid_identity_inputs_and_debug_output_never_expose_keys() {
        assert!(PersistedPeerIdentity::new(CERT_ONE, KEY_TWO).is_err());
        assert!(LocalNodeIdentity::new(ClusterId([0; 16]), NodeId(17), peer_one(), None).is_err());
        assert!(LocalNodeIdentity::new(ClusterId([7; 16]), NodeId(0), peer_one(), None).is_err());
        assert!(
            LocalNodeIdentity::new(ClusterId([7; 16]), NodeId(17), peer_one(), Some(peer_one()))
                .is_err()
        );
        let rendered = format!("{:?}", identity(peer_one()));
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("MIGHAgEA"));
    }

    #[test]
    fn generated_cluster_identity_is_a_valid_distinct_keypair() {
        let first = generate(ClusterId([3; 16]), NodeId(9)).unwrap();
        let second = generate(ClusterId([3; 16]), NodeId(10)).unwrap();

        assert_eq!(first.cluster_id(), ClusterId([3; 16]));
        assert_eq!(first.node_id(), NodeId(9));
        assert!(first.overlap_peer_identity().is_none());
        assert_ne!(
            validate_peer_identity(first.presented_peer_identity()).unwrap(),
            validate_peer_identity(second.presented_peer_identity()).unwrap(),
        );
    }
}
