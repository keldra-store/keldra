//! One-time, mode-0600 input used to admit an empty node to an existing cluster.
//!
//! A bundle is operator-carried bootstrap material, not an authoritative
//! persistence plane. The joining node consumes it into `node-identity.json`;
//! only the capability hash and peer SPKI pin enter bounded Raft state.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

use keldra_consensus::{
    ClusterId, JoinCapabilityHash, MAX_PEER_ADDRESS_BYTES, NodeId, PeerAddress, PeerSpkiSha256,
    PeerTlsIdentity,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::node_identity::{
    LocalNodeIdentity, PendingJoinIdentity, PendingJoinSeed, PersistedPeerIdentity,
};

const JOIN_BUNDLE_FORMAT_VERSION: u16 = 1;
const JOIN_CAPABILITY_CONTEXT: &str = "keldra.cluster/join-capability/v1";
const MAX_JOIN_BUNDLE_BYTES: u64 = 1024 * 1024;
const MAX_SEEDS: usize = 1_023;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JoinSeed {
    pub(crate) node_id: NodeId,
    pub(crate) peer_address: PeerAddress,
    pub(crate) current_peer_spki_sha256: PeerSpkiSha256,
    pub(crate) overlap_peer_spki_sha256: Option<PeerSpkiSha256>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct JoinBundle {
    pub(crate) cluster_id: ClusterId,
    pub(crate) node_id: NodeId,
    pub(crate) peer_address: PeerAddress,
    pub(crate) storage_weight_millionths: u32,
    pub(crate) peer_identity: PersistedPeerIdentity,
    pub(crate) seeds: Vec<JoinSeed>,
    join_capability: [u8; 32],
}

impl std::fmt::Debug for JoinBundle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JoinBundle")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("peer_address", &self.peer_address)
            .field("storage_weight_millionths", &self.storage_weight_millionths)
            .field("peer_identity", &self.peer_identity)
            .field("seeds", &self.seeds)
            .field("join_capability", &"<redacted>")
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinBundleDocument {
    format_version: u16,
    cluster_id: [u8; 16],
    node_id: u64,
    peer_address: String,
    storage_weight_millionths: u32,
    peer_identity: PersistedPeerIdentity,
    seeds: Vec<JoinSeed>,
    join_capability: [u8; 32],
}

#[derive(Debug, Error)]
pub(crate) enum JoinBundleError {
    #[cfg(not(unix))]
    #[error("join bundles require a Unix host")]
    UnsupportedPlatform,
    #[error("join bundle already exists")]
    AlreadyExists,
    #[error("join bundle conflicts with the requested node: {0}")]
    Conflict(&'static str),
    #[error("join bundle is invalid: {0}")]
    Invalid(&'static str),
    #[error("unsupported join-bundle format version {0}")]
    UnsupportedFormat(u16),
    #[error("join-bundle I/O failed: {0}")]
    Io(#[from] io::Error),
}

impl JoinBundle {
    pub(crate) fn generate(
        cluster_id: ClusterId,
        node_id: NodeId,
        peer_address: PeerAddress,
        storage_weight_millionths: u32,
        seeds: Vec<JoinSeed>,
    ) -> Result<Self, JoinBundleError> {
        let generated = crate::node_identity::generate(cluster_id, node_id)
            .map_err(|_| JoinBundleError::Invalid("peer identity generation failed"))?;
        let mut join_capability = [0_u8; 32];
        getrandom::fill(&mut join_capability)
            .map_err(|_| JoinBundleError::Invalid("join capability generation failed"))?;
        let bundle = Self {
            cluster_id,
            node_id,
            peer_address,
            storage_weight_millionths,
            peer_identity: generated.presented_peer_identity().clone(),
            seeds,
            join_capability,
        };
        validate(&bundle)?;
        Ok(bundle)
    }

    pub(crate) fn capability_hash(&self) -> JoinCapabilityHash {
        hash_capability(self.join_capability)
    }

    pub(crate) fn capability(&self) -> [u8; 32] {
        self.join_capability
    }

    pub(crate) fn seeds(&self) -> &[JoinSeed] {
        &self.seeds
    }

    pub(crate) fn peer_spki_sha256(&self) -> Result<PeerSpkiSha256, JoinBundleError> {
        PeerTlsIdentity::from_pem(
            self.peer_identity.certificate_pem().as_bytes(),
            self.peer_identity.private_key_pem().as_bytes(),
        )
        .map(|identity| identity.spki_sha256())
        .map_err(|_| JoinBundleError::Invalid("peer identity is malformed"))
    }

    pub(crate) fn local_identity(&self) -> Result<LocalNodeIdentity, JoinBundleError> {
        let identity = LocalNodeIdentity::new(
            self.cluster_id,
            self.node_id,
            self.peer_identity.clone(),
            None,
        )
        .map_err(|_| JoinBundleError::Invalid("peer identity is malformed"))?;
        let pending = PendingJoinIdentity::new(
            self.node_id,
            self.peer_address.clone(),
            self.storage_weight_millionths,
            self.seeds
                .iter()
                .cloned()
                .map(|seed| PendingJoinSeed {
                    node_id: seed.node_id,
                    peer_address: seed.peer_address,
                    current_peer_spki_sha256: seed.current_peer_spki_sha256,
                    overlap_peer_spki_sha256: seed.overlap_peer_spki_sha256,
                })
                .collect(),
            self.join_capability,
        )
        .map_err(|_| JoinBundleError::Invalid("pending join material is malformed"))?;
        identity
            .with_pending_join(pending)
            .map_err(|_| JoinBundleError::Invalid("pending join material is malformed"))
    }

    pub(crate) fn ensure_request(
        &self,
        cluster_id: ClusterId,
        node_id: NodeId,
        peer_address: &PeerAddress,
        storage_weight_millionths: u32,
    ) -> Result<(), JoinBundleError> {
        if self.cluster_id != cluster_id {
            return Err(JoinBundleError::Conflict("cluster ID differs"));
        }
        if self.node_id != node_id {
            return Err(JoinBundleError::Conflict("node ID differs"));
        }
        if &self.peer_address != peer_address {
            return Err(JoinBundleError::Conflict("peer address differs"));
        }
        if self.storage_weight_millionths != storage_weight_millionths {
            return Err(JoinBundleError::Conflict("storage weight differs"));
        }
        Ok(())
    }
}

pub(crate) fn hash_capability(capability: [u8; 32]) -> JoinCapabilityHash {
    JoinCapabilityHash(blake3::derive_key(JOIN_CAPABILITY_CONTEXT, &capability))
}

/// Create the deterministic bundle path exactly once, or load the same
/// still-pending preparation after a lost response. Existing input is never
/// replaced and must describe the exact requested stable node fields.
pub(crate) fn create_or_load(
    directory: &Path,
    cluster_id: ClusterId,
    node_id: NodeId,
    peer_address: PeerAddress,
    storage_weight_millionths: u32,
    seeds: Vec<JoinSeed>,
) -> Result<(PathBuf, JoinBundle), JoinBundleError> {
    let path = generated_path(directory, node_id);
    match load_for_request(
        directory,
        cluster_id,
        node_id,
        &peer_address,
        storage_weight_millionths,
    ) {
        Ok(existing) => return Ok(existing),
        Err(JoinBundleError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let generated = JoinBundle::generate(
        cluster_id,
        node_id,
        peer_address.clone(),
        storage_weight_millionths,
        seeds,
    )?;
    match write(&path, &generated) {
        Ok(()) => Ok((path, generated)),
        Err(JoinBundleError::AlreadyExists) => {
            let existing = load(&path)?;
            existing.ensure_request(
                cluster_id,
                node_id,
                &peer_address,
                storage_weight_millionths,
            )?;
            Ok((path, existing))
        }
        Err(error) => Err(error),
    }
}

/// Load a previously prepared bundle without ever regenerating its private
/// identity or single-use capability.
pub(crate) fn load_for_request(
    directory: &Path,
    cluster_id: ClusterId,
    node_id: NodeId,
    peer_address: &PeerAddress,
    storage_weight_millionths: u32,
) -> Result<(PathBuf, JoinBundle), JoinBundleError> {
    let path = generated_path(directory, node_id);
    let existing = load(&path)?;
    existing.ensure_request(cluster_id, node_id, peer_address, storage_weight_millionths)?;
    Ok((path, existing))
}

/// Generate and fsync one bounded replacement before its descriptor pair is
/// proposed to Raft. A retry reuses the exact private material already at the
/// deterministic preparation path.
pub(crate) fn prepare_refresh(
    directory: &Path,
    cluster_id: ClusterId,
    node_id: NodeId,
    peer_address: PeerAddress,
    storage_weight_millionths: u32,
    seeds: Vec<JoinSeed>,
) -> Result<(PathBuf, JoinBundle), JoinBundleError> {
    let path = refresh_path(directory, node_id);
    match load(&path) {
        Ok(existing) => {
            existing.ensure_request(
                cluster_id,
                node_id,
                &peer_address,
                storage_weight_millionths,
            )?;
            return Ok((path, existing));
        }
        Err(JoinBundleError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let generated = JoinBundle::generate(
        cluster_id,
        node_id,
        peer_address.clone(),
        storage_weight_millionths,
        seeds,
    )?;
    match write(&path, &generated) {
        Ok(()) => Ok((path, generated)),
        Err(JoinBundleError::AlreadyExists) => {
            let existing = load(&path)?;
            existing.ensure_request(
                cluster_id,
                node_id,
                &peer_address,
                storage_weight_millionths,
            )?;
            Ok((path, existing))
        }
        Err(error) => Err(error),
    }
}

/// Atomically replace the operator bundle with the already-fsynced refresh.
pub(crate) fn install_refresh(
    directory: &Path,
    node_id: NodeId,
    expected: &JoinBundle,
) -> Result<(PathBuf, JoinBundle), JoinBundleError> {
    let prepared_path = refresh_path(directory, node_id);
    let bundle = match load(&prepared_path) {
        Ok(bundle) => bundle,
        Err(JoinBundleError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            return load_installed_refresh(directory, node_id, expected);
        }
        Err(error) => return Err(error),
    };
    if &bundle != expected {
        return Err(JoinBundleError::Conflict(
            "prepared refresh differs from the committed replacement",
        ));
    }
    let final_path = generated_path(directory, node_id);
    if let Err(error) = fs::rename(&prepared_path, &final_path) {
        if error.kind() == io::ErrorKind::NotFound {
            return load_installed_refresh(directory, node_id, expected);
        }
        return Err(error.into());
    }
    File::open(directory)?.sync_all()?;
    Ok((final_path, bundle))
}

fn load_installed_refresh(
    directory: &Path,
    node_id: NodeId,
    expected: &JoinBundle,
) -> Result<(PathBuf, JoinBundle), JoinBundleError> {
    let final_path = generated_path(directory, node_id);
    let installed = load(&final_path)?;
    if &installed != expected {
        return Err(JoinBundleError::Conflict(
            "installed refresh differs from the committed replacement",
        ));
    }
    Ok((final_path, installed))
}

pub(crate) fn load_refresh(
    directory: &Path,
    node_id: NodeId,
) -> Result<(PathBuf, JoinBundle), JoinBundleError> {
    let path = refresh_path(directory, node_id);
    load(&path).map(|bundle| (path, bundle))
}

pub(crate) fn discard_refresh(directory: &Path, node_id: NodeId) -> Result<(), JoinBundleError> {
    let path = refresh_path(directory, node_id);
    match fs::remove_file(path) {
        Ok(()) => File::open(directory)?.sync_all().map_err(Into::into),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn write(path: &Path, bundle: &JoinBundle) -> Result<(), JoinBundleError> {
    validate(bundle)?;
    let encoded = encode(bundle)?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut file = match create_private(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(JoinBundleError::AlreadyExists);
        }
        Err(error) => return Err(error.into()),
    };
    file.write_all(&encoded)?;
    file.sync_all()?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(crate) fn load(path: &Path) -> Result<JoinBundle, JoinBundleError> {
    decode(&read_private(path)?)
}

/// Persist the stable local identity and remove the operator-carried input.
/// A retry after identity persistence validates the exact same stable IDs and
/// then removes the remaining bundle.
pub(crate) fn consume(data_dir: &Path, bundle_path: &Path) -> Result<JoinBundle, JoinBundleError> {
    let bundle = load(bundle_path)?;
    let identity = bundle.local_identity()?;
    match crate::node_identity::create(data_dir, &identity) {
        Ok(()) => {}
        Err(crate::node_identity::NodeIdentityError::AlreadyExists) => {
            let existing = crate::node_identity::load(data_dir, bundle.cluster_id, bundle.node_id)
                .map_err(|_| JoinBundleError::Invalid("existing node identity differs"))?;
            if existing != identity {
                return Err(JoinBundleError::Invalid("existing node identity differs"));
            }
        }
        Err(_) => {
            return Err(JoinBundleError::Invalid(
                "node identity could not be persisted",
            ));
        }
    }
    fs::remove_file(bundle_path)?;
    let parent = bundle_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(bundle)
}

pub(crate) fn generated_path(directory: &Path, node_id: NodeId) -> PathBuf {
    directory.join(format!("keldra-node-{}.join.json", node_id.0))
}

pub(crate) fn refresh_path(directory: &Path, node_id: NodeId) -> PathBuf {
    directory.join(format!(".keldra-node-{}.join.refresh.tmp", node_id.0))
}

fn validate(bundle: &JoinBundle) -> Result<(), JoinBundleError> {
    if bundle.cluster_id.0 == [0; 16]
        || !(1..=1_023).contains(&bundle.node_id.0)
        || !valid_peer_address(&bundle.peer_address)
        || bundle.storage_weight_millionths == 0
        || bundle.join_capability == [0; 32]
        || bundle.seeds.is_empty()
        || bundle.seeds.len() > MAX_SEEDS
    {
        return Err(JoinBundleError::Invalid("bounded fields are invalid"));
    }
    if bundle.seeds.iter().any(|seed| {
        seed.node_id == bundle.node_id
            || !(1..=1_023).contains(&seed.node_id.0)
            || !valid_peer_address(&seed.peer_address)
            || seed.current_peer_spki_sha256.0 == [0; 32]
            || seed.overlap_peer_spki_sha256 == Some(seed.current_peer_spki_sha256)
            || seed
                .overlap_peer_spki_sha256
                .is_some_and(|pin| pin.0 == [0; 32])
    }) {
        return Err(JoinBundleError::Invalid("seed set is invalid"));
    }
    for (index, seed) in bundle.seeds.iter().enumerate() {
        if bundle.seeds[..index]
            .iter()
            .any(|earlier| earlier.node_id == seed.node_id)
        {
            return Err(JoinBundleError::Invalid("seed node IDs must be unique"));
        }
    }
    bundle.peer_spki_sha256()?;
    Ok(())
}

fn valid_peer_address(address: &PeerAddress) -> bool {
    !address.0.is_empty()
        && address.0.len() <= MAX_PEER_ADDRESS_BYTES
        && !address
            .0
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
}

fn encode(bundle: &JoinBundle) -> Result<Vec<u8>, JoinBundleError> {
    let encoded = serde_json::to_vec(&JoinBundleDocument {
        format_version: JOIN_BUNDLE_FORMAT_VERSION,
        cluster_id: bundle.cluster_id.0,
        node_id: bundle.node_id.0,
        peer_address: bundle.peer_address.0.clone(),
        storage_weight_millionths: bundle.storage_weight_millionths,
        peer_identity: bundle.peer_identity.clone(),
        seeds: bundle.seeds.clone(),
        join_capability: bundle.join_capability,
    })
    .map_err(|_| JoinBundleError::Invalid("JSON encoding failed"))?;
    if encoded.len() as u64 > MAX_JOIN_BUNDLE_BYTES {
        return Err(JoinBundleError::Invalid("file exceeds 1 MiB"));
    }
    Ok(encoded)
}

fn decode(encoded: &[u8]) -> Result<JoinBundle, JoinBundleError> {
    let document: JoinBundleDocument = serde_json::from_slice(encoded)
        .map_err(|_| JoinBundleError::Invalid("JSON is malformed"))?;
    if document.format_version != JOIN_BUNDLE_FORMAT_VERSION {
        return Err(JoinBundleError::UnsupportedFormat(document.format_version));
    }
    let bundle = JoinBundle {
        cluster_id: ClusterId(document.cluster_id),
        node_id: NodeId(document.node_id),
        peer_address: PeerAddress(document.peer_address),
        storage_weight_millionths: document.storage_weight_millionths,
        peer_identity: document.peer_identity,
        seeds: document.seeds,
        join_capability: document.join_capability,
    };
    validate(&bundle)?;
    Ok(bundle)
}

#[cfg(unix)]
fn create_private(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

#[cfg(not(unix))]
fn create_private(_path: &Path) -> io::Result<File> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "Unix required"))
}

#[cfg(unix)]
fn read_private(path: &Path) -> Result<Vec<u8>, JoinBundleError> {
    let linked = fs::symlink_metadata(path)?;
    if !linked.file_type().is_file()
        || linked.file_type().is_symlink()
        || linked.permissions().mode() & 0o7777 != 0o600
        || linked.len() > MAX_JOIN_BUNDLE_BYTES
    {
        return Err(JoinBundleError::Invalid(
            "path must be a bounded mode-0600 regular file",
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
        return Err(JoinBundleError::Invalid("file changed while opening"));
    }
    let mut encoded = Vec::new();
    file.take(MAX_JOIN_BUNDLE_BYTES + 1)
        .read_to_end(&mut encoded)?;
    if encoded.len() as u64 > MAX_JOIN_BUNDLE_BYTES {
        return Err(JoinBundleError::Invalid("file exceeds 1 MiB"));
    }
    Ok(encoded)
}

#[cfg(not(unix))]
fn read_private(_path: &Path) -> Result<Vec<u8>, JoinBundleError> {
    Err(JoinBundleError::UnsupportedPlatform)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed() -> JoinSeed {
        JoinSeed {
            node_id: NodeId(1),
            peer_address: PeerAddress("127.0.0.1:50052".into()),
            current_peer_spki_sha256: PeerSpkiSha256([1; 32]),
            overlap_peer_spki_sha256: None,
        }
    }

    #[test]
    fn round_trip_is_private_and_never_prints_capability_or_key() {
        let directory = tempfile::tempdir().unwrap();
        let bundle = JoinBundle::generate(
            ClusterId([7; 16]),
            NodeId(2),
            PeerAddress("127.0.0.1:50062".into()),
            500_000,
            vec![seed()],
        )
        .unwrap();
        let path = generated_path(directory.path(), NodeId(2));
        write(&path, &bundle).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
            0o600
        );
        assert_eq!(load(&path).unwrap(), bundle);
        let debug = format!("{bundle:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains(&hex::encode(bundle.capability())));
        assert!(!debug.contains("PRIVATE KEY"));
    }

    #[test]
    fn consume_creates_one_identity_and_removes_the_input() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("node");
        fs::create_dir(&data).unwrap();
        let bundle = JoinBundle::generate(
            ClusterId([8; 16]),
            NodeId(3),
            PeerAddress("127.0.0.1:50072".into()),
            1_000_000,
            vec![seed()],
        )
        .unwrap();
        let path = generated_path(directory.path(), NodeId(3));
        write(&path, &bundle).unwrap();
        let consumed = consume(&data, &path).unwrap();
        assert_eq!(consumed, bundle);
        assert!(!path.exists());
        assert_eq!(
            crate::node_identity::load(&data, ClusterId([8; 16]), NodeId(3)).unwrap(),
            bundle.local_identity().unwrap()
        );
        let identity = crate::node_identity::load(&data, ClusterId([8; 16]), NodeId(3)).unwrap();
        let pending = identity.pending_join().unwrap();
        assert_eq!(pending.peer_address(), &bundle.peer_address);
        assert_eq!(pending.capability(), bundle.capability());
        assert!(!format!("{identity:?}").contains(&hex::encode(bundle.capability())));
        crate::node_identity::clear_pending_join(&data, ClusterId([8; 16]), NodeId(3)).unwrap();
        assert!(
            crate::node_identity::load(&data, ClusterId([8; 16]), NodeId(3))
                .unwrap()
                .pending_join()
                .is_none()
        );
    }

    #[test]
    fn capability_hash_is_stable_and_secret_is_not_the_hash() {
        let bundle = JoinBundle::generate(
            ClusterId([9; 16]),
            NodeId(4),
            PeerAddress("127.0.0.1:50082".into()),
            1_000_000,
            vec![seed()],
        )
        .unwrap();
        assert_eq!(bundle.capability_hash(), bundle.capability_hash());
        assert_ne!(bundle.capability_hash().0, bundle.capability());
        assert_ne!(bundle.capability_hash().0, [0; 32]);
    }

    #[test]
    fn exact_create_retry_returns_the_same_private_bundle() {
        let directory = tempfile::tempdir().unwrap();
        let address = PeerAddress("127.0.0.1:50092".into());
        let (path, first) = create_or_load(
            directory.path(),
            ClusterId([10; 16]),
            NodeId(5),
            address.clone(),
            750_000,
            vec![seed()],
        )
        .unwrap();
        let encoded = fs::read(&path).unwrap();

        let (retry_path, retry) = create_or_load(
            directory.path(),
            ClusterId([10; 16]),
            NodeId(5),
            address,
            750_000,
            vec![seed()],
        )
        .unwrap();
        assert_eq!(retry_path, path);
        assert_eq!(retry, first);
        assert_eq!(fs::read(&path).unwrap(), encoded);
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o7777,
            0o600
        );
    }

    #[test]
    fn retry_never_replaces_a_conflicting_bundle() {
        let directory = tempfile::tempdir().unwrap();
        let (path, first) = create_or_load(
            directory.path(),
            ClusterId([11; 16]),
            NodeId(6),
            PeerAddress("127.0.0.1:50102".into()),
            1_000_000,
            vec![seed()],
        )
        .unwrap();
        let encoded = fs::read(&path).unwrap();

        let error = create_or_load(
            directory.path(),
            ClusterId([11; 16]),
            NodeId(6),
            PeerAddress("127.0.0.1:50103".into()),
            1_000_000,
            vec![seed()],
        )
        .unwrap_err();
        assert!(matches!(
            error,
            JoinBundleError::Conflict("peer address differs")
        ));
        assert_eq!(fs::read(path).unwrap(), encoded);
        assert_eq!(
            load(&generated_path(directory.path(), NodeId(6))).unwrap(),
            first
        );
    }
}
