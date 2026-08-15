//! Explicit storage roles and durable node-local root binding.
//!
//! Authoritative roots are identified by durable markers rather than their
//! textual mount paths. This prevents a typo or missing mount from silently
//! creating an empty database while still allowing the same filesystem to be
//! mounted at a different path.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

const FORMAT_VERSION: u8 = 1;
const LAYOUT_FILE: &str = "storage-layout-v1.json";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoragePaths {
    pub state: PathBuf,
    pub metadata: PathBuf,
    pub metadata_wal: PathBuf,
    pub payload: PathBuf,
    pub scratch: PathBuf,
    pub cache: PathBuf,
    pub upload_spool: PathBuf,
    pub upload_spool_max_bytes: u64,
}

impl StoragePaths {
    pub fn under(root: impl AsRef<Path>, upload_spool_max_bytes: u64) -> Self {
        let root = root.as_ref();
        Self {
            state: root.to_path_buf(),
            metadata: root.join("metadata"),
            metadata_wal: root.join("metadata"),
            payload: root.join("blobs"),
            scratch: root.join("index-scratch"),
            cache: root.join("cache"),
            upload_spool: root.join("upload-spool"),
            upload_spool_max_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExplicitAuthoritativePaths {
    pub state: bool,
    pub metadata: bool,
    pub metadata_wal: bool,
    pub payload: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StorageBinding {
    pub(crate) newly_initialized: bool,
    pub(crate) installation_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum StorageRole {
    State,
    Metadata,
    MetadataWal,
    Payload,
}

impl StorageRole {
    const ALL: [Self; 4] = [
        Self::State,
        Self::Metadata,
        Self::MetadataWal,
        Self::Payload,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::State => "state",
            Self::Metadata => "metadata",
            Self::MetadataWal => "metadata_wal",
            Self::Payload => "payload",
        }
    }

    fn marker_name(self) -> String {
        format!(".anvil-{}-root-v1.json", self.name())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct RootMarker {
    format_version: u8,
    installation_id: String,
    node_id: u16,
    role: String,
    root_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct LayoutManifest {
    format_version: u8,
    installation_id: String,
    node_id: u16,
    roots: BTreeMap<String, String>,
}

pub(crate) fn bind_authoritative_roots(
    paths: &StoragePaths,
    node_id: u16,
) -> Result<StorageBinding> {
    ensure!(
        paths.upload_spool_max_bytes > 0,
        "upload spool maximum must be non-zero"
    );
    for role in StorageRole::ALL {
        fs::create_dir_all(role_path(paths, role))
            .with_context(|| format!("create {} storage root", role.name()))?;
    }
    fs::create_dir_all(&paths.scratch).context("create disposable scratch root")?;
    fs::create_dir_all(&paths.cache).context("create disposable cache root")?;
    fs::create_dir_all(&paths.upload_spool).context("create disposable upload spool root")?;

    let layout_path = paths.state.join(LAYOUT_FILE);
    if layout_path.exists() {
        let manifest: LayoutManifest = read_json(&layout_path)?;
        validate_manifest(&manifest, node_id)?;
        for role in StorageRole::ALL {
            let marker = read_marker(paths, role)?;
            validate_marker(&marker, role, node_id, &manifest.installation_id)?;
            ensure!(
                manifest.roots.get(role.name()) == Some(&marker.root_id),
                "{} storage root does not match the node's pinned layout",
                role.name()
            );
        }
        return Ok(StorageBinding {
            newly_initialized: false,
            installation_id: manifest.installation_id,
        });
    }

    initialize_layout(paths, node_id)
}

fn initialize_layout(paths: &StoragePaths, node_id: u16) -> Result<StorageBinding> {
    let mut existing = Vec::new();
    for role in StorageRole::ALL {
        let path = marker_path(paths, role);
        if path.exists() {
            existing.push((role, read_json::<RootMarker>(&path)?));
        }
    }
    let installation_id = match existing.first() {
        Some((_, marker)) => marker.installation_id.clone(),
        None => random_storage_id()?,
    };
    for (role, marker) in &existing {
        validate_marker(marker, *role, node_id, &installation_id)?;
    }

    let existing_roles = existing
        .iter()
        .map(|(role, _)| *role)
        .collect::<BTreeSet<_>>();
    let mut roots = BTreeMap::new();
    for role in StorageRole::ALL {
        let marker = if existing_roles.contains(&role) {
            read_marker(paths, role)?
        } else {
            let marker = RootMarker {
                format_version: FORMAT_VERSION,
                installation_id: installation_id.clone(),
                node_id,
                role: role.name().to_owned(),
                root_id: random_storage_id()?,
            };
            write_new_json(&marker_path(paths, role), &marker)?;
            marker
        };
        roots.insert(role.name().to_owned(), marker.root_id);
    }
    let manifest = LayoutManifest {
        format_version: FORMAT_VERSION,
        installation_id: installation_id.clone(),
        node_id,
        roots,
    };
    write_new_json(&paths.state.join(LAYOUT_FILE), &manifest)?;
    sync_directory(&paths.state)?;
    Ok(StorageBinding {
        newly_initialized: true,
        installation_id,
    })
}

fn validate_manifest(manifest: &LayoutManifest, node_id: u16) -> Result<()> {
    ensure!(
        manifest.format_version == FORMAT_VERSION,
        "unsupported storage layout format"
    );
    ensure!(
        manifest.node_id == node_id,
        "storage layout belongs to another node ID"
    );
    ensure!(
        manifest.roots.len() == StorageRole::ALL.len(),
        "storage layout is incomplete"
    );
    Ok(())
}

fn validate_marker(
    marker: &RootMarker,
    role: StorageRole,
    node_id: u16,
    installation_id: &str,
) -> Result<()> {
    ensure!(
        marker.format_version == FORMAT_VERSION,
        "unsupported {} root format",
        role.name()
    );
    ensure!(
        marker.role == role.name(),
        "storage root has the wrong role"
    );
    ensure!(
        marker.node_id == node_id,
        "{} storage root belongs to another node ID",
        role.name()
    );
    ensure!(
        marker.installation_id == installation_id,
        "{} storage root belongs to another node installation",
        role.name()
    );
    Ok(())
}

fn role_path(paths: &StoragePaths, role: StorageRole) -> &Path {
    match role {
        StorageRole::State => &paths.state,
        StorageRole::Metadata => &paths.metadata,
        StorageRole::MetadataWal => &paths.metadata_wal,
        StorageRole::Payload => &paths.payload,
    }
}

fn marker_path(paths: &StoragePaths, role: StorageRole) -> PathBuf {
    role_path(paths, role).join(role.marker_name())
}

fn read_marker(paths: &StoragePaths, role: StorageRole) -> Result<RootMarker> {
    let path = marker_path(paths, role);
    if !path.exists() {
        bail!(
            "{} storage root marker is missing at {}",
            role.name(),
            path.display()
        );
    }
    read_json(&path)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("decode {}", path.display()))
}

fn write_new_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            bail!(
                "storage layout file appeared concurrently at {}",
                path.display()
            )
        }
        Err(error) => return Err(error).with_context(|| format!("create {}", path.display())),
    };
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    sync_directory(path.parent().context("storage layout path has no parent")?)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn random_storage_id() -> Result<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow::anyhow!("generate storage-root identity: {error}"))?;
    Ok(hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roots_are_pinned_by_identity_not_textual_path() {
        let root = tempfile::tempdir().unwrap();
        let first = StoragePaths::under(root.path().join("first"), 1024);
        let binding = bind_authoritative_roots(&first, 7).unwrap();
        assert!(binding.newly_initialized);

        let moved_root = root.path().join("moved");
        fs::rename(&first.state, &moved_root).unwrap();
        let moved = StoragePaths::under(&moved_root, 1024);
        let reopened = bind_authoritative_roots(&moved, 7).unwrap();
        assert!(!reopened.newly_initialized);
        assert_eq!(reopened.installation_id, binding.installation_id);
    }

    #[test]
    fn empty_replacement_root_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let paths = StoragePaths::under(root.path().join("node"), 1024);
        bind_authoritative_roots(&paths, 1).unwrap();
        fs::remove_file(marker_path(&paths, StorageRole::Payload)).unwrap();
        let error = bind_authoritative_roots(&paths, 1).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("payload storage root marker is missing")
        );
    }

    #[test]
    fn swapping_roots_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let mut first = StoragePaths::under(root.path().join("first"), 1024);
        let second = StoragePaths::under(root.path().join("second"), 1024);
        bind_authoritative_roots(&first, 1).unwrap();
        bind_authoritative_roots(&second, 2).unwrap();
        first.payload = second.payload;
        let error = bind_authoritative_roots(&first, 1).unwrap_err();
        assert!(error.to_string().contains("another node ID"));
    }
}
