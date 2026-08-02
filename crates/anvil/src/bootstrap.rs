use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anvil_store::{SYSTEM_STORAGE_TENANT_ID, Store, SystemBootstrapRequest, SystemBootstrapState};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const CREDENTIAL_SCHEMA: &str = "anvil.system-bootstrap-credential.v1";
const DEFAULT_CREDENTIAL_FILE: &str = "system-bootstrap-credential.json";
const MAX_CREDENTIAL_FILE_BYTES: u64 = 4 * 1024;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapCredential {
    schema: String,
    storage_tenant: String,
    app_id: String,
    client_id: String,
    client_secret: String,
}

/// Enforces the explicit, one-time bootstrap contract before any public
/// service is started.
pub(crate) async fn enforce(
    store: &Store,
    data_dir: &Path,
    run_system_bootstrap: bool,
    configured_output: Option<&Path>,
) -> Result<()> {
    let state_store = store.clone();
    let state = tokio::task::spawn_blocking(move || state_store.system_bootstrap_state())
        .await
        .context("join system bootstrap marker read")?
        .context("read system bootstrap marker")?;

    match (state, run_system_bootstrap) {
        (SystemBootstrapState::Complete { version: 1 }, true) => {
            bail!("system bootstrap has already completed")
        }
        (SystemBootstrapState::Complete { version: 1 }, false) => return Ok(()),
        (SystemBootstrapState::Complete { version }, _) => {
            bail!("system bootstrap marker version {version} is unsupported")
        }
        (SystemBootstrapState::Missing, false) => {
            bail!(
                "system bootstrap has not completed; start this node once with \
                 --run-system-bootstrap"
            )
        }
        (SystemBootstrapState::Missing, true) => {}
    }

    let output = configured_output
        .map(Path::to_path_buf)
        .unwrap_or_else(|| data_dir.join(DEFAULT_CREDENTIAL_FILE));
    let credential = load_or_create_credential(&output).with_context(|| {
        format!(
            "prepare system bootstrap credential at {}",
            output.display()
        )
    })?;
    let logged_output = fs::canonicalize(&output).unwrap_or_else(|_| output.clone());
    eprintln!(
        "system bootstrap credential is durable at {}; bootstrap metadata is not yet committed; \
         after bootstrap completes, copy this file to a secret store, then delete this generated copy",
        logged_output.display()
    );
    let request = SystemBootstrapRequest {
        app_id: credential.app_id,
        client_id: credential.client_id,
        client_secret: credential.client_secret,
    };
    let bootstrap_store = store.clone();
    tokio::task::spawn_blocking(move || bootstrap_store.bootstrap_system(request))
        .await
        .context("join atomic system bootstrap")?
        .context("commit atomic system bootstrap")?;

    eprintln!(
        "system bootstrap completed; copy the credential file at {} to a secret store, then \
         delete this generated copy",
        logged_output.display()
    );
    Ok(())
}

fn load_or_create_credential(path: &Path) -> Result<BootstrapCredential> {
    match fs::symlink_metadata(path) {
        Ok(_) => return read_secure_credential(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let parent = parent_directory(path)?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create credential directory {}", parent.display()))?;
    let credential = generate_credential();
    let body = encode_credential(&credential)?;
    let temporary = temporary_path(path)?;
    let mut file = create_private_file(&temporary)?;
    let publish_result = (|| -> Result<()> {
        file.write_all(&body)?;
        file.sync_all()?;
        drop(file);
        fs::hard_link(&temporary, path)?;
        fs::remove_file(&temporary)?;
        sync_directory(parent)?;
        Ok(())
    })();

    match publish_result {
        Ok(()) => Ok(credential),
        Err(error) if is_already_exists(&error) => {
            let _ = fs::remove_file(&temporary);
            read_secure_credential(path)
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(error)
        }
    }
}

fn read_secure_credential(path: &Path) -> Result<BootstrapCredential> {
    let linked_metadata = fs::symlink_metadata(path)?;
    if !linked_metadata.file_type().is_file() {
        bail!("bootstrap credential is not a regular file");
    }
    require_private_mode(&linked_metadata)?;

    let mut file = File::open(path)?;
    let opened_metadata = file.metadata()?;
    require_same_file(&linked_metadata, &opened_metadata)?;
    require_private_mode(&opened_metadata)?;
    if opened_metadata.len() > MAX_CREDENTIAL_FILE_BYTES {
        bail!("bootstrap credential exceeds {MAX_CREDENTIAL_FILE_BYTES} bytes");
    }

    let mut body = Vec::with_capacity(opened_metadata.len() as usize);
    Read::by_ref(&mut file)
        .take(MAX_CREDENTIAL_FILE_BYTES + 1)
        .read_to_end(&mut body)?;
    if body.len() as u64 > MAX_CREDENTIAL_FILE_BYTES {
        bail!("bootstrap credential exceeds {MAX_CREDENTIAL_FILE_BYTES} bytes");
    }
    file.sync_all()?;
    sync_directory(parent_directory(path)?)?;

    let credential: BootstrapCredential =
        serde_json::from_slice(&body).context("decode bootstrap credential JSON")?;
    validate_credential(&credential)?;
    Ok(credential)
}

fn generate_credential() -> BootstrapCredential {
    BootstrapCredential {
        schema: CREDENTIAL_SCHEMA.to_owned(),
        storage_tenant: SYSTEM_STORAGE_TENANT_ID.to_owned(),
        app_id: format!("bootstrap-{}", Uuid::new_v4().simple()),
        client_id: format!("client-{}", Uuid::new_v4().simple()),
        client_secret: format!(
            "secret-{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        ),
    }
}

fn validate_credential(credential: &BootstrapCredential) -> Result<()> {
    if credential.schema != CREDENTIAL_SCHEMA {
        bail!("bootstrap credential schema is unsupported");
    }
    if credential.storage_tenant != SYSTEM_STORAGE_TENANT_ID {
        bail!("bootstrap credential does not belong to the system tenant");
    }
    require_generated_component(&credential.app_id, "bootstrap-", 32, "application ID")?;
    require_generated_component(&credential.client_id, "client-", 32, "client ID")?;
    require_generated_component(&credential.client_secret, "secret-", 64, "client secret")?;
    Ok(())
}

fn require_generated_component(
    value: &str,
    prefix: &str,
    encoded_length: usize,
    name: &str,
) -> Result<()> {
    let Some(encoded) = value.strip_prefix(prefix) else {
        bail!("bootstrap credential {name} is invalid");
    };
    if encoded.len() != encoded_length
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("bootstrap credential {name} is invalid");
    }
    Ok(())
}

fn encode_credential(credential: &BootstrapCredential) -> Result<Vec<u8>> {
    let mut body = serde_json::to_vec_pretty(credential)?;
    body.push(b'\n');
    Ok(body)
}

fn parent_directory(path: &Path) -> Result<&Path> {
    if path.file_name().is_none() {
        bail!("bootstrap credential output must name a file");
    }
    Ok(path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new(".")))
}

fn temporary_path(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .context("bootstrap credential output must name a file")?;
    let mut temporary_name = OsString::from(".");
    temporary_name.push(file_name);
    temporary_name.push(format!(".{}.tmp", Uuid::new_v4().simple()));
    Ok(parent_directory(path)?.join(temporary_name))
}

fn is_already_exists(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists)
}

#[cfg(unix)]
fn create_private_file(path: &Path) -> Result<File> {
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
fn create_private_file(_path: &Path) -> Result<File> {
    bail!("system bootstrap requires mode-0600 credential files")
}

#[cfg(unix)]
fn require_private_mode(metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o7777 != 0o600 {
        bail!("bootstrap credential must have mode 0600");
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_private_mode(_metadata: &fs::Metadata) -> Result<()> {
    bail!("system bootstrap requires mode-0600 credential files")
}

#[cfg(unix)]
fn require_same_file(linked: &fs::Metadata, opened: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    if linked.dev() != opened.dev() || linked.ino() != opened.ino() {
        bail!("bootstrap credential changed while it was opened");
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_same_file(_linked: &fs::Metadata, _opened: &fs::Metadata) -> Result<()> {
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn generated_credential_is_private_and_reused_exactly() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bootstrap.json");

        let first = load_or_create_credential(&path).unwrap();
        let second = load_or_create_credential(&path).unwrap();

        assert!(first == second);
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(first.app_id.len(), "bootstrap-".len() + 32);
        assert_eq!(first.client_id.len(), "client-".len() + 32);
        assert_eq!(first.client_secret.len(), "secret-".len() + 64);
    }

    #[cfg(unix)]
    #[test]
    fn existing_credential_with_broad_permissions_is_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bootstrap.json");
        let credential = generate_credential();
        fs::write(&path, encode_credential(&credential).unwrap()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();

        let error = load_or_create_credential(&path).err().unwrap();
        assert!(error.to_string().contains("mode 0600"));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_is_not_accepted_as_a_retry_credential() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target.json");
        let link = directory.path().join("bootstrap.json");
        let credential = generate_credential();
        fs::write(&target, encode_credential(&credential).unwrap()).unwrap();
        symlink(&target, &link).unwrap();

        let error = load_or_create_credential(&link).err().unwrap();
        assert!(error.to_string().contains("not a regular file"));
    }
}
