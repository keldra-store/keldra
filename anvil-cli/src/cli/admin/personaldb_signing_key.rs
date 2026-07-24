use super::common::{AdminClient, MutationOptions, print_rpc_response, with_auth};
use anvil::anvil_api as api;
use clap::{Subcommand, ValueEnum};
use personaldb_protocol::{
    DatabaseId, Ed25519ProtocolSigner, KeyGeneration, KeyTrustPolicy, ProtocolSigner,
    PublicKeyStatus, SignaturePurpose,
};
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

const MAX_PRIVATE_KEY_BYTES: u64 = 16 * 1024;

#[derive(Subcommand)]
pub enum PersonalDbSigningKeyCommands {
    /// Import an Ed25519 PKCS#8 DER private key into encrypted server storage
    Import {
        #[clap(flatten)]
        context: MutationOptions,
        /// Path to an Ed25519 private key encoded as PKCS#8 DER
        #[clap(long = "private-key-pkcs8", value_name = "FILE")]
        private_key_pkcs8: PathBuf,
        /// Monotonically increasing protocol key generation
        #[clap(long = "key-generation")]
        key_generation: u64,
        /// PersonalDB protocol purpose authorized for this key
        #[clap(long, value_enum)]
        purpose: PersonalDbSigningPurposeArg,
        /// Database scope; repeat to authorize more than one database
        #[clap(long = "database-scope", value_name = "DATABASE_ID")]
        database_scopes: Vec<String>,
        /// Group scope; repeat to authorize more than one group
        #[clap(long = "group-scope", value_name = "GROUP_ID")]
        group_scopes: Vec<String>,
        /// Inclusive first PersonalDB log index at which the key is authoritative
        #[clap(long, default_value_t = 0)]
        valid_from_log_index: u64,
        /// Exclusive PersonalDB log index at which the key stops being authoritative
        #[clap(long)]
        valid_until_log_index: Option<u64>,
        /// Initial key lifecycle status
        #[clap(long, value_enum, default_value = "active")]
        status: PersonalDbSigningImportStatusArg,
    },
    /// List public metadata for PersonalDB signing keys
    List,
    /// Apply a one-way lifecycle transition to a PersonalDB signing key
    SetStatus {
        #[clap(flatten)]
        context: MutationOptions,
        /// Canonical sha256 key identifier
        #[clap(long)]
        key_id: String,
        /// New non-active lifecycle status
        #[clap(long, value_enum)]
        status: PersonalDbSigningTerminalStatusArg,
        /// Exclusive PersonalDB log index at which the key stops being authoritative
        #[clap(long)]
        valid_until_log_index: u64,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PersonalDbSigningPurposeArg {
    GroupControl,
    ProposalAdmission,
    Witness,
    Snapshot,
}

impl PersonalDbSigningPurposeArg {
    fn to_protocol(self) -> SignaturePurpose {
        match self {
            Self::GroupControl => SignaturePurpose::GroupControl,
            Self::ProposalAdmission => SignaturePurpose::ProposalAdmission,
            Self::Witness => SignaturePurpose::Witness,
            Self::Snapshot => SignaturePurpose::Snapshot,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum PersonalDbSigningImportStatusArg {
    #[default]
    Active,
    Retiring,
    RevokedFuture,
    Compromised,
}

impl PersonalDbSigningImportStatusArg {
    fn to_protocol(self) -> PublicKeyStatus {
        match self {
            Self::Active => PublicKeyStatus::Active,
            Self::Retiring => PublicKeyStatus::Retiring,
            Self::RevokedFuture => PublicKeyStatus::RevokedFuture,
            Self::Compromised => PublicKeyStatus::Compromised,
        }
    }

    fn as_protocol_str(self) -> &'static str {
        self.to_protocol().as_str()
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PersonalDbSigningTerminalStatusArg {
    Retiring,
    RevokedFuture,
    Compromised,
}

impl PersonalDbSigningTerminalStatusArg {
    fn as_protocol_str(self) -> &'static str {
        match self {
            Self::Retiring => PublicKeyStatus::Retiring.as_str(),
            Self::RevokedFuture => PublicKeyStatus::RevokedFuture.as_str(),
            Self::Compromised => PublicKeyStatus::Compromised.as_str(),
        }
    }
}

pub(super) async fn handle_personaldb_signing_key_command(
    command: &PersonalDbSigningKeyCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        PersonalDbSigningKeyCommands::Import {
            context,
            private_key_pkcs8,
            key_generation,
            purpose,
            database_scopes,
            group_scopes,
            valid_from_log_index,
            valid_until_log_index,
            status,
        } => {
            let private_key_pkcs8_der = read_private_key_file(private_key_pkcs8).await?;
            let public_key = derive_public_key(
                &private_key_pkcs8_der,
                *key_generation,
                *purpose,
                database_scopes,
                group_scopes,
                *valid_from_log_index,
                *valid_until_log_index,
                *status,
            )?;
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "personaldb_signing_key",
                Some(&admin_context),
                None,
                client.import_personal_db_signing_key(with_auth(
                    api::ImportPersonalDbSigningKeyRequest {
                        context: Some(admin_context.clone()),
                        private_key_pkcs8_der,
                        public_key,
                        key_generation: *key_generation,
                        purpose: purpose.to_protocol().as_str().to_string(),
                        database_scopes: database_scopes.clone(),
                        group_scopes: group_scopes.clone(),
                        valid_from_log_index: *valid_from_log_index,
                        valid_until_log_index: *valid_until_log_index,
                        status: status.as_protocol_str().to_string(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        PersonalDbSigningKeyCommands::List => {
            print_rpc_response(
                "personaldb_signing_keys",
                None,
                None,
                client.list_personal_db_signing_keys(with_auth(
                    api::ListPersonalDbSigningKeysRequest { page: None },
                    token,
                )?),
            )
            .await?;
        }
        PersonalDbSigningKeyCommands::SetStatus {
            context,
            key_id,
            status,
            valid_until_log_index,
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "personaldb_signing_key",
                Some(&admin_context),
                None,
                client.set_personal_db_signing_key_status(with_auth(
                    api::SetPersonalDbSigningKeyStatusRequest {
                        context: Some(admin_context.clone()),
                        key_id: key_id.clone(),
                        status: status.as_protocol_str().to_string(),
                        valid_until_log_index: Some(*valid_until_log_index),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn derive_public_key(
    private_key_pkcs8_der: &[u8],
    generation: u64,
    purpose: PersonalDbSigningPurposeArg,
    database_scopes: &[String],
    group_scopes: &[String],
    valid_from_log_index: u64,
    valid_until_log_index: Option<u64>,
    status: PersonalDbSigningImportStatusArg,
) -> anyhow::Result<Vec<u8>> {
    let mut policy = KeyTrustPolicy::new(
        KeyGeneration::new(generation)?,
        purpose.to_protocol(),
        valid_from_log_index,
    )
    .with_database_scopes(
        database_scopes
            .iter()
            .cloned()
            .map(DatabaseId::new)
            .collect(),
    )
    .with_group_scopes(group_scopes.to_vec())
    .with_status(status.to_protocol());
    if let Some(valid_until_log_index) = valid_until_log_index {
        policy = policy.with_valid_until(valid_until_log_index);
    }

    let signer = Ed25519ProtocolSigner::from_pkcs8_der(private_key_pkcs8_der, policy)?;
    Ok(signer.trust_record().public_key.as_bytes().to_vec())
}

pub(super) async fn read_private_key_file(path: &Path) -> anyhow::Result<Vec<u8>> {
    let path_metadata = tokio::fs::symlink_metadata(path).await.map_err(|err| {
        anyhow::anyhow!(
            "failed to inspect PersonalDB signing key file {}: {err}",
            path.display()
        )
    })?;
    validate_private_key_metadata(path, &path_metadata)?;

    let file = tokio::fs::File::open(path).await.map_err(|err| {
        anyhow::anyhow!(
            "failed to open PersonalDB signing key file {}: {err}",
            path.display()
        )
    })?;
    let opened_metadata = file.metadata().await.map_err(|err| {
        anyhow::anyhow!(
            "failed to inspect opened PersonalDB signing key file {}: {err}",
            path.display()
        )
    })?;
    validate_private_key_metadata(path, &opened_metadata)?;
    validate_same_file(path, &path_metadata, &opened_metadata)?;

    let mut private_key = Vec::with_capacity(opened_metadata.len() as usize);
    file.take(MAX_PRIVATE_KEY_BYTES + 1)
        .read_to_end(&mut private_key)
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "failed to read PersonalDB signing key file {}: {err}",
                path.display()
            )
        })?;
    if private_key.len() as u64 > MAX_PRIVATE_KEY_BYTES {
        anyhow::bail!(
            "PersonalDB signing key file {} exceeds the {} byte limit",
            path.display(),
            MAX_PRIVATE_KEY_BYTES
        );
    }

    let final_metadata = tokio::fs::symlink_metadata(path).await.map_err(|err| {
        anyhow::anyhow!(
            "failed to re-inspect PersonalDB signing key file {}: {err}",
            path.display()
        )
    })?;
    validate_private_key_metadata(path, &final_metadata)?;
    validate_same_file(path, &opened_metadata, &final_metadata)?;
    Ok(private_key)
}

fn validate_private_key_metadata(path: &Path, metadata: &std::fs::Metadata) -> anyhow::Result<()> {
    if metadata.file_type().is_symlink() {
        anyhow::bail!(
            "PersonalDB signing key file {} must not be a symbolic link",
            path.display()
        );
    }
    if !metadata.is_file() {
        anyhow::bail!(
            "PersonalDB signing key file {} must be a regular file",
            path.display()
        );
    }
    if metadata.len() > MAX_PRIVATE_KEY_BYTES {
        anyhow::bail!(
            "PersonalDB signing key file {} exceeds the {} byte limit",
            path.display(),
            MAX_PRIVATE_KEY_BYTES
        );
    }
    validate_unix_private_key_permissions(path, metadata)
}

#[cfg(unix)]
fn validate_unix_private_key_permissions(
    path: &Path,
    metadata: &std::fs::Metadata,
) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode = metadata.permissions().mode();
    if mode & 0o077 != 0 {
        anyhow::bail!(
            "PersonalDB signing key file {} must not grant group or other permissions (mode {:04o})",
            path.display(),
            mode & 0o7777
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_unix_private_key_permissions(
    _path: &Path,
    _metadata: &std::fs::Metadata,
) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn validate_same_file(
    path: &Path,
    expected: &std::fs::Metadata,
    actual: &std::fs::Metadata,
) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    if expected.dev() != actual.dev() || expected.ino() != actual.ino() {
        anyhow::bail!(
            "PersonalDB signing key file {} changed while it was being read",
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_same_file(
    _path: &Path,
    _expected: &std::fs::Metadata,
    _actual: &std::fs::Metadata,
) -> anyhow::Result<()> {
    Ok(())
}
