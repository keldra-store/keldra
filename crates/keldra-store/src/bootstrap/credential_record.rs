use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{CredentialSecretEnvelope, StorageTenantId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StoredApplicationCredential {
    pub(crate) format_version: u16,
    pub(crate) app_id: String,
    pub(crate) client_id: String,
    pub(crate) storage_tenant: StorageTenantId,
    pub(crate) active: bool,
    pub(crate) verifier: StoredCredentialVerifier,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) sigv4_secret: Option<CredentialSecretEnvelope>,
}

/// KDF identity and costs are durable data so a later release can add an
/// explicit migration branch without guessing which verifier produced a
/// credential record.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "algorithm", rename_all = "snake_case")]
pub(crate) enum StoredCredentialVerifier {
    Argon2id {
        version: u32,
        memory_cost_kib: u32,
        time_cost: u32,
        parallelism: u32,
        output_length: u32,
        salt: [u8; 32],
        output: [u8; 32],
    },
}

impl fmt::Debug for StoredCredentialVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Argon2id {
                version,
                memory_cost_kib,
                time_cost,
                parallelism,
                output_length,
                ..
            } => formatter
                .debug_struct("Argon2id")
                .field("version", version)
                .field("memory_cost_kib", memory_cost_kib)
                .field("time_cost", time_cost)
                .field("parallelism", parallelism)
                .field("output_length", output_length)
                .field("salt", &"[REDACTED]")
                .field("output", &"[REDACTED]")
                .finish(),
        }
    }
}
