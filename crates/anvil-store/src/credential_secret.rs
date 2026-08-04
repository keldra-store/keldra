use serde::{Deserialize, Serialize};

const FORMAT_VERSION: u8 = 1;
const NONCE_BYTES: usize = 12;
const TAG_BYTES: usize = 16;
const MAX_PLAINTEXT_BYTES: usize = 4 * 1024;

/// Opaque AES-256-GCM envelope for the plaintext-equivalent material that an
/// S3 SigV4 verifier necessarily needs. The encryption key stays outside the
/// store; this value is replicated as part of the existing credential record.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialSecretEnvelope {
    format_version: u8,
    nonce: [u8; NONCE_BYTES],
    ciphertext: Vec<u8>,
}

impl CredentialSecretEnvelope {
    pub fn new(nonce: [u8; NONCE_BYTES], ciphertext: Vec<u8>) -> Result<Self, &'static str> {
        let value = Self {
            format_version: FORMAT_VERSION,
            nonce,
            ciphertext,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn nonce(&self) -> &[u8; NONCE_BYTES] {
        &self.nonce
    }

    pub fn ciphertext(&self) -> &[u8] {
        &self.ciphertext
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.format_version != FORMAT_VERSION {
            return Err("credential secret envelope format is unsupported");
        }
        if !(TAG_BYTES..=MAX_PLAINTEXT_BYTES + TAG_BYTES).contains(&self.ciphertext.len()) {
            return Err("credential secret envelope length is invalid");
        }
        Ok(())
    }
}

impl std::fmt::Debug for CredentialSecretEnvelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialSecretEnvelope")
            .field("format_version", &self.format_version)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_is_bounded_and_never_debugs_secret_bytes() {
        let envelope = CredentialSecretEnvelope::new([7; 12], vec![9; 48]).unwrap();
        assert_eq!(envelope.nonce(), &[7; 12]);
        assert_eq!(envelope.ciphertext(), &[9; 48]);
        assert!(!format!("{envelope:?}").contains("9999"));
        assert!(CredentialSecretEnvelope::new([0; 12], vec![0; 15]).is_err());
    }
}
