use anyhow::{Context, Result, bail};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::{OsRng, TryRngCore};

pub const NODE_SIGNING_ALGORITHM: &str = "ed25519";
const ED25519_SECRET_KEY_BYTES: usize = 32;
const ED25519_PUBLIC_KEY_BYTES: usize = 32;

#[derive(Clone)]
pub struct NodeSigningKeypair {
    signing_key: SigningKey,
}

impl std::fmt::Debug for NodeSigningKeypair {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeSigningKeypair")
            .field("public_key", &hex::encode(self.public_key_bytes()))
            .finish_non_exhaustive()
    }
}

impl NodeSigningKeypair {
    pub fn generate() -> Result<Self> {
        let mut secret = [0_u8; ED25519_SECRET_KEY_BYTES];
        OsRng
            .try_fill_bytes(&mut secret)
            .context("generate node signing key")?;
        Ok(Self {
            signing_key: SigningKey::from_bytes(&secret),
        })
    }

    pub fn from_secret_key_bytes(bytes: &[u8]) -> Result<Self> {
        let secret: [u8; ED25519_SECRET_KEY_BYTES] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("node signing secret key must contain 32 bytes"))?;
        Ok(Self {
            signing_key: SigningKey::from_bytes(&secret),
        })
    }

    pub fn secret_key_bytes(&self) -> [u8; ED25519_SECRET_KEY_BYTES] {
        self.signing_key.to_bytes()
    }

    pub fn public_key(&self) -> NodeVerifyingKey {
        NodeVerifyingKey {
            verifying_key: self.signing_key.verifying_key(),
        }
    }

    pub fn public_key_bytes(&self) -> [u8; ED25519_PUBLIC_KEY_BYTES] {
        self.signing_key.verifying_key().to_bytes()
    }

    pub fn sign(&self, message: &[u8]) -> Vec<u8> {
        self.signing_key.sign(message).to_bytes().to_vec()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeVerifyingKey {
    verifying_key: VerifyingKey,
}

impl NodeVerifyingKey {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let bytes: [u8; ED25519_PUBLIC_KEY_BYTES] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("node signing public key must contain 32 bytes"))?;
        let verifying_key = VerifyingKey::from_bytes(&bytes)
            .context("node signing public key is not valid Ed25519")?;
        Ok(Self { verifying_key })
    }

    pub fn to_bytes(self) -> [u8; ED25519_PUBLIC_KEY_BYTES] {
        self.verifying_key.to_bytes()
    }

    pub fn verify(self, message: &[u8], signature: &[u8]) -> Result<()> {
        let signature = Signature::from_slice(signature)
            .context("node receipt signature is not a canonical Ed25519 signature")?;
        self.verifying_key
            .verify(message, &signature)
            .map_err(|_| anyhow::anyhow!("node receipt signature verification failed"))
    }
}

pub fn validate_public_key_bytes(bytes: &[u8]) -> Result<()> {
    if bytes.len() != ED25519_PUBLIC_KEY_BYTES {
        bail!("node signing public key must contain 32 bytes");
    }
    NodeVerifyingKey::from_bytes(bytes).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_key_round_trips_and_verifies() {
        let keypair = NodeSigningKeypair::generate().unwrap();
        let restored =
            NodeSigningKeypair::from_secret_key_bytes(&keypair.secret_key_bytes()).unwrap();
        assert_eq!(keypair.public_key_bytes(), restored.public_key_bytes());

        let signature = restored.sign(b"anvil-node-signing-test");
        NodeVerifyingKey::from_bytes(&keypair.public_key_bytes())
            .unwrap()
            .verify(b"anvil-node-signing-test", &signature)
            .unwrap();
    }

    #[test]
    fn verification_rejects_wrong_payload() {
        let keypair = NodeSigningKeypair::generate().unwrap();
        let signature = keypair.sign(b"expected");
        assert!(
            keypair
                .public_key()
                .verify(b"different", &signature)
                .is_err()
        );
    }
}
