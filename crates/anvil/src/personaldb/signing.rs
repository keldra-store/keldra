use std::sync::Arc;

use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
use personaldb_protocol::{
    Ed25519ProtocolSigner, KeyGeneration, KeyTrustPolicy, ProtocolSigner, PublicKeyTrustRecord,
    SignaturePurpose,
};
use tonic::Status;

use crate::authentication::{JwtManager, PersonalDbSigningPurpose};

#[derive(Clone)]
pub(super) struct PersonalDbSigners {
    group_control: Arc<Ed25519ProtocolSigner>,
    projection_builder: Arc<Ed25519ProtocolSigner>,
    snapshot: Arc<Ed25519ProtocolSigner>,
    witness: Arc<Ed25519ProtocolSigner>,
    trust_records_json: Arc<Vec<Vec<u8>>>,
}

impl PersonalDbSigners {
    pub(super) fn derive(tokens: &JwtManager) -> Result<Self, Status> {
        let group_control = signer(
            tokens,
            PersonalDbSigningPurpose::GroupControl,
            SignaturePurpose::GroupControl,
        )?;
        let projection_builder = signer(
            tokens,
            PersonalDbSigningPurpose::ProjectionBuilder,
            SignaturePurpose::ProjectionBuilder,
        )?;
        let snapshot = signer(
            tokens,
            PersonalDbSigningPurpose::Snapshot,
            SignaturePurpose::Snapshot,
        )?;
        let witness = signer(
            tokens,
            PersonalDbSigningPurpose::Witness,
            SignaturePurpose::Witness,
        )?;
        let trust_records_json = Arc::new(
            [
                group_control.trust_record(),
                projection_builder.trust_record(),
                snapshot.trust_record(),
                witness.trust_record(),
            ]
            .into_iter()
            .map(encode_trust_record)
            .collect::<Result<Vec<_>, _>>()?,
        );
        Ok(Self {
            group_control,
            projection_builder,
            snapshot,
            witness,
            trust_records_json,
        })
    }

    pub(super) fn group_control(&self) -> &dyn ProtocolSigner {
        self.group_control.as_ref()
    }

    pub(super) fn projection_builder(&self) -> &dyn ProtocolSigner {
        self.projection_builder.as_ref()
    }

    pub(super) fn snapshot(&self) -> &dyn ProtocolSigner {
        self.snapshot.as_ref()
    }

    pub(super) fn witness(&self) -> &dyn ProtocolSigner {
        self.witness.as_ref()
    }

    pub(super) fn trust_records_json(&self) -> Vec<Vec<u8>> {
        self.trust_records_json.as_ref().clone()
    }
}

fn signer(
    tokens: &JwtManager,
    seed_purpose: PersonalDbSigningPurpose,
    protocol_purpose: SignaturePurpose,
) -> Result<Arc<Ed25519ProtocolSigner>, Status> {
    let key = SigningKey::from_bytes(&tokens.personaldb_signing_seed(seed_purpose));
    let der = key
        .to_pkcs8_der()
        .map_err(|_| Status::internal("PersonalDB signing key could not be encoded"))?;
    let generation = KeyGeneration::new(1)
        .map_err(|_| Status::internal("PersonalDB signing generation is invalid"))?;
    let policy = KeyTrustPolicy::new(generation, protocol_purpose, 0);
    Ed25519ProtocolSigner::from_pkcs8_der(der.as_bytes(), policy)
        .map(Arc::new)
        .map_err(|_| Status::internal("PersonalDB signing key could not be initialized"))
}

fn encode_trust_record(record: &PublicKeyTrustRecord) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(record)
        .map_err(|_| Status::internal("PersonalDB trust record could not be encoded"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_key_derivation_is_stable_and_purpose_separated() {
        let first = PersonalDbSigners::derive(&JwtManager::new([7_u8; 32]).unwrap()).unwrap();
        let second = PersonalDbSigners::derive(&JwtManager::new([7_u8; 32]).unwrap()).unwrap();
        assert_eq!(first.trust_records_json(), second.trust_records_json());
        let records = first.trust_records_json();
        assert_eq!(records.len(), 4);
        assert!(records.windows(2).all(|pair| pair[0] != pair[1]));
    }
}
