use crate::{
    config::Config, mvcc_bootstrap::MvccSubsystem, persistence::Persistence,
    personaldb_signing::PersonalDbProtocolKeyring,
};
use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use personaldb_protocol::{
    Ed25519ProtocolSigner, Ed25519PublicKey, KeyGeneration, KeyTrustPolicy, ProtocolSigner,
    PublicKeyTrustRecord, PublicKeyTrustStore, SignaturePurpose,
};
use std::sync::Arc;

const GROUP_CONTROL_PKCS8_B64: &str =
    "MC4CAQAwBQYDK2VwBCIEIBERERERERERERERERERERERERERERERERERERERERER";
const GROUP_CONTROL_PUBLIC_B64U: &str = "0EqyMnQrtKs6E2i9RhXk5tAiSrcaAWuvhSCjMsl3hzc";
const SNAPSHOT_PKCS8_B64: &str = "MC4CAQAwBQYDK2VwBCIEICIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIi";
const SNAPSHOT_PUBLIC_B64U: &str = "oJql9HpnWYAv-VX43C0qFKXJnSO-l_hkEn_5ODRVpPA";
const WITNESS_PKCS8_B64: &str = "MC4CAQAwBQYDK2VwBCIEIDMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMz";
const WITNESS_PUBLIC_B64U: &str = "F8t5-ytBIPKx7GXkGY1uCLKOgT_rAeSkAIObheGAgM4";

/// Builds the production persistence stack with a real, single-node MVCC
/// subsystem for unit tests.
///
/// `Persistence::new` intentionally remains a synchronous construction step
/// because production installs the cluster-scoped MVCC runtime during
/// `AppState` startup. Unit tests that exercise persistence directly must
/// mirror that second step instead of relying on a legacy non-MVCC write path.
pub(crate) async fn persistence_with_mvcc(config: &Config) -> Result<Persistence> {
    let persistence = Persistence::new(config)?;
    let core_store = persistence
        .core_store()
        .await
        .context("open test persistence CoreStore")?;
    let mvcc = Arc::new(
        MvccSubsystem::bootstrap(config, core_store.core_meta_database())
            .await
            .context("bootstrap test persistence MVCC subsystem")?,
    );
    persistence
        .install_mvcc(mvcc)
        .context("install test persistence MVCC subsystem")?;
    Ok(persistence)
}

pub(crate) fn personaldb_protocol_keyring() -> PersonalDbProtocolKeyring {
    let signers = [
        signer(
            GROUP_CONTROL_PKCS8_B64,
            GROUP_CONTROL_PUBLIC_B64U,
            SignaturePurpose::GroupControl,
        ),
        signer(
            SNAPSHOT_PKCS8_B64,
            SNAPSHOT_PUBLIC_B64U,
            SignaturePurpose::Snapshot,
        ),
        signer(
            WITNESS_PKCS8_B64,
            WITNESS_PUBLIC_B64U,
            SignaturePurpose::Witness,
        ),
    ];
    let trust_store = PublicKeyTrustStore::from_records(
        signers.iter().map(|signer| signer.trust_record().clone()),
    )
    .unwrap();
    PersonalDbProtocolKeyring::new_test_only(trust_store, signers).unwrap()
}

fn signer(
    private_key_b64: &str,
    public_key_b64u: &str,
    purpose: SignaturePurpose,
) -> Arc<dyn ProtocolSigner> {
    let private_key = STANDARD.decode(private_key_b64).unwrap();
    let public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(public_key_b64u)
        .unwrap();
    let public_key = Ed25519PublicKey::try_from(public_key.as_slice()).unwrap();
    let policy = KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), purpose, 0);
    let record = PublicKeyTrustRecord::new(public_key, policy);
    Arc::new(Ed25519ProtocolSigner::from_pkcs8_der_with_trust_record(&private_key, record).unwrap())
}
