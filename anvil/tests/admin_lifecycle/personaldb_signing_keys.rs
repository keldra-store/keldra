use super::*;
use personaldb_protocol::{
    Ed25519ProtocolSigner, KeyGeneration, KeyTrustPolicy, ProtocolSigner, SignaturePurpose,
};

#[tokio::test]
async fn personaldb_signing_key_admin_lifecycle_is_authorised_audited_and_public_only() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let mut private_key_pkcs8_der = hex::decode("302e020100300506032b657004220420").unwrap();
    private_key_pkcs8_der.extend([0x91; 32]);
    let signer = Ed25519ProtocolSigner::from_pkcs8_der(
        &private_key_pkcs8_der,
        KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
    )
    .unwrap();
    let trust_record = signer.trust_record().clone();

    let imported = client
        .import_personal_db_signing_key(with_auth(
            tonic::Request::new(ImportPersonalDbSigningKeyRequest {
                context: Some(context("personaldb-signing-key-import", 0)),
                private_key_pkcs8_der,
                public_key: trust_record.public_key.as_bytes().to_vec(),
                key_generation: trust_record.key_generation.get(),
                purpose: trust_record.purpose.as_str().to_string(),
                database_scopes: Vec::new(),
                group_scopes: Vec::new(),
                valid_from_log_index: 0,
                valid_until_log_index: None,
                status: "active".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(imported.runtime_reload_required);
    assert!(!imported.audit_event_id.is_empty());
    let imported_key = imported.key.unwrap();
    assert_eq!(imported_key.key_id, trust_record.key_id.as_str());
    assert_eq!(imported_key.public_key, trust_record.public_key.as_bytes());
    assert_eq!(imported_key.record_revision, 1);

    let listed = client
        .list_personal_db_signing_keys(with_auth(
            tonic::Request::new(ListPersonalDbSigningKeysRequest { page: None }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.keys, vec![imported_key.clone()]);

    let retired = client
        .set_personal_db_signing_key_status(with_auth(
            tonic::Request::new(SetPersonalDbSigningKeyStatusRequest {
                context: Some(context("personaldb-signing-key-retire", 1)),
                key_id: imported_key.key_id.clone(),
                status: "retiring".to_string(),
                valid_until_log_index: Some(100),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(retired.runtime_reload_required);
    let retired_key = retired.key.unwrap();
    assert_eq!(retired_key.status, "retiring");
    assert_eq!(retired_key.valid_until_log_index, Some(100));
    assert_eq!(retired_key.record_revision, 2);

    let stale = client
        .set_personal_db_signing_key_status(with_auth(
            tonic::Request::new(SetPersonalDbSigningKeyStatusRequest {
                context: Some(context("personaldb-signing-key-stale", 1)),
                key_id: imported_key.key_id,
                status: "compromised".to_string(),
                valid_until_log_index: Some(90),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::Aborted);
}
