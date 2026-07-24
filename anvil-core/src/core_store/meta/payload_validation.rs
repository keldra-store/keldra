use super::*;
use unicode_normalization::UnicodeNormalization;

pub(super) fn validate_coremeta_common(
    common: &CoreMetaRowCommonProto,
    expected_visibility: CoreMetaVisibilityState,
) -> Result<()> {
    validate_coremeta_common_shape(common)?;
    if common.visibility_state_enum() != expected_visibility {
        bail!("CoreMeta row common visibility state mismatch");
    }
    Ok(())
}

pub(super) fn validate_coremeta_common_shape(common: &CoreMetaRowCommonProto) -> Result<()> {
    if common.payload_schema_version != CORE_META_VALUE_SCHEMA_VERSION {
        bail!(
            "CoreMeta row common payload schema version {} is not supported",
            common.payload_schema_version
        );
    }
    match common.visibility_state_enum() {
        CoreMetaVisibilityState::Pending
        | CoreMetaVisibilityState::Committed
        | CoreMetaVisibilityState::Aborted
        | CoreMetaVisibilityState::RolledBack => {}
        CoreMetaVisibilityState::Unspecified => {
            bail!("CoreMeta row common visibility state must be specified");
        }
    }
    if !common.root_key_hash.is_empty() {
        validate_coremeta_hash(&common.root_key_hash, "CoreMeta row common root key hash")?;
        if common.root_generation == 0 {
            bail!("CoreMeta row common rooted rows must use a non-zero root generation");
        }
    }
    if !common.transaction_id.is_empty() {
        validate_coremeta_logical_id(&common.transaction_id, "CoreMeta row common transaction id")?;
    }
    if common.realm_id.as_bytes().contains(&0) || !common.realm_id.chars().eq(common.realm_id.nfc())
    {
        bail!("CoreMeta row common realm must be NFC-normalized text without NUL");
    }
    Ok(())
}

pub(super) fn validate_coremeta_hash(hash: &str, label: &str) -> Result<()> {
    let Some((algorithm, value)) = hash.split_once(':') else {
        bail!("{label} must use algorithm:hex encoding");
    };
    if algorithm.is_empty()
        || value.is_empty()
        || !hash.is_ascii()
        || value.bytes().any(|byte| !byte.is_ascii_hexdigit())
    {
        bail!("{label} must use ASCII algorithm:hex encoding");
    }
    Ok(())
}

pub(super) fn validate_coremeta_logical_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{label} must not be empty");
    }
    if value.len() > 512 {
        bail!("{label} is too long");
    }
    if value.as_bytes().contains(&0) || !value.chars().eq(value.nfc()) {
        bail!("{label} must be NFC-normalized text without NUL");
    }
    Ok(())
}

pub fn core_meta_payload_digest(table_id: u16, payload: &[u8]) -> String {
    core_meta_payload_hash(table_id, CORE_META_VALUE_SCHEMA_VERSION, payload)
}

pub(super) fn core_meta_payload_hash(table_id: u16, schema_version: u32, payload: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(b"anvil.coremeta.value.v1");
    hasher.update(&[0]);
    hasher.update(&table_id.to_le_bytes());
    hasher.update(&schema_version.to_le_bytes());
    hasher.update(payload);
    format!("blake3:{}", hasher.finalize().to_hex())
}
