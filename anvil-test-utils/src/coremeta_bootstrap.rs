use super::*;

pub(super) fn test_admin_context(
    label: &str,
    expected_generation: u64,
) -> anvil::anvil_api::AdminRequestContext {
    anvil::anvil_api::AdminRequestContext {
        request_id: format!("test-{label}-{}", uuid::Uuid::new_v4().simple()),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        audit_reason: format!("test {label}"),
        expected_generation,
    }
}

pub(super) fn install_canonical_coremeta_bootstrap_snapshot(states: &[AppState]) {
    let Some(canonical) = states.first() else {
        return;
    };
    let snapshot = canonical
        .core_store
        .export_portable_coremeta_bootstrap_rows(4096)
        .expect("export canonical CoreMeta bootstrap snapshot");

    for target in states.iter().skip(1) {
        target
            .core_store
            .install_portable_coremeta_bootstrap_rows(&snapshot)
            .expect("install canonical CoreMeta bootstrap snapshot");
    }
}
