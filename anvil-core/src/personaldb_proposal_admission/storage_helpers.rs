use super::codec::*;
use super::*;
use crate::{
    core_store::{CoreMetaStore, CoreMutationRootPublication},
    formats::writer::WriterFamily,
};

pub(super) async fn next_group_root_generation(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
) -> Result<u64> {
    let anchor_key = personaldb_root_anchor_key(tenant_id, database_id);
    let anchor = CoreStore::new(storage.clone())
        .await?
        .read_internal_root_anchor(&anchor_key, 1)
        .await
        .context("read PersonalDB group root anchor")?;
    let expected_root_key_hash = personaldb_root_key_hash(tenant_id, database_id);
    if anchor.root_key_hash != expected_root_key_hash {
        bail!("PersonalDB group root anchor scope mismatch");
    }
    anchor
        .generation
        .checked_add(1)
        .ok_or_else(|| anyhow!("PersonalDB group root generation overflow"))
}

pub(super) async fn commit_group_batch(
    storage: &Storage,
    transaction_id: String,
    tenant_id: i64,
    database_id: &str,
    principal: &str,
    preconditions: Vec<CoreMutationPrecondition>,
    operations: Vec<CoreMutationOperation>,
) -> Result<()> {
    let scope_partition = personaldb_partition_id(tenant_id, database_id);
    let receipt = CoreStore::new(storage.clone())
        .await?
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: scope_partition.clone(),
            committed_by_principal: principal.to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    scope_partition,
                    WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
                CoreMutationRootPublication::new(
                    personaldb_root_anchor_key(tenant_id, database_id),
                    WriterFamily::PersonalDb.as_str(),
                ),
            ],
            preconditions,
            operations,
        })
        .await?;
    ensure_committed_receipt(&receipt)
}

pub(super) fn ensure_committed_receipt(receipt: &CoreMutationBatchReceipt) -> Result<()> {
    if receipt.state != CoreTransactionState::Committed {
        bail!(
            "PersonalDB admission CoreStore transaction {} did not commit: {}",
            receipt.transaction_id,
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation failure")
        );
    }
    Ok(())
}

pub(super) fn put_operation(
    tenant_id: i64,
    database_id: &str,
    table_id: u16,
    tuple_key: Vec<u8>,
    payload: Vec<u8>,
) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaPut {
        partition_id: personaldb_partition_id(tenant_id, database_id),
        cf: CF_PERSONALDB.to_string(),
        table_id,
        tuple_key,
        payload,
    }
}

pub(super) fn absent_precondition(table_id: u16, tuple_key: Vec<u8>) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaRow {
        cf: CF_PERSONALDB.to_string(),
        table_id,
        tuple_key,
        expected_payload_hash: None,
        require_absent: true,
        require_present: false,
    }
}

pub(super) fn exact_precondition(
    table_id: u16,
    tuple_key: Vec<u8>,
    payload: &[u8],
) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaRow {
        cf: CF_PERSONALDB.to_string(),
        table_id,
        tuple_key,
        expected_payload_hash: Some(core_meta_payload_digest(table_id, payload)),
        require_absent: false,
        require_present: true,
    }
}

pub(super) fn read_raw_row(
    storage: &Storage,
    table_id: u16,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    // Proposal claims, slots, reservations, and witness candidates are local
    // admission state read to construct exact mutation preconditions.
    CoreMetaStore::open(storage.core_store_meta_path())?.get(CF_PERSONALDB, table_id, key)
}

pub(super) fn read_claim_row(
    storage: &Storage,
    key: &[u8],
) -> Result<Option<ProposalIdempotencyClaimIdentityV1>> {
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW, key)? else {
        return Ok(None);
    };
    let row =
        decode_deterministic_proto::<ClaimRowProto>(&payload, "proposal idempotency claim row")?;
    let claim = claim_from_proto(
        row.claim
            .ok_or_else(|| anyhow!("proposal idempotency claim row missing claim"))?,
    )?;
    let common = row
        .common
        .ok_or_else(|| anyhow!("proposal idempotency claim row missing CoreMeta common"))?;
    validate_row_scope(&common, parse_claim_tenant_id(&claim)?, &claim.database_id)?;
    validate_claim(&claim)?;
    Ok(Some(claim))
}

pub(super) fn read_slot_row(
    storage: &Storage,
    key: &[u8],
) -> Result<Option<ProposalAdmissionSlotV1>> {
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_PROPOSAL_SLOT_ROW, key)? else {
        return Ok(None);
    };
    let (_common, slot) = decode_slot_row(&payload)?;
    validate_slot(&slot)?;
    Ok(Some(slot))
}

pub(super) fn read_reservation_row(
    storage: &Storage,
    key: &[u8],
) -> Result<Option<(i64, ProposalAdmissionReservationV1)>> {
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW, key)?
    else {
        return Ok(None);
    };
    let (common, reservation) = decode_reservation_row(&payload)?;
    let tenant_id = tenant_id_from_realm(&common.realm_id)?;
    validate_row_scope(&common, tenant_id, &reservation.identity.database_id)?;
    validate_reservation(&reservation)?;
    Ok(Some((tenant_id, reservation)))
}

pub(super) fn read_candidate_row(
    storage: &Storage,
    key: &[u8],
) -> Result<Option<WitnessSigningCandidateV1>> {
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW, key)? else {
        return Ok(None);
    };
    let (_common, candidate) = decode_candidate_row(&payload)?;
    validate_candidate(&candidate)?;
    Ok(Some(candidate))
}

pub(super) fn decode_slot_row(
    payload: &[u8],
) -> Result<(CoreMetaRowCommonProto, ProposalAdmissionSlotV1)> {
    let row = decode_deterministic_proto::<SlotRowProto>(payload, "proposal admission slot row")?;
    Ok((
        row.common
            .ok_or_else(|| anyhow!("proposal slot row missing CoreMeta common"))?,
        slot_from_proto(
            row.slot
                .ok_or_else(|| anyhow!("proposal slot row missing slot"))?,
        )?,
    ))
}

pub(super) fn decode_reservation_row(
    payload: &[u8],
) -> Result<(CoreMetaRowCommonProto, ProposalAdmissionReservationV1)> {
    let row =
        decode_deterministic_proto::<ReservationRowProto>(payload, "proposal reservation row")?;
    Ok((
        row.common
            .ok_or_else(|| anyhow!("proposal reservation row missing CoreMeta common"))?,
        reservation_from_proto(
            row.reservation
                .ok_or_else(|| anyhow!("proposal reservation row missing reservation"))?,
        )?,
    ))
}

pub(super) fn decode_candidate_row(
    payload: &[u8],
) -> Result<(CoreMetaRowCommonProto, WitnessSigningCandidateV1)> {
    let row =
        decode_deterministic_proto::<CandidateRowProto>(payload, "witness signing candidate row")?;
    Ok((
        row.common
            .ok_or_else(|| anyhow!("witness candidate row missing CoreMeta common"))?,
        candidate_from_proto(
            row.candidate
                .ok_or_else(|| anyhow!("witness candidate row missing candidate"))?,
        )?,
    ))
}

pub(super) fn decode_receipt_row(
    payload: &[u8],
) -> Result<(CoreMetaRowCommonProto, WitnessDualSigningReceiptV1)> {
    let row =
        decode_deterministic_proto::<ReceiptRowProto>(payload, "witness dual-signing receipt row")?;
    Ok((
        row.common
            .ok_or_else(|| anyhow!("witness receipt row missing CoreMeta common"))?,
        receipt_from_proto(
            row.receipt
                .ok_or_else(|| anyhow!("witness receipt row missing receipt"))?,
        )?,
    ))
}

pub(super) fn row_common(
    tenant_id: i64,
    database_id: &str,
    root_generation: u64,
    transaction_id: &str,
    created_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        personaldb_realm_id(tenant_id),
        personaldb_root_key_hash(tenant_id, database_id),
        root_generation,
        transaction_id.to_string(),
        created_at_unix_nanos,
    )
}

pub(super) fn validate_row_scope(
    common: &CoreMetaRowCommonProto,
    tenant_id: i64,
    database_id: &str,
) -> Result<()> {
    if common.realm_id != personaldb_realm_id(tenant_id)
        || common.root_key_hash != personaldb_root_key_hash(tenant_id, database_id)
        || common.root_generation == 0
    {
        bail!("PersonalDB admission CoreMeta row scope mismatch");
    }
    Ok(())
}

pub(super) fn claim_key(claim: &ProposalIdempotencyClaimIdentityV1) -> Result<Vec<u8>> {
    let mut hasher = Sha256::new();
    for part in [
        claim.tenant_id.as_bytes(),
        claim.application_id.as_bytes(),
        claim.operation_id.as_bytes(),
        claim.request_id.as_bytes(),
        claim.database_id.as_bytes(),
    ] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(CLAIM_KEY_PREFIX),
        CoreMetaTuplePart::Hash(&digest),
    ])
}

pub(super) fn slot_key(
    tenant_id: i64,
    database_id: &str,
    next_log_index: u64,
    client_log_epoch: u64,
) -> Result<Vec<u8>> {
    validate_tenant_database(tenant_id, database_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(SLOT_KEY_PREFIX),
        CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8(database_id),
        CoreMetaTuplePart::U64(next_log_index),
        CoreMetaTuplePart::U64(client_log_epoch),
    ])
}

pub(super) fn reservation_key(reservation_id: &str) -> Result<Vec<u8>> {
    validate_reservation_id(reservation_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(RESERVATION_KEY_PREFIX),
        CoreMetaTuplePart::Hash(reservation_id),
    ])
}

pub(super) fn candidate_key(
    tenant_id: i64,
    database_id: &str,
    next_log_index: u64,
    client_log_epoch: u64,
    reservation_id: &str,
) -> Result<Vec<u8>> {
    validate_tenant_database(tenant_id, database_id)?;
    validate_reservation_id(reservation_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(CANDIDATE_KEY_PREFIX),
        CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8(database_id),
        CoreMetaTuplePart::U64(next_log_index),
        CoreMetaTuplePart::U64(client_log_epoch),
        CoreMetaTuplePart::Hash(reservation_id),
    ])
}

pub(super) fn receipt_key(reservation_id: &str) -> Result<Vec<u8>> {
    validate_reservation_id(reservation_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(RECEIPT_KEY_PREFIX),
        CoreMetaTuplePart::Hash(reservation_id),
    ])
}

pub(super) fn domain_hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    hasher.finalize().into()
}

pub(super) fn unix_seconds_to_nanos(seconds: i64) -> Result<u64> {
    let seconds = u64::try_from(seconds).context("protocol timestamp must be nonnegative")?;
    seconds
        .checked_mul(1_000_000_000)
        .ok_or_else(|| anyhow!("protocol timestamp nanoseconds overflow"))
}
