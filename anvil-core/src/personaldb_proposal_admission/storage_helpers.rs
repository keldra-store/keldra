use super::codec::*;
use super::*;
use crate::mvcc_transaction::{DurabilityLevel, PredicateKind, ReadConsistency};

pub(super) async fn next_group_root_generation(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
) -> Result<u64> {
    mvcc.runtime
        .applied_version()?
        .checked_add(1)
        .ok_or_else(|| anyhow!("PersonalDB group root generation overflow"))
}

pub(super) async fn commit_group_batch(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: String,
    tenant_id: i64,
    database_id: &str,
    principal: &str,
    preconditions: Vec<CoreMutationPrecondition>,
    operations: Vec<CoreMutationOperation>,
) -> Result<()> {
    let assignment = mvcc
        .reconcile_work_assignment(
            PERSONALDB_GROUP_PARTITION_FAMILY,
            &personaldb_partition_id(tenant_id, database_id),
        )
        .await?
        .ok_or_else(|| anyhow!("local node does not own the PersonalDB group assignment"))?;
    let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            principal,
            &transaction_id,
            std::time::Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            now,
        )
        .await?;
    let mut product_mutations = Vec::with_capacity(operations.len());
    for operation in operations {
        match operation {
            CoreMutationOperation::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } => {
                product_mutations.push(crate::mvcc_product::ProductMutation::put(
                    crate::mvcc_product::coremeta_logical_key(&cf, table_id, &tuple_key)?,
                    payload,
                ));
            }
            CoreMutationOperation::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } => {
                product_mutations.push(crate::mvcc_product::ProductMutation::delete(
                    crate::mvcc_product::coremeta_logical_key(&cf, table_id, &tuple_key)?,
                ));
            }
            CoreMutationOperation::StreamAppend { .. } => {
                bail!("PersonalDB admission MVCC transaction received a physical stream append")
            }
        }
    }
    mvcc.stage_product_mutations(&handle.transaction_id, principal, product_mutations, now)?;
    for precondition in preconditions {
        let CoreMutationPrecondition::CoreMetaRow {
            cf,
            table_id,
            tuple_key,
            expected_payload_hash,
            require_absent,
            require_present,
        } = precondition
        else {
            bail!("PersonalDB admission requires MVCC-compatible row predicates");
        };
        let key = crate::mvcc_product::coremeta_logical_key(&cf, table_id, &tuple_key)?;
        let current = mvcc.read_latest_value(&key)?;
        let kind = if require_absent {
            PredicateKind::Absent
        } else if require_present {
            let current = current.ok_or_else(|| anyhow!("PersonalDB predicate row is missing"))?;
            if expected_payload_hash.as_deref()
                != Some(core_meta_payload_digest(table_id, &current).as_str())
            {
                bail!("PersonalDB exact predicate payload changed");
            }
            PredicateKind::ValueHash(*blake3::hash(&current).as_bytes())
        } else {
            PredicateKind::Exists
        };
        mvcc.stage_predicate(&handle.transaction_id, principal, key, kind, now)?;
    }
    mvcc.stage_assignment_guard(&handle.transaction_id, principal, &assignment, now)?;
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            principal,
            now,
        )
        .await?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
            bail!("PersonalDB admission MVCC transaction aborted: {reason:?}")
        }
    }
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

pub(super) fn committed_head_key(tenant_id: i64, database_id: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8("committed-head-current"),
        CoreMetaTuplePart::Utf8(database_id),
    ])
}

pub(super) fn read_committed_head_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    trust_store: &PublicKeyTrustStore,
) -> Result<Option<(Vec<u8>, PersonalDbCommittedHead)>> {
    read_committed_head_mvcc_at_snapshot(
        mvcc,
        tenant_id,
        database_id,
        trust_store,
        mvcc.runtime.applied_version()?,
    )
}

pub(super) fn read_committed_head_mvcc_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    trust_store: &PublicKeyTrustStore,
    snapshot_version: u64,
) -> Result<Option<(Vec<u8>, PersonalDbCommittedHead)>> {
    let key = committed_head_key(tenant_id, database_id)?;
    let logical_key =
        crate::mvcc_product::coremeta_logical_key(CF_PERSONALDB, TABLE_PERSONALDB_GROUP_ROW, &key)?;
    let Some(payload) = mvcc
        .runtime
        .read_at(&logical_key, snapshot_version)?
        .map(|row| row.value)
    else {
        return Ok(None);
    };
    let head = decode_committed_head(&payload)?;
    head.verify(trust_store)?;
    if head.tenant_id != tenant_id.to_string() || head.database_id != database_id {
        bail!("PersonalDB committed head MVCC scope mismatch");
    }
    Ok(Some((payload, head)))
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    table_id: u16,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    // Proposal claims, slots, reservations, and witness candidates are local
    // admission state read to construct exact mutation preconditions.
    mvcc.read_latest_value(&crate::mvcc_product::coremeta_logical_key(
        CF_PERSONALDB,
        table_id,
        key,
    )?)
}

pub(super) fn read_claim_row(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    key: &[u8],
) -> Result<Option<ProposalIdempotencyClaimIdentityV1>> {
    let Some(payload) = read_raw_row(mvcc, TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW, key)? else {
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    key: &[u8],
) -> Result<Option<ProposalAdmissionSlotV1>> {
    let Some(payload) = read_raw_row(mvcc, TABLE_PERSONALDB_PROPOSAL_SLOT_ROW, key)? else {
        return Ok(None);
    };
    let (_common, slot) = decode_slot_row(&payload)?;
    validate_slot(&slot)?;
    Ok(Some(slot))
}

pub(super) fn read_reservation_row(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    key: &[u8],
) -> Result<Option<(i64, ProposalAdmissionReservationV1)>> {
    let Some(payload) = read_raw_row(mvcc, TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW, key)? else {
        return Ok(None);
    };
    let (common, reservation) = decode_reservation_row(&payload)?;
    let tenant_id = tenant_id_from_realm(&common.realm_id)?;
    validate_row_scope(&common, tenant_id, &reservation.identity.database_id)?;
    validate_reservation(&reservation)?;
    Ok(Some((tenant_id, reservation)))
}

pub(super) fn read_candidate_row(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    key: &[u8],
) -> Result<Option<WitnessSigningCandidateV1>> {
    let Some(payload) = read_raw_row(mvcc, TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW, key)? else {
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
