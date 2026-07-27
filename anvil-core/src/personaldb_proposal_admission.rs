//! PersonalDB committed-head product row.
//!
//! The former witness proposal-admission protocol lived in this module. Public
//! writes are now authorized and certified by the compact-Raft MVCC assignment,
//! leaving only the committed-head product-data helpers.

use crate::{
    core_store::{
        CF_PERSONALDB, CoreMetaTuplePart, TABLE_PERSONALDB_GROUP_ROW, core_meta_tuple_key,
    },
    personaldb_coremeta::{PersonalDbWritePlan, personaldb_realm_id},
    personaldb_heads::{PersonalDbCommittedHead, decode_committed_head, encode_committed_head},
};
use anyhow::{Result, anyhow, bail};
use personaldb_protocol::PublicKeyTrustStore;

pub fn read_personaldb_committed_head_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    trust_store: &PublicKeyTrustStore,
) -> Result<Option<PersonalDbCommittedHead>> {
    Ok(read_committed_head_mvcc_at_snapshot(
        mvcc,
        tenant_id,
        database_id,
        trust_store,
        mvcc.runtime.applied_version()?,
    )?
    .map(|(_, head)| head))
}

pub fn read_personaldb_committed_head_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    trust_store: &PublicKeyTrustStore,
    snapshot_version: u64,
) -> Result<Option<PersonalDbCommittedHead>> {
    Ok(read_committed_head_mvcc_at_snapshot(
        mvcc,
        tenant_id,
        database_id,
        trust_store,
        snapshot_version,
    )?
    .map(|(_, head)| head))
}

pub fn stage_personaldb_committed_head_mvcc(
    plan: &mut PersonalDbWritePlan,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    expected: &PersonalDbCommittedHead,
    next: &PersonalDbCommittedHead,
    trust_store: &PublicKeyTrustStore,
) -> Result<()> {
    expected.verify(trust_store)?;
    next.verify(trust_store)?;
    let tuple = committed_head_key(tenant_id, database_id)?;
    let (payload, current) = read_committed_head_mvcc_at_snapshot(
        mvcc,
        tenant_id,
        database_id,
        trust_store,
        mvcc.runtime.applied_version()?,
    )?
    .ok_or_else(|| anyhow!("PersonalDB committed head is absent"))?;
    if current != *expected {
        bail!("PersonalDB committed head changed before staging");
    }
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_PERSONALDB,
        TABLE_PERSONALDB_GROUP_ROW,
        &tuple,
    )?;
    plan.stage_put(
        key,
        encode_committed_head(next)?,
        crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(&payload).as_bytes()),
    );
    Ok(())
}

pub fn stage_personaldb_committed_head_seed(
    plan: &mut PersonalDbWritePlan,
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbCommittedHead,
    trust_store: &PublicKeyTrustStore,
) -> Result<()> {
    head.verify(trust_store)?;
    let tuple = committed_head_key(tenant_id, database_id)?;
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_PERSONALDB,
        TABLE_PERSONALDB_GROUP_ROW,
        &tuple,
    )?;
    plan.stage_put(
        key,
        encode_committed_head(head)?,
        crate::mvcc_transaction::PredicateKind::Absent,
    );
    Ok(())
}

fn committed_head_key(tenant_id: i64, database_id: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8("committed-head-current"),
        CoreMetaTuplePart::Utf8(database_id),
    ])
}

fn read_committed_head_mvcc_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    trust_store: &PublicKeyTrustStore,
    snapshot_version: u64,
) -> Result<Option<(Vec<u8>, PersonalDbCommittedHead)>> {
    let tuple = committed_head_key(tenant_id, database_id)?;
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_PERSONALDB,
        TABLE_PERSONALDB_GROUP_ROW,
        &tuple,
    )?;
    let Some(payload) = mvcc
        .runtime
        .read_at(&key, snapshot_version)?
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
