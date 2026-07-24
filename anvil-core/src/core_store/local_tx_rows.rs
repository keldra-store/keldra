use super::*;
use crate::formats::writer::WriterFamily;
use prost::{Message, Oneof};

#[path = "local_tx_rows/publication_guard.rs"]
mod publication_guard;

pub(super) use publication_guard::{
    CorePublicationGuardContext, CorePublicationGuardSummary, hydrate_publication_guard_context,
    publication_guard_summary,
};

const CORE_TRANSACTION_HEADER_ROW_SCHEMA: &str = "anvil.core.transaction_header_row.v1";
const CORE_TRANSACTION_STAGED_UPDATE_ROW_SCHEMA: &str =
    "anvil.core.transaction_staged_update_row.v1";
const CORE_TRANSACTION_PRECONDITION_ROW_SCHEMA: &str = "anvil.core.transaction_precondition_row.v3";
// Transaction rows may retain tiny CoreMeta payloads for recovery, but larger
// staged payloads must stay in the CoreStore byte pipeline as locators.
const CORE_TRANSACTION_STAGED_INLINE_PAYLOAD_BYTES: usize = 16 * 1024;

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionHeaderRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    transaction_id: String,
    #[prost(string, tag = "4")]
    scope_partition: String,
    #[prost(enumeration = "CoreTransactionHeaderStateProto", tag = "5")]
    state: i32,
    #[prost(string, tag = "6")]
    preconditions_hash: String,
    #[prost(string, tag = "7")]
    operations_hash: String,
    #[prost(uint64, tag = "8")]
    visible_update_count: u64,
    #[prost(uint64, tag = "9")]
    precondition_count: u64,
    #[prost(string, optional, tag = "10")]
    finalisation_error: Option<String>,
    #[prost(string, tag = "11")]
    committed_at: String,
    #[prost(string, tag = "12")]
    committed_by_principal: String,
    #[prost(uint64, tag = "13")]
    created_at_unix_nanos: u64,
    #[prost(uint64, tag = "14")]
    expires_at_unix_nanos: u64,
    #[prost(string, tag = "15")]
    root_anchor_key: String,
    #[prost(string, tag = "16")]
    root_key_hash: String,
    #[prost(uint64, optional, tag = "17")]
    committed_root_generation: Option<u64>,
    #[prost(string, tag = "18")]
    purpose: String,
    #[prost(string, optional, tag = "19")]
    failure_evidence: Option<String>,
    #[prost(string, tag = "20")]
    outcome: String,
    #[prost(string, repeated, tag = "21")]
    writer_families: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
enum CoreTransactionHeaderStateProto {
    Unspecified = 0,
    Open = 1,
    Prepared = 2,
    Committed = 3,
    FinalisationFailed = 4,
    Aborted = 5,
    RolledBack = 6,
    Expired = 7,
    Failed = 8,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionStagedUpdateRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    transaction_id: String,
    #[prost(uint64, tag = "4")]
    ordinal: u64,
    #[prost(message, optional, tag = "5")]
    update: Option<CoreTransactionUpdateRowProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionUpdateRowProto {
    #[prost(oneof = "transaction_update_row_proto::Kind", tags = "2, 3, 4")]
    kind: Option<transaction_update_row_proto::Kind>,
}

mod transaction_update_row_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(message, tag = "2")]
        StreamAppend(super::CoreTransactionStreamAppendRowProto),
        #[prost(message, tag = "3")]
        CoreMetaPut(super::CoreTransactionCoreMetaPutRowProto),
        #[prost(message, tag = "4")]
        CoreMetaDelete(super::CoreTransactionCoreMetaDeleteRowProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionStreamAppendRowProto {
    #[prost(string, tag = "1")]
    stream_id: String,
    #[prost(uint64, tag = "2")]
    visible_sequence: u64,
    #[prost(string, tag = "3")]
    prepared_record_hash: String,
    #[prost(string, tag = "4")]
    partition_id: String,
    #[prost(string, tag = "5")]
    record_kind: String,
    #[prost(message, optional, tag = "6")]
    payload_ref: Option<CoreTransactionPayloadRefProto>,
    #[prost(string, optional, tag = "7")]
    idempotency_key_hash: Option<String>,
    #[prost(string, tag = "8")]
    previous_event_hash: String,
    #[prost(string, tag = "9")]
    created_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionCoreMetaPutRowProto {
    #[prost(string, tag = "1")]
    cf: String,
    #[prost(uint32, tag = "2")]
    table_id: u32,
    #[prost(bytes, tag = "3")]
    tuple_key: Vec<u8>,
    #[prost(string, optional, tag = "4")]
    previous_payload_hash: Option<String>,
    #[prost(string, tag = "5")]
    payload_hash: String,
    #[prost(message, optional, tag = "6")]
    payload_ref: Option<CoreTransactionPayloadRefProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionCoreMetaDeleteRowProto {
    #[prost(string, tag = "1")]
    cf: String,
    #[prost(uint32, tag = "2")]
    table_id: u32,
    #[prost(bytes, tag = "3")]
    tuple_key: Vec<u8>,
    #[prost(string, optional, tag = "4")]
    previous_payload_hash: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionPayloadRefProto {
    #[prost(bytes, optional, tag = "1")]
    inline_payload: Option<Vec<u8>>,
    #[prost(message, optional, tag = "2")]
    locator: Option<CoreMetaLocatorProto>,
    #[prost(string, tag = "3")]
    payload_sha256: String,
    #[prost(uint64, tag = "4")]
    payload_length: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionPreconditionRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    transaction_id: String,
    #[prost(uint64, tag = "4")]
    ordinal: u64,
    #[prost(message, optional, tag = "5")]
    precondition: Option<CoreMutationPreconditionRowProto>,
    #[prost(uint64, tag = "6")]
    visible_update_boundary: u64,
    #[prost(bool, tag = "7")]
    revalidate_at_publication: bool,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationPreconditionRowProto {
    #[prost(oneof = "mutation_precondition_row_proto::Kind", tags = "2, 3, 4, 5")]
    kind: Option<mutation_precondition_row_proto::Kind>,
}

mod mutation_precondition_row_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(message, tag = "2")]
        Fence(super::CoreMutationFencePreconditionRowProto),
        #[prost(message, tag = "3")]
        StreamHead(super::CoreMutationStreamHeadPreconditionRowProto),
        #[prost(message, tag = "4")]
        CoreMetaRow(super::CoreMutationCoreMetaRowPreconditionRowProto),
        #[prost(message, tag = "5")]
        CoreMetaLease(super::CoreMutationCoreMetaLeasePreconditionRowProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct CoreFencePreconditionRowProto {
    #[prost(string, tag = "1")]
    fence_name: String,
    #[prost(uint64, tag = "2")]
    fence_token: u64,
    #[prost(string, tag = "3")]
    authenticated_principal: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationFencePreconditionRowProto {
    #[prost(string, tag = "1")]
    fence_name: String,
    #[prost(uint64, tag = "2")]
    fence_token: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationCoreMetaRowPreconditionRowProto {
    #[prost(string, tag = "1")]
    cf: String,
    #[prost(uint32, tag = "2")]
    table_id: u32,
    #[prost(bytes, tag = "3")]
    tuple_key: Vec<u8>,
    #[prost(string, optional, tag = "4")]
    expected_payload_hash: Option<String>,
    #[prost(bool, tag = "5")]
    require_absent: bool,
    #[prost(bool, tag = "6")]
    require_present: bool,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationCoreMetaLeasePreconditionRowProto {
    #[prost(string, tag = "1")]
    cf: String,
    #[prost(uint32, tag = "2")]
    table_id: u32,
    #[prost(bytes, tag = "3")]
    tuple_key: Vec<u8>,
    #[prost(string, tag = "4")]
    expected_payload_hash: String,
    #[prost(uint64, tag = "5")]
    expires_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationStreamHeadPreconditionRowProto {
    #[prost(string, tag = "1")]
    stream_id: String,
    #[prost(uint64, tag = "2")]
    expected_last_sequence: u64,
    #[prost(string, tag = "3")]
    expected_last_event_hash: String,
}

pub(super) struct CoreTransactionHeaderRow {
    pub transaction: CoreTransaction,
    pub visible_update_count: u64,
    pub precondition_count: u64,
}

pub(super) enum CoreTransactionPayloadRef {
    Inline {
        payload: Vec<u8>,
        payload_sha256: String,
        payload_length: u64,
    },
    Locator {
        locator: CoreMetaLocatorProto,
        payload_sha256: String,
        payload_length: u64,
    },
}

pub(super) enum CoreTransactionUpdateRow {
    StreamAppend {
        partition_id: String,
        stream_id: String,
        record_kind: String,
        payload_ref: CoreTransactionPayloadRef,
        idempotency_key_hash: Option<String>,
        visible_sequence: u64,
        previous_event_hash: String,
        prepared_record_hash: String,
        created_at: String,
    },
    CoreMetaPut {
        cf: String,
        table_id: u16,
        tuple_key: Vec<u8>,
        previous_payload_hash: Option<String>,
        payload_hash: String,
        payload_ref: CoreTransactionPayloadRef,
    },
    CoreMetaDelete {
        cf: String,
        table_id: u16,
        tuple_key: Vec<u8>,
        previous_payload_hash: Option<String>,
    },
}

pub(super) struct CoreTransactionStagedUpdateRow {
    pub ordinal: u64,
    pub update: CoreTransactionUpdateRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CoreTransactionPreconditionRow {
    ordinal: u64,
    pub(super) visible_update_boundary: u64,
    pub(super) precondition: CoreMutationPrecondition,
    // Recovery retains the admitted precondition as audit evidence after an
    // exact effect is durable, but must not evaluate the before-state again.
    pub(super) revalidate_at_publication: bool,
}

pub(super) enum OwnedCoreMetaBatchOp {
    Put {
        cf: &'static str,
        table_id: u16,
        tuple_key: Vec<u8>,
        payload: Vec<u8>,
        common: Option<CoreMetaRowCommonProto>,
    },
    Delete {
        cf: &'static str,
        table_id: u16,
        tuple_key: Vec<u8>,
        common: Option<CoreMetaRowCommonProto>,
    },
}

impl CoreStore {
    pub(super) fn has_pending_transaction_stream_prefix(
        &self,
        stream_prefix: &str,
    ) -> Result<bool> {
        Ok(!self
            .meta
            .scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_EXPLICIT_TRANSACTION_ROW,
                &pending_transaction_stream_prefix(stream_prefix),
                None,
                1,
            )?
            .is_empty())
    }

    pub(super) async fn write_pending_transaction_with_staged_rows_unlocked(
        &self,
        transaction: &CoreTransaction,
        new_preconditions: &[CoreMutationPrecondition],
    ) -> Result<()> {
        if transaction.state == CoreTransactionState::Committed {
            bail!("CoreStore committed transactions must use root publication");
        }
        if transaction.committed_root_generation.is_some() {
            bail!("CoreStore unpublished transaction must not name a root generation");
        }
        let owned_ops = if matches!(
            transaction.state,
            CoreTransactionState::Open | CoreTransactionState::Prepared
        ) {
            self.transaction_rows_as_coremeta_ops_unlocked(transaction, new_preconditions, true)
                .await?
        } else {
            let mut preconditions = self
                .read_transaction_preconditions_unlocked(&transaction.transaction_id)
                .await?;
            let visible_update_boundary = u64::try_from(transaction.visible_updates.len())
                .map_err(|_| anyhow!("CoreStore transaction has too many staged updates"))?;
            let first_ordinal = u64::try_from(preconditions.len())
                .map_err(|_| anyhow!("CoreStore transaction has too many staged preconditions"))?;
            for (offset, precondition) in new_preconditions.iter().cloned().enumerate() {
                let offset = u64::try_from(offset).map_err(|_| {
                    anyhow!("CoreStore transaction precondition ordinal exceeds u64")
                })?;
                let ordinal = first_ordinal.checked_add(offset).ok_or_else(|| {
                    anyhow!("CoreStore transaction precondition ordinal overflow")
                })?;
                preconditions.push(CoreTransactionPreconditionRow {
                    ordinal,
                    visible_update_boundary,
                    precondition,
                    revalidate_at_publication: true,
                });
            }
            self.complete_transaction_rows_as_coremeta_ops_unlocked(transaction, &preconditions)
                .await?
        };
        let ops = borrow_owned_coremeta_batch_ops(&owned_ops);
        self.meta.write_local_committed_batch(&ops)?;
        Ok(())
    }

    pub(super) async fn transaction_rows_as_coremeta_ops_unlocked(
        &self,
        transaction: &CoreTransaction,
        new_preconditions: &[CoreMutationPrecondition],
        revalidate_at_publication: bool,
    ) -> Result<Vec<OwnedCoreMetaBatchOp>> {
        validate_transaction_root_scope(transaction)?;
        validate_logical_id(&transaction.transaction_id, "transaction id")?;
        let existing_header =
            self.read_transaction_header_row_unlocked(&transaction.transaction_id)?;
        let existing_update_count = existing_header
            .as_ref()
            .map(|header| header.visible_update_count)
            .unwrap_or(0);
        let existing_precondition_count = existing_header
            .as_ref()
            .map(|header| header.precondition_count)
            .unwrap_or(0);
        let visible_update_count = u64::try_from(transaction.visible_updates.len())
            .map_err(|_| anyhow!("CoreStore transaction has too many staged updates"))?;
        let new_precondition_count = u64::try_from(new_preconditions.len())
            .map_err(|_| anyhow!("CoreStore transaction has too many staged preconditions"))?;
        let precondition_count = existing_precondition_count.saturating_add(new_precondition_count);

        let header_key = transaction_header_tuple_key(&transaction.transaction_id)?;
        let header_payload =
            encode_transaction_header_row(transaction, visible_update_count, precondition_count)?;
        let mut owned_ops = vec![OwnedCoreMetaBatchOp::Put {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
            tuple_key: header_key,
            payload: header_payload,
            common: None,
        }];

        let first_new_update = if existing_header.is_some() {
            match self
                .read_transaction_from_rows_unlocked(&transaction.transaction_id)
                .await?
            {
                Some(existing)
                    if transaction_updates_have_prefix(
                        &transaction.visible_updates,
                        &existing.visible_updates,
                    ) =>
                {
                    existing_update_count.min(visible_update_count)
                }
                Some(_) => 0,
                None => 0,
            }
        } else {
            0
        };
        for ordinal in first_new_update..visible_update_count {
            let update = transaction
                .visible_updates
                .get(usize::try_from(ordinal).map_err(|_| {
                    anyhow!("CoreStore transaction staged update ordinal exceeds usize")
                })?)
                .ok_or_else(|| anyhow!("CoreStore transaction staged update is missing"))?;
            let row = self
                .transaction_update_row_from_update(transaction, ordinal, update)
                .await?;
            let payload = encode_transaction_update_row(transaction, ordinal, &row)?;
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
                tuple_key: transaction_update_tuple_key(&transaction.transaction_id, ordinal)?,
                payload: payload.clone(),
                common: None,
            });
            if matches!(
                transaction.state,
                CoreTransactionState::Open | CoreTransactionState::Prepared
            ) && let CoreTransactionUpdate::StreamAppend { stream_id, .. } = update
            {
                owned_ops.push(OwnedCoreMetaBatchOp::Put {
                    cf: CF_TRANSACTIONS,
                    table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
                    tuple_key: pending_transaction_stream_key(
                        stream_id,
                        &transaction.transaction_id,
                        ordinal,
                    ),
                    payload,
                    common: None,
                });
            }
        }

        for (offset, precondition) in new_preconditions.iter().enumerate() {
            let ordinal = existing_precondition_count.saturating_add(offset as u64);
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
                tuple_key: transaction_precondition_tuple_key(
                    &transaction.transaction_id,
                    ordinal,
                )?,
                payload: encode_transaction_precondition_row(
                    transaction,
                    ordinal,
                    existing_update_count,
                    precondition,
                    revalidate_at_publication,
                )?,
                common: None,
            });
        }

        Ok(owned_ops)
    }

    pub(super) async fn complete_transaction_rows_as_coremeta_ops_unlocked(
        &self,
        transaction: &CoreTransaction,
        preconditions: &[CoreTransactionPreconditionRow],
    ) -> Result<Vec<OwnedCoreMetaBatchOp>> {
        validate_transaction_root_scope(transaction)?;
        validate_logical_id(&transaction.transaction_id, "transaction id")?;
        let visible_update_count = u64::try_from(transaction.visible_updates.len())
            .map_err(|_| anyhow!("CoreStore transaction has too many staged updates"))?;
        let precondition_count = u64::try_from(preconditions.len())
            .map_err(|_| anyhow!("CoreStore transaction has too many staged preconditions"))?;
        let mut owned_ops = vec![OwnedCoreMetaBatchOp::Put {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
            tuple_key: transaction_header_tuple_key(&transaction.transaction_id)?,
            payload: encode_transaction_header_row(
                transaction,
                visible_update_count,
                precondition_count,
            )?,
            common: None,
        }];
        for (ordinal, update) in transaction.visible_updates.iter().enumerate() {
            let ordinal = u64::try_from(ordinal)
                .map_err(|_| anyhow!("CoreStore transaction update ordinal exceeds u64"))?;
            let row = self
                .transaction_update_row_from_update(transaction, ordinal, update)
                .await?;
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
                tuple_key: transaction_update_tuple_key(&transaction.transaction_id, ordinal)?,
                payload: encode_transaction_update_row(transaction, ordinal, &row)?,
                common: None,
            });
            if let CoreTransactionUpdate::StreamAppend { stream_id, .. } = update {
                owned_ops.push(OwnedCoreMetaBatchOp::Delete {
                    cf: CF_TRANSACTIONS,
                    table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
                    tuple_key: pending_transaction_stream_key(
                        stream_id,
                        &transaction.transaction_id,
                        ordinal,
                    ),
                    common: None,
                });
            }
        }
        for (ordinal, precondition) in preconditions.iter().enumerate() {
            let ordinal = u64::try_from(ordinal)
                .map_err(|_| anyhow!("CoreStore transaction precondition ordinal exceeds u64"))?;
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
                tuple_key: transaction_precondition_tuple_key(
                    &transaction.transaction_id,
                    ordinal,
                )?,
                payload: encode_transaction_precondition_row(
                    transaction,
                    ordinal,
                    precondition.visible_update_boundary,
                    &precondition.precondition,
                    precondition.revalidate_at_publication,
                )?,
                common: None,
            });
        }
        Ok(owned_ops)
    }

    pub(super) fn transaction_header_as_coremeta_op_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<OwnedCoreMetaBatchOp> {
        validate_transaction_root_scope(transaction)?;
        validate_logical_id(&transaction.transaction_id, "transaction id")?;
        let precondition_count = self
            .read_transaction_header_row_unlocked(&transaction.transaction_id)?
            .map(|header| header.precondition_count)
            .unwrap_or(0);
        let visible_update_count = u64::try_from(transaction.visible_updates.len())
            .map_err(|_| anyhow!("CoreStore transaction has too many staged updates"))?;
        Ok(OwnedCoreMetaBatchOp::Put {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
            tuple_key: transaction_header_tuple_key(&transaction.transaction_id)?,
            payload: encode_transaction_header_row(
                transaction,
                visible_update_count,
                precondition_count,
            )?,
            common: None,
        })
    }

    pub(super) async fn read_transaction_from_rows_unlocked(
        &self,
        transaction_id: &str,
    ) -> Result<Option<CoreTransaction>> {
        validate_logical_id(transaction_id, "transaction id")?;
        let Some(header) = self.read_transaction_header_row_unlocked(transaction_id)? else {
            return Ok(None);
        };
        let update_rows = self
            .read_transaction_update_rows_unlocked(&header.transaction, header.visible_update_count)
            .await?;
        let mut updates_by_ordinal = BTreeMap::new();
        for row in update_rows {
            if row.ordinal >= header.visible_update_count {
                continue;
            }
            if updates_by_ordinal.insert(row.ordinal, row).is_some() {
                bail!("CoreStore transaction has duplicate staged update row ordinal");
            }
        }

        let mut visible_updates = Vec::with_capacity(
            usize::try_from(header.visible_update_count)
                .map_err(|_| anyhow!("CoreStore transaction update count exceeds usize"))?,
        );
        for ordinal in 0..header.visible_update_count {
            let row = updates_by_ordinal
                .remove(&ordinal)
                .ok_or_else(|| anyhow!("CoreStore transaction staged update row is missing"))?;
            visible_updates.push(self.transaction_update_from_row(row).await?);
        }

        let mut transaction = header.transaction;
        transaction.visible_updates = visible_updates;
        validate_transaction_root_scope(&transaction)?;
        Ok(Some(transaction))
    }

    pub(super) fn read_transaction_header_row_unlocked(
        &self,
        transaction_id: &str,
    ) -> Result<Option<CoreTransactionHeaderRow>> {
        self.read_transaction_header_row_unlocked_from(&self.meta, transaction_id)
    }

    pub(super) fn read_transaction_header_row_unlocked_from<R: CoreMetaReader>(
        &self,
        reader: &R,
        transaction_id: &str,
    ) -> Result<Option<CoreTransactionHeaderRow>> {
        // Transaction headers coordinate staged state and are themselves an
        // input to publication visibility, so they must be read physically.
        let Some(payload) = reader.get(
            CF_TRANSACTIONS,
            TABLE_EXPLICIT_TRANSACTION_ROW,
            &transaction_header_tuple_key(transaction_id)?,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(decode_transaction_header_row(
            &payload,
            transaction_id,
        )?))
    }

    async fn read_transaction_update_rows_unlocked(
        &self,
        transaction: &CoreTransaction,
        visible_update_count: u64,
    ) -> Result<Vec<CoreTransactionStagedUpdateRow>> {
        let capacity = usize::try_from(visible_update_count)
            .map_err(|_| anyhow!("CoreStore transaction update count exceeds usize"))?;
        let mut rows = Vec::with_capacity(capacity);
        // Staged update rows are local transaction state, not published product
        // rows; commit must inspect them before a root can become visible.
        for ordinal in 0..visible_update_count {
            let payload = self
                .meta
                .get(
                    CF_TRANSACTIONS,
                    TABLE_EXPLICIT_TRANSACTION_ROW,
                    &transaction_update_tuple_key(&transaction.transaction_id, ordinal)?,
                )?
                .ok_or_else(|| anyhow!("CoreStore transaction staged update row is missing"))?;
            rows.push(decode_transaction_update_row(&payload, transaction)?);
        }
        Ok(rows)
    }

    pub(super) async fn read_transaction_preconditions_unlocked(
        &self,
        transaction_id: &str,
    ) -> Result<Vec<CoreTransactionPreconditionRow>> {
        let Some(header) = self.read_transaction_header_row_unlocked(transaction_id)? else {
            return Ok(Vec::new());
        };
        let capacity = usize::try_from(header.precondition_count)
            .map_err(|_| anyhow!("CoreStore transaction precondition count exceeds usize"))?;
        let mut rows = Vec::with_capacity(capacity);
        // Preconditions are local staging inputs that commit must revalidate
        // even though their transaction has not yet been published.
        for ordinal in 0..header.precondition_count {
            let payload = self
                .meta
                .get(
                    CF_TRANSACTIONS,
                    TABLE_EXPLICIT_TRANSACTION_ROW,
                    &transaction_precondition_tuple_key(transaction_id, ordinal)?,
                )?
                .ok_or_else(|| anyhow!("CoreStore transaction precondition row is missing"))?;
            rows.push(decode_transaction_precondition_row(
                &payload,
                &header.transaction,
            )?);
        }
        rows.sort_by_key(|row| row.ordinal);
        if rows
            .iter()
            .enumerate()
            .any(|(ordinal, row)| row.ordinal != ordinal as u64)
        {
            bail!("CoreStore transaction precondition ordinals are not contiguous");
        }
        if rows
            .iter()
            .any(|row| row.visible_update_boundary > header.visible_update_count)
        {
            bail!("CoreStore transaction precondition boundary exceeds staged update count");
        }
        if rows
            .windows(2)
            .any(|rows| rows[0].visible_update_boundary > rows[1].visible_update_boundary)
        {
            bail!("CoreStore transaction precondition boundaries are not monotonic");
        }
        Ok(rows)
    }

    async fn transaction_update_row_from_update(
        &self,
        transaction: &CoreTransaction,
        ordinal: u64,
        update: &CoreTransactionUpdate,
    ) -> Result<CoreTransactionUpdateRow> {
        Ok(match update {
            CoreTransactionUpdate::StreamAppend {
                partition_id,
                stream_id,
                record_kind,
                payload,
                idempotency_key_hash,
                visible_sequence,
                previous_event_hash,
                prepared_record_hash,
                created_at,
            } => CoreTransactionUpdateRow::StreamAppend {
                partition_id: partition_id.clone(),
                stream_id: stream_id.clone(),
                record_kind: record_kind.clone(),
                payload_ref: self
                    .transaction_payload_ref(transaction, ordinal, payload)
                    .await?,
                idempotency_key_hash: idempotency_key_hash.clone(),
                visible_sequence: *visible_sequence,
                previous_event_hash: previous_event_hash.clone(),
                prepared_record_hash: prepared_record_hash.clone(),
                created_at: created_at.clone(),
            },
            CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                previous_payload_hash,
                payload,
                payload_hash,
            } => {
                let actual_hash = core_meta_payload_digest(*table_id, payload);
                if &actual_hash != payload_hash {
                    bail!("CoreStore transaction staged CoreMeta payload hash mismatch");
                }
                CoreTransactionUpdateRow::CoreMetaPut {
                    cf: canonical_coremeta_cf_name(cf)?.to_string(),
                    table_id: *table_id,
                    tuple_key: tuple_key.clone(),
                    previous_payload_hash: previous_payload_hash.clone(),
                    payload_hash: payload_hash.clone(),
                    payload_ref: self
                        .transaction_payload_ref(transaction, ordinal, payload)
                        .await?,
                }
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                previous_payload_hash,
            } => CoreTransactionUpdateRow::CoreMetaDelete {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id: *table_id,
                tuple_key: tuple_key.clone(),
                previous_payload_hash: previous_payload_hash.clone(),
            },
        })
    }

    async fn transaction_payload_ref(
        &self,
        transaction: &CoreTransaction,
        ordinal: u64,
        payload: &[u8],
    ) -> Result<CoreTransactionPayloadRef> {
        let payload_sha256 = format!("sha256:{}", sha256_hex(payload));
        let payload_length = payload.len() as u64;
        if payload.len() <= CORE_TRANSACTION_STAGED_INLINE_PAYLOAD_BYTES {
            return Ok(CoreTransactionPayloadRef::Inline {
                payload: payload.to_vec(),
                payload_sha256,
                payload_length,
            });
        }

        let policy = self.pipeline_policy_for_storage_class(None)?;
        let logical_name = format!(
            "transaction-payload:{}:{}:{}",
            transaction.transaction_id,
            ordinal,
            payload_sha256.trim_start_matches("sha256:")
        );
        let write = self
            .write_logical_file_with_locator(WriteLogicalFileRequest {
                writer_family: WriterFamily::CoreControl.as_str().to_string(),
                generation: ordinal,
                logical_file_id: logical_name,
                source: payload.to_vec(),
                range_hints: Vec::new(),
                pipeline_policy: policy,
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: format!(
                    "transaction-payload:{}:{ordinal}",
                    transaction.transaction_id
                ),
                region_id: self.node_identity.region_id.clone(),
            })
            .await?;
        Ok(CoreTransactionPayloadRef::Locator {
            locator: core_meta_locator_from_manifest_locator(&write.locator)?,
            payload_sha256,
            payload_length,
        })
    }

    async fn transaction_update_from_row(
        &self,
        row: CoreTransactionStagedUpdateRow,
    ) -> Result<CoreTransactionUpdate> {
        Ok(match row.update {
            CoreTransactionUpdateRow::StreamAppend {
                partition_id,
                stream_id,
                record_kind,
                payload_ref,
                idempotency_key_hash,
                visible_sequence,
                previous_event_hash,
                prepared_record_hash,
                created_at,
            } => {
                let payload = self.read_transaction_payload_ref(payload_ref).await?;
                CoreTransactionUpdate::StreamAppend {
                    partition_id,
                    stream_id,
                    record_kind,
                    payload,
                    idempotency_key_hash,
                    visible_sequence,
                    previous_event_hash,
                    prepared_record_hash,
                    created_at,
                }
            }
            CoreTransactionUpdateRow::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                previous_payload_hash,
                payload_hash,
                payload_ref,
            } => {
                let payload = self.read_transaction_payload_ref(payload_ref).await?;
                validate_coremeta_operation_payload(&cf, table_id, &tuple_key, &payload)?;
                let actual_hash = core_meta_payload_digest(table_id, &payload);
                if actual_hash != payload_hash {
                    bail!("CoreStore transaction staged CoreMeta payload hash mismatch");
                }
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    previous_payload_hash,
                    payload,
                    payload_hash,
                }
            }
            CoreTransactionUpdateRow::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                previous_payload_hash,
            } => CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                previous_payload_hash,
            },
        })
    }

    async fn read_transaction_payload_ref(
        &self,
        payload_ref: CoreTransactionPayloadRef,
    ) -> Result<Vec<u8>> {
        match payload_ref {
            CoreTransactionPayloadRef::Inline {
                payload,
                payload_sha256,
                payload_length,
            } => {
                validate_payload_bytes(&payload, &payload_sha256, payload_length)?;
                Ok(payload)
            }
            CoreTransactionPayloadRef::Locator {
                locator,
                payload_sha256,
                payload_length,
            } => {
                let manifest_locator = core_meta_locator_to_manifest_locator(&locator)?;
                let manifest = self.read_logical_file_manifest(&manifest_locator).await?;
                if manifest.logical_size != payload_length {
                    bail!("CoreStore transaction staged payload locator length mismatch");
                }
                let payload = self
                    .read_logical_range(ReadLogicalRangeRequest {
                        ranges: vec![CoreByteRange {
                            start: 0,
                            end_exclusive: manifest.logical_size,
                        }],
                        manifest,
                        authz_scope: AuthzScopeRef {
                            anvil_storage_tenant_id: "system".to_string(),
                            authz_realm_id: "corestore".to_string(),
                        },
                        expected_boundary: None,
                        prefetch_policy: CorePrefetchPolicy::default(),
                        trace_context: CoreTraceContext::default(),
                    })
                    .await?;
                validate_payload_bytes(&payload, &payload_sha256, payload_length)?;
                Ok(payload)
            }
        }
    }
}

fn encode_transaction_header_row(
    transaction: &CoreTransaction,
    visible_update_count: u64,
    precondition_count: u64,
) -> Result<Vec<u8>> {
    validate_transaction_root_scope(transaction)?;
    validate_logical_id(&transaction.transaction_id, "transaction id")?;
    validate_transaction_writer_families(&transaction.writer_families)?;
    encode_message(CoreTransactionHeaderRowProto {
        common: Some(transaction_row_common(transaction)?),
        schema: CORE_TRANSACTION_HEADER_ROW_SCHEMA.to_string(),
        transaction_id: transaction.transaction_id.clone(),
        scope_partition: transaction.scope_partition.clone(),
        state: transaction_state_to_proto(transaction.state) as i32,
        preconditions_hash: transaction.preconditions_hash.clone(),
        operations_hash: transaction.operations_hash.clone(),
        writer_families: transaction.writer_families.clone(),
        visible_update_count,
        precondition_count,
        finalisation_error: transaction.finalisation_error.clone(),
        committed_at: transaction.committed_at.clone(),
        committed_by_principal: transaction.committed_by_principal.clone(),
        created_at_unix_nanos: transaction.created_at_unix_nanos,
        expires_at_unix_nanos: transaction.expires_at_unix_nanos,
        root_anchor_key: transaction.root_anchor_key.clone(),
        root_key_hash: transaction.root_key_hash.clone(),
        committed_root_generation: transaction.committed_root_generation,
        purpose: transaction.purpose.clone(),
        failure_evidence: transaction.failure_evidence.clone(),
        outcome: transaction.outcome.clone(),
    })
}

pub(super) fn borrow_owned_coremeta_batch_ops(
    ops: &[OwnedCoreMetaBatchOp],
) -> Vec<CoreMetaBatchOp<'_>> {
    ops.iter()
        .map(|op| match op {
            OwnedCoreMetaBatchOp::Put {
                cf,
                table_id,
                tuple_key,
                payload,
                common,
            } => CoreMetaBatchOp {
                cf,
                table_id: *table_id,
                tuple_key,
                common: common.clone(),
                kind: CoreMetaBatchOpKind::Put(payload),
            },
            OwnedCoreMetaBatchOp::Delete {
                cf,
                table_id,
                tuple_key,
                common,
            } => CoreMetaBatchOp {
                cf,
                table_id: *table_id,
                tuple_key,
                common: common.clone(),
                kind: CoreMetaBatchOpKind::Delete,
            },
        })
        .collect()
}

fn decode_transaction_header_row(
    bytes: &[u8],
    expected_transaction_id: &str,
) -> Result<CoreTransactionHeaderRow> {
    let proto = CoreTransactionHeaderRowProto::decode(bytes)?;
    ensure_message_round_trips(&proto, bytes, "CoreStore transaction header row")?;
    if proto.schema != CORE_TRANSACTION_HEADER_ROW_SCHEMA {
        bail!("CoreStore transaction header row has invalid schema");
    }
    if proto.transaction_id != expected_transaction_id {
        bail!("CoreStore transaction header row scope mismatch");
    }
    let common = proto
        .common
        .clone()
        .ok_or_else(|| anyhow!("CoreStore transaction header row missing CoreMeta common"))?;
    let transaction = CoreTransaction {
        schema: CORE_TRANSACTION_SCHEMA.to_string(),
        transaction_id: proto.transaction_id,
        scope_partition: proto.scope_partition,
        state: transaction_state_from_proto(proto.state)?,
        preconditions_hash: proto.preconditions_hash,
        operations_hash: proto.operations_hash,
        writer_families: proto.writer_families,
        visible_updates: Vec::new(),
        finalisation_error: proto.finalisation_error,
        committed_at: proto.committed_at,
        committed_by_principal: proto.committed_by_principal,
        created_at_unix_nanos: proto.created_at_unix_nanos,
        expires_at_unix_nanos: proto.expires_at_unix_nanos,
        root_anchor_key: proto.root_anchor_key,
        root_key_hash: proto.root_key_hash,
        committed_root_generation: proto.committed_root_generation,
        purpose: proto.purpose,
        failure_evidence: proto.failure_evidence,
        outcome: proto.outcome,
    };
    validate_transaction_root_scope(&transaction)?;
    validate_transaction_writer_families(&transaction.writer_families)?;
    validate_transaction_header_common(&transaction, &common)?;
    Ok(CoreTransactionHeaderRow {
        transaction,
        visible_update_count: proto.visible_update_count,
        precondition_count: proto.precondition_count,
    })
}

fn encode_transaction_update_row(
    transaction: &CoreTransaction,
    ordinal: u64,
    update: &CoreTransactionUpdateRow,
) -> Result<Vec<u8>> {
    validate_logical_id(&transaction.transaction_id, "transaction id")?;
    encode_message(CoreTransactionStagedUpdateRowProto {
        common: Some(transaction_row_common(transaction)?),
        schema: CORE_TRANSACTION_STAGED_UPDATE_ROW_SCHEMA.to_string(),
        transaction_id: transaction.transaction_id.clone(),
        ordinal,
        update: Some(transaction_update_to_proto(update)?),
    })
}

fn validate_transaction_header_common(
    transaction: &CoreTransaction,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    validate_transaction_row_common(transaction, common)
}

fn decode_transaction_update_row(
    bytes: &[u8],
    transaction: &CoreTransaction,
) -> Result<CoreTransactionStagedUpdateRow> {
    let proto = CoreTransactionStagedUpdateRowProto::decode(bytes)?;
    ensure_message_round_trips(&proto, bytes, "CoreStore transaction staged update row")?;
    if proto.schema != CORE_TRANSACTION_STAGED_UPDATE_ROW_SCHEMA {
        bail!("CoreStore transaction staged update row has invalid schema");
    }
    if proto.transaction_id != transaction.transaction_id {
        bail!("CoreStore transaction staged update row scope mismatch");
    }
    let common = proto.common.as_ref().ok_or_else(|| {
        anyhow!("CoreStore transaction staged update row missing CoreMeta common")
    })?;
    validate_transaction_row_common(transaction, common)?;
    Ok(CoreTransactionStagedUpdateRow {
        ordinal: proto.ordinal,
        update: transaction_update_from_proto(
            proto
                .update
                .ok_or_else(|| anyhow!("CoreStore transaction staged update row missing update"))?,
        )?,
    })
}

fn encode_transaction_precondition_row(
    transaction: &CoreTransaction,
    ordinal: u64,
    visible_update_boundary: u64,
    precondition: &CoreMutationPrecondition,
    revalidate_at_publication: bool,
) -> Result<Vec<u8>> {
    validate_logical_id(&transaction.transaction_id, "transaction id")?;
    encode_message(CoreTransactionPreconditionRowProto {
        common: Some(transaction_row_common(transaction)?),
        schema: CORE_TRANSACTION_PRECONDITION_ROW_SCHEMA.to_string(),
        transaction_id: transaction.transaction_id.clone(),
        ordinal,
        precondition: Some(mutation_precondition_to_proto(precondition)?),
        visible_update_boundary,
        revalidate_at_publication,
    })
}

fn decode_transaction_precondition_row(
    bytes: &[u8],
    transaction: &CoreTransaction,
) -> Result<CoreTransactionPreconditionRow> {
    let proto = CoreTransactionPreconditionRowProto::decode(bytes)?;
    ensure_message_round_trips(&proto, bytes, "CoreStore transaction precondition row")?;
    if proto.schema != CORE_TRANSACTION_PRECONDITION_ROW_SCHEMA
        || proto.transaction_id != transaction.transaction_id
    {
        bail!("CoreStore transaction precondition row scope mismatch");
    }
    let common = proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore transaction precondition row missing CoreMeta common"))?;
    validate_transaction_row_common(transaction, common)?;
    Ok(CoreTransactionPreconditionRow {
        ordinal: proto.ordinal,
        visible_update_boundary: proto.visible_update_boundary,
        precondition: mutation_precondition_from_proto(proto.precondition.ok_or_else(|| {
            anyhow!("CoreStore transaction precondition row is missing precondition")
        })?)?,
        revalidate_at_publication: proto.revalidate_at_publication,
    })
}

fn transaction_update_to_proto(
    update: &CoreTransactionUpdateRow,
) -> Result<CoreTransactionUpdateRowProto> {
    let kind = match update {
        CoreTransactionUpdateRow::StreamAppend {
            partition_id,
            stream_id,
            record_kind,
            payload_ref,
            idempotency_key_hash,
            visible_sequence,
            previous_event_hash,
            prepared_record_hash,
            created_at,
        } => {
            transaction_update_row_proto::Kind::StreamAppend(CoreTransactionStreamAppendRowProto {
                stream_id: stream_id.clone(),
                visible_sequence: *visible_sequence,
                prepared_record_hash: prepared_record_hash.clone(),
                partition_id: partition_id.clone(),
                record_kind: record_kind.clone(),
                payload_ref: Some(payload_ref_to_proto(payload_ref)?),
                idempotency_key_hash: idempotency_key_hash.clone(),
                previous_event_hash: previous_event_hash.clone(),
                created_at: created_at.clone(),
            })
        }
        CoreTransactionUpdateRow::CoreMetaPut {
            cf,
            table_id,
            tuple_key,
            previous_payload_hash,
            payload_hash,
            payload_ref,
        } => transaction_update_row_proto::Kind::CoreMetaPut(CoreTransactionCoreMetaPutRowProto {
            cf: canonical_coremeta_cf_name(cf)?.to_string(),
            table_id: u32::from(*table_id),
            tuple_key: tuple_key.clone(),
            previous_payload_hash: previous_payload_hash.clone(),
            payload_hash: payload_hash.clone(),
            payload_ref: Some(payload_ref_to_proto(payload_ref)?),
        }),
        CoreTransactionUpdateRow::CoreMetaDelete {
            cf,
            table_id,
            tuple_key,
            previous_payload_hash,
        } => transaction_update_row_proto::Kind::CoreMetaDelete(
            CoreTransactionCoreMetaDeleteRowProto {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id: u32::from(*table_id),
                tuple_key: tuple_key.clone(),
                previous_payload_hash: previous_payload_hash.clone(),
            },
        ),
    };
    Ok(CoreTransactionUpdateRowProto { kind: Some(kind) })
}

fn transaction_update_from_proto(
    proto: CoreTransactionUpdateRowProto,
) -> Result<CoreTransactionUpdateRow> {
    Ok(
        match proto
            .kind
            .ok_or_else(|| anyhow!("CoreStore transaction staged update is missing kind"))?
        {
            transaction_update_row_proto::Kind::StreamAppend(value) => {
                CoreTransactionUpdateRow::StreamAppend {
                    partition_id: value.partition_id,
                    stream_id: value.stream_id,
                    record_kind: value.record_kind,
                    payload_ref: payload_ref_from_proto(value.payload_ref.ok_or_else(|| {
                        anyhow!("CoreStore transaction staged stream append is missing payload ref")
                    })?)?,
                    idempotency_key_hash: value.idempotency_key_hash,
                    visible_sequence: value.visible_sequence,
                    previous_event_hash: value.previous_event_hash,
                    prepared_record_hash: value.prepared_record_hash,
                    created_at: value.created_at,
                }
            }
            transaction_update_row_proto::Kind::CoreMetaPut(value) => {
                CoreTransactionUpdateRow::CoreMetaPut {
                    cf: canonical_coremeta_cf_name(&value.cf)?.to_string(),
                    table_id: u16::try_from(value.table_id)
                        .map_err(|_| anyhow!("CoreMeta put update table id exceeds u16"))?,
                    tuple_key: value.tuple_key,
                    previous_payload_hash: value.previous_payload_hash,
                    payload_hash: value.payload_hash,
                    payload_ref: payload_ref_from_proto(value.payload_ref.ok_or_else(|| {
                        anyhow!("CoreStore transaction staged CoreMeta put is missing payload ref")
                    })?)?,
                }
            }
            transaction_update_row_proto::Kind::CoreMetaDelete(value) => {
                CoreTransactionUpdateRow::CoreMetaDelete {
                    cf: canonical_coremeta_cf_name(&value.cf)?.to_string(),
                    table_id: u16::try_from(value.table_id)
                        .map_err(|_| anyhow!("CoreMeta delete update table id exceeds u16"))?,
                    tuple_key: value.tuple_key,
                    previous_payload_hash: value.previous_payload_hash,
                }
            }
        },
    )
}

fn payload_ref_to_proto(
    payload_ref: &CoreTransactionPayloadRef,
) -> Result<CoreTransactionPayloadRefProto> {
    match payload_ref {
        CoreTransactionPayloadRef::Inline {
            payload,
            payload_sha256,
            payload_length,
        } => {
            validate_payload_bytes(payload, payload_sha256, *payload_length)?;
            if payload.len() > CORE_TRANSACTION_STAGED_INLINE_PAYLOAD_BYTES {
                bail!("CoreStore transaction inline staged payload exceeds row inline limit");
            }
            Ok(CoreTransactionPayloadRefProto {
                inline_payload: Some(payload.clone()),
                locator: None,
                payload_sha256: payload_sha256.clone(),
                payload_length: *payload_length,
            })
        }
        CoreTransactionPayloadRef::Locator {
            locator,
            payload_sha256,
            payload_length,
        } => {
            validate_hash(payload_sha256, "transaction staged payload sha256")?;
            Ok(CoreTransactionPayloadRefProto {
                inline_payload: None,
                locator: Some(locator.clone()),
                payload_sha256: payload_sha256.clone(),
                payload_length: *payload_length,
            })
        }
    }
}

fn payload_ref_from_proto(
    proto: CoreTransactionPayloadRefProto,
) -> Result<CoreTransactionPayloadRef> {
    validate_hash(&proto.payload_sha256, "transaction staged payload sha256")?;
    if let Some(payload) = proto.inline_payload {
        if proto.locator.is_some() {
            bail!("CoreStore transaction payload ref cannot be both inline and locator backed");
        }
        validate_payload_bytes(&payload, &proto.payload_sha256, proto.payload_length)?;
        if payload.len() > CORE_TRANSACTION_STAGED_INLINE_PAYLOAD_BYTES {
            bail!("CoreStore transaction inline staged payload exceeds row inline limit");
        }
        return Ok(CoreTransactionPayloadRef::Inline {
            payload,
            payload_sha256: proto.payload_sha256,
            payload_length: proto.payload_length,
        });
    }
    let locator = proto
        .locator
        .ok_or_else(|| anyhow!("CoreStore transaction payload ref is missing storage"))?;
    Ok(CoreTransactionPayloadRef::Locator {
        locator,
        payload_sha256: proto.payload_sha256,
        payload_length: proto.payload_length,
    })
}

fn mutation_precondition_to_proto(
    precondition: &CoreMutationPrecondition,
) -> Result<CoreMutationPreconditionRowProto> {
    let kind = match precondition {
        CoreMutationPrecondition::Fence {
            fence_name,
            fence_token,
        } => mutation_precondition_row_proto::Kind::Fence(CoreMutationFencePreconditionRowProto {
            fence_name: fence_name.clone(),
            fence_token: *fence_token,
        }),
        CoreMutationPrecondition::CoreMetaRow {
            cf,
            table_id,
            tuple_key,
            expected_payload_hash,
            require_absent,
            require_present,
        } => mutation_precondition_row_proto::Kind::CoreMetaRow(
            CoreMutationCoreMetaRowPreconditionRowProto {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id: u32::from(*table_id),
                tuple_key: tuple_key.clone(),
                expected_payload_hash: expected_payload_hash.clone(),
                require_absent: *require_absent,
                require_present: *require_present,
            },
        ),
        CoreMutationPrecondition::CoreMetaLease {
            cf,
            table_id,
            tuple_key,
            expected_payload_hash,
            expires_at_unix_nanos,
        } => mutation_precondition_row_proto::Kind::CoreMetaLease(
            CoreMutationCoreMetaLeasePreconditionRowProto {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id: u32::from(*table_id),
                tuple_key: tuple_key.clone(),
                expected_payload_hash: expected_payload_hash.clone(),
                expires_at_unix_nanos: *expires_at_unix_nanos,
            },
        ),
        CoreMutationPrecondition::StreamHead {
            stream_id,
            expected_last_sequence,
            expected_last_event_hash,
        } => mutation_precondition_row_proto::Kind::StreamHead(
            CoreMutationStreamHeadPreconditionRowProto {
                stream_id: stream_id.clone(),
                expected_last_sequence: *expected_last_sequence,
                expected_last_event_hash: expected_last_event_hash.clone(),
            },
        ),
    };
    Ok(CoreMutationPreconditionRowProto { kind: Some(kind) })
}

fn mutation_precondition_from_proto(
    precondition: CoreMutationPreconditionRowProto,
) -> Result<CoreMutationPrecondition> {
    Ok(
        match precondition
            .kind
            .ok_or_else(|| anyhow!("CoreStore transaction precondition kind is missing"))?
        {
            mutation_precondition_row_proto::Kind::Fence(value) => {
                CoreMutationPrecondition::Fence {
                    fence_name: value.fence_name,
                    fence_token: value.fence_token,
                }
            }
            mutation_precondition_row_proto::Kind::StreamHead(value) => {
                CoreMutationPrecondition::StreamHead {
                    stream_id: value.stream_id,
                    expected_last_sequence: value.expected_last_sequence,
                    expected_last_event_hash: value.expected_last_event_hash,
                }
            }
            mutation_precondition_row_proto::Kind::CoreMetaRow(value) => {
                CoreMutationPrecondition::CoreMetaRow {
                    cf: canonical_coremeta_cf_name(&value.cf)?.to_string(),
                    table_id: u16::try_from(value.table_id).map_err(|_| {
                        anyhow!("CoreStore transaction precondition table exceeds u16")
                    })?,
                    tuple_key: value.tuple_key,
                    expected_payload_hash: value.expected_payload_hash,
                    require_absent: value.require_absent,
                    require_present: value.require_present,
                }
            }
            mutation_precondition_row_proto::Kind::CoreMetaLease(value) => {
                CoreMutationPrecondition::CoreMetaLease {
                    cf: canonical_coremeta_cf_name(&value.cf)?.to_string(),
                    table_id: u16::try_from(value.table_id).map_err(|_| {
                        anyhow!("CoreStore transaction lease precondition table exceeds u16")
                    })?,
                    tuple_key: value.tuple_key,
                    expected_payload_hash: value.expected_payload_hash,
                    expires_at_unix_nanos: value.expires_at_unix_nanos,
                }
            }
        },
    )
}

fn fence_precondition_to_proto(value: &CoreFencePrecondition) -> CoreFencePreconditionRowProto {
    CoreFencePreconditionRowProto {
        fence_name: value.fence_name.clone(),
        fence_token: value.fence_token,
        authenticated_principal: value.authenticated_principal.clone(),
    }
}

fn transaction_header_tuple_key(transaction_id: &str) -> Result<Vec<u8>> {
    validate_logical_id(transaction_id, "transaction id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("transaction"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8("header"),
    ])
}

fn transaction_update_tuple_key(transaction_id: &str, ordinal: u64) -> Result<Vec<u8>> {
    validate_logical_id(transaction_id, "transaction id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("transaction"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8("update"),
        CoreMetaTuplePart::U64(ordinal),
    ])
}

fn transaction_precondition_tuple_key(transaction_id: &str, ordinal: u64) -> Result<Vec<u8>> {
    validate_logical_id(transaction_id, "transaction id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("transaction"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8("precondition"),
        CoreMetaTuplePart::U64(ordinal),
    ])
}

fn transaction_state_to_proto(state: CoreTransactionState) -> CoreTransactionHeaderStateProto {
    match state {
        CoreTransactionState::Open => CoreTransactionHeaderStateProto::Open,
        CoreTransactionState::Prepared => CoreTransactionHeaderStateProto::Prepared,
        CoreTransactionState::Committed => CoreTransactionHeaderStateProto::Committed,
        CoreTransactionState::FinalisationFailed => {
            CoreTransactionHeaderStateProto::FinalisationFailed
        }
        CoreTransactionState::Aborted => CoreTransactionHeaderStateProto::Aborted,
        CoreTransactionState::RolledBack => CoreTransactionHeaderStateProto::RolledBack,
        CoreTransactionState::Expired => CoreTransactionHeaderStateProto::Expired,
        CoreTransactionState::Failed => CoreTransactionHeaderStateProto::Failed,
    }
}

fn transaction_state_from_proto(state: i32) -> Result<CoreTransactionState> {
    Ok(match CoreTransactionHeaderStateProto::try_from(state)? {
        CoreTransactionHeaderStateProto::Open => CoreTransactionState::Open,
        CoreTransactionHeaderStateProto::Prepared => CoreTransactionState::Prepared,
        CoreTransactionHeaderStateProto::Committed => CoreTransactionState::Committed,
        CoreTransactionHeaderStateProto::FinalisationFailed => {
            CoreTransactionState::FinalisationFailed
        }
        CoreTransactionHeaderStateProto::Aborted => CoreTransactionState::Aborted,
        CoreTransactionHeaderStateProto::RolledBack => CoreTransactionState::RolledBack,
        CoreTransactionHeaderStateProto::Expired => CoreTransactionState::Expired,
        CoreTransactionHeaderStateProto::Failed => CoreTransactionState::Failed,
        CoreTransactionHeaderStateProto::Unspecified => {
            bail!("CoreStore transaction state is unset")
        }
    })
}

fn visibility_for_transaction_state(state: CoreTransactionState) -> CoreMetaVisibilityState {
    match state {
        CoreTransactionState::Open | CoreTransactionState::Prepared => {
            CoreMetaVisibilityState::Pending
        }
        CoreTransactionState::Committed => CoreMetaVisibilityState::Committed,
        CoreTransactionState::RolledBack => CoreMetaVisibilityState::RolledBack,
        CoreTransactionState::FinalisationFailed
        | CoreTransactionState::Aborted
        | CoreTransactionState::Expired
        | CoreTransactionState::Failed => CoreMetaVisibilityState::Aborted,
    }
}

fn transaction_row_common(transaction: &CoreTransaction) -> Result<CoreMetaRowCommonProto> {
    let (root_key_hash, root_generation) = if transaction.state == CoreTransactionState::Committed {
        (
            transaction.root_key_hash.clone(),
            transaction.committed_root_generation.ok_or_else(|| {
                anyhow!("CoreStore committed transaction is missing its root generation")
            })?,
        )
    } else {
        if transaction.committed_root_generation.is_some() {
            bail!("CoreStore unpublished transaction must not name a root generation");
        }
        (String::new(), 0)
    };
    Ok(CoreMetaRowCommonProto {
        realm_id: transaction.committed_by_principal.clone(),
        root_key_hash,
        root_generation,
        transaction_id: transaction.transaction_id.clone(),
        visibility_state: visibility_for_transaction_state(transaction.state) as i32,
        created_at_unix_nanos: transaction.created_at_unix_nanos,
        payload_schema_version: 1,
    })
}

fn validate_transaction_row_common(
    transaction: &CoreTransaction,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    if common.realm_id != transaction.committed_by_principal
        || common.transaction_id != transaction.transaction_id
        || common.visibility_state_enum() != visibility_for_transaction_state(transaction.state)
    {
        bail!("CoreStore transaction row CoreMeta common metadata mismatch");
    }
    if transaction.state == CoreTransactionState::Committed {
        let committed_root_generation = transaction.committed_root_generation.ok_or_else(|| {
            anyhow!("CoreStore committed transaction is missing its root generation")
        })?;
        if common.root_key_hash != transaction.root_key_hash
            || common.root_generation != committed_root_generation
            || committed_root_generation == 0
        {
            bail!("CoreStore committed transaction row root scope mismatch");
        }
    } else if !common.root_key_hash.is_empty()
        || common.root_generation != 0
        || transaction.committed_root_generation.is_some()
    {
        bail!("CoreStore unpublished transaction row must not claim a visible root generation");
    }
    Ok(())
}

fn validate_transaction_writer_families(writer_families: &[String]) -> Result<()> {
    if writer_families.is_empty() {
        bail!("CoreStore transaction must declare at least one writer family");
    }
    let mut canonical = writer_families.to_vec();
    canonical.sort();
    canonical.dedup();
    if canonical != writer_families {
        bail!("CoreStore transaction writer families must be sorted and unique");
    }
    if writer_families
        .iter()
        .any(|family| WriterFamily::from_name(family).is_none())
    {
        bail!("CoreStore transaction names an unknown writer family");
    }
    if !writer_families
        .iter()
        .any(|family| family == WriterFamily::CoreControl.as_str())
    {
        bail!("CoreStore transaction writer families must include core_control");
    }
    Ok(())
}

fn transaction_updates_have_prefix(
    value: &[CoreTransactionUpdate],
    prefix: &[CoreTransactionUpdate],
) -> bool {
    value.len() >= prefix.len() && value.iter().zip(prefix.iter()).all(|(a, b)| a == b)
}

fn validate_payload_bytes(payload: &[u8], expected_sha256: &str, expected_len: u64) -> Result<()> {
    validate_hash(expected_sha256, "transaction staged payload sha256")?;
    if payload.len() as u64 != expected_len {
        bail!("CoreStore transaction staged payload length mismatch");
    }
    let actual = format!("sha256:{}", sha256_hex(payload));
    if actual != expected_sha256 {
        bail!("CoreStore transaction staged payload sha256 mismatch");
    }
    Ok(())
}

fn encode_message(message: impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_message_round_trips(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    let mut canonical = Vec::new();
    message.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("{label} is not deterministically encoded");
    }
    Ok(())
}
