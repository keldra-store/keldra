use super::*;

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
