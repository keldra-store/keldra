use super::{
    WriterSegmentCatalogRecord, validate_record, writer_realm, writer_root_key_hash,
    writer_scope_hash,
};
use crate::core_store::{
    CF_MATERIALISATION, CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMetaVisibilityState,
    CoreMutationPrecondition, CoreStore, TABLE_WRITER_HEAD_ROW, core_meta_payload_digest,
    core_meta_tuple_key, decode_deterministic_proto, encode_deterministic_proto,
};
use anyhow::{Result, anyhow, bail};
use prost::Message;

const WRITER_HEAD_SCHEMA: &str = "anvil.coremeta.writer_head.v1";

pub(super) struct WriterHead {
    pub(super) record: WriterSegmentCatalogRecord,
    pub(super) publication_generation: u64,
    expected_payload_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct WriterHeadProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    family: String,
    #[prost(string, tag = "4")]
    scope: String,
    #[prost(string, tag = "5")]
    scope_hash: String,
    #[prost(uint64, tag = "6")]
    current_generation: u64,
    #[prost(uint64, tag = "7")]
    source_cursor: u64,
    #[prost(string, tag = "8")]
    segment_ref: String,
    #[prost(string, tag = "9")]
    core_object_ref_target: String,
    #[prost(string, tag = "10")]
    segment_hash: String,
    #[prost(uint64, tag = "11")]
    segment_length: u64,
    #[prost(uint64, tag = "12")]
    compacted_through_cursor: u64,
    #[prost(uint64, tag = "13")]
    segment_created_at_unix_nanos: u64,
    #[prost(uint64, tag = "14")]
    publication_generation: u64,
    #[prost(uint64, tag = "15")]
    published_at_unix_nanos: u64,
    #[prost(string, tag = "16")]
    publication_transaction_id: String,
}

pub(super) fn read(store: &CoreStore, family: &str, scope: &str) -> Result<Option<WriterHead>> {
    let Some(payload) = store.read_coremeta_row(
        CF_MATERIALISATION,
        TABLE_WRITER_HEAD_ROW,
        &tuple_key(family, scope)?,
    )?
    else {
        return Ok(None);
    };
    decode(&payload, family, scope).map(Some)
}

pub(super) fn precondition(
    family: &str,
    scope: &str,
    current: Option<&WriterHead>,
) -> Result<CoreMutationPrecondition> {
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_MATERIALISATION.to_string(),
        table_id: TABLE_WRITER_HEAD_ROW,
        tuple_key: tuple_key(family, scope)?,
        expected_payload_hash: current.map(|head| head.expected_payload_hash.clone()),
        require_absent: current.is_none(),
        require_present: current.is_some(),
    })
}

pub(super) fn encode(
    record: &WriterSegmentCatalogRecord,
    publication_generation: u64,
    publication_transaction_id: &str,
    published_at_unix_nanos: u64,
) -> Result<Vec<u8>> {
    validate_record(record)?;
    if publication_generation == 0
        || publication_transaction_id.is_empty()
        || published_at_unix_nanos == 0
    {
        bail!("writer publication identity, generation, and timestamp must be present");
    }
    Ok(encode_deterministic_proto(&WriterHeadProto {
        common: Some(common(
            record,
            publication_generation,
            publication_transaction_id,
            published_at_unix_nanos,
        )),
        schema: WRITER_HEAD_SCHEMA.to_string(),
        family: record.family.clone(),
        scope: record.scope.clone(),
        scope_hash: writer_scope_hash(&record.family, &record.scope),
        current_generation: record.generation,
        source_cursor: record.source_cursor,
        segment_ref: record.segment_ref.clone(),
        core_object_ref_target: record.core_object_ref_target.clone(),
        segment_hash: record.segment_hash.clone(),
        segment_length: record.segment_length,
        compacted_through_cursor: record.source_cursor,
        segment_created_at_unix_nanos: record.created_at_unix_nanos,
        publication_generation,
        published_at_unix_nanos,
        publication_transaction_id: publication_transaction_id.to_string(),
    }))
}

pub(super) fn tuple_key(family: &str, scope: &str) -> Result<Vec<u8>> {
    if family.is_empty() || scope.is_empty() {
        bail!("writer head family and scope must not be empty");
    }
    let scope_hash = writer_scope_hash(family, scope);
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(family),
        CoreMetaTuplePart::Hash(&scope_hash),
    ])
}

fn decode(bytes: &[u8], family: &str, scope: &str) -> Result<WriterHead> {
    let proto = decode_deterministic_proto::<WriterHeadProto>(bytes, "writer head")?;
    if proto.schema != WRITER_HEAD_SCHEMA {
        bail!("writer head schema mismatch");
    }
    if proto.family != family
        || proto.scope != scope
        || proto.scope_hash != writer_scope_hash(family, scope)
        || proto.compacted_through_cursor > proto.source_cursor
        || proto.publication_generation == 0
        || proto.published_at_unix_nanos == 0
        || proto.publication_transaction_id.is_empty()
    {
        bail!("writer head scope or cursor mismatch");
    }
    let common = proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("writer head is missing CoreMeta common"))?;
    let record = WriterSegmentCatalogRecord {
        family: proto.family,
        scope: proto.scope,
        segment_ref: proto.segment_ref,
        core_object_ref_target: proto.core_object_ref_target,
        segment_hash: proto.segment_hash,
        segment_length: proto.segment_length,
        generation: proto.current_generation,
        source_cursor: proto.source_cursor,
        created_at_unix_nanos: proto.segment_created_at_unix_nanos,
    };
    validate_record(&record)?;
    if super::validate_writer_common(
        &record.family,
        &record.scope,
        &proto.publication_transaction_id,
        proto.published_at_unix_nanos,
        common,
    )
    .is_err()
    {
        bail!("writer head CoreMeta common mismatch");
    }
    Ok(WriterHead {
        record,
        publication_generation: proto.publication_generation,
        expected_payload_hash: core_meta_payload_digest(TABLE_WRITER_HEAD_ROW, bytes),
    })
}

fn common(
    record: &WriterSegmentCatalogRecord,
    publication_generation: u64,
    publication_transaction_id: &str,
    published_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    CoreMetaRowCommonProto {
        realm_id: writer_realm(&record.family, &record.scope),
        root_key_hash: writer_root_key_hash(&record.family, &record.scope),
        root_generation: publication_generation,
        transaction_id: publication_transaction_id.to_string(),
        visibility_state: CoreMetaVisibilityState::Committed as i32,
        created_at_unix_nanos: published_at_unix_nanos,
        payload_schema_version: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> WriterSegmentCatalogRecord {
        WriterSegmentCatalogRecord {
            family: "test-writer".into(),
            scope: "tenant/42/index/main".into(),
            segment_ref: "segment:7".into(),
            core_object_ref_target: "core-object-ref:test-7".into(),
            segment_hash: format!("{:064x}", 7),
            segment_length: 7,
            generation: 7,
            source_cursor: 70,
            created_at_unix_nanos: 700,
        }
    }

    #[test]
    fn writer_head_accepts_independent_physical_root_generation() {
        let record = record();
        let payload = encode(&record, 3, "tx-writer", 701).unwrap();
        let mut common = crate::core_store::core_meta_row_common_from_payload(&payload).unwrap();
        common.root_generation = 91;
        let rebound = crate::core_store::replace_core_meta_row_common(&payload, &common).unwrap();

        let decoded = decode(&rebound, &record.family, &record.scope).unwrap();
        assert_eq!(decoded.record, record);
        assert_eq!(decoded.publication_generation, 3);
        assert_ne!(common.root_generation, decoded.publication_generation);
    }
}
