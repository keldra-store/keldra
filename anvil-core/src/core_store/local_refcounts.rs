use super::local_tx_rows::OwnedCoreMetaBatchOp;
use super::*;
use crate::object_links;
use crate::persistence::{Bucket, Object};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use prost::Message;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

const CORE_PAYLOAD_REFERENCE_SCHEMA: &str = "anvil.core.payload_reference.v1";
#[cfg(test)]
const TEST_PAYLOAD_REFERENCE_PAGE_ROWS: usize = 128;

#[derive(Clone, PartialEq, Message)]
struct PayloadReferenceRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    payload_identity: String,
    #[prost(string, tag = "4")]
    storage_kind: String,
    #[prost(string, tag = "5")]
    reference_kind: String,
    #[prost(int64, tag = "6")]
    tenant_id: i64,
    #[prost(int64, tag = "7")]
    bucket_id: i64,
    #[prost(string, tag = "8")]
    bucket_name: String,
    #[prost(string, tag = "9")]
    object_key: String,
    #[prost(string, tag = "10")]
    version_id: String,
    #[prost(string, tag = "11")]
    target_ref: String,
    #[prost(uint64, tag = "12")]
    logical_size: u64,
    #[prost(string, tag = "13")]
    content_hash: String,
    #[prost(string, tag = "14")]
    created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PayloadReferenceDescriptor {
    payload_identity: String,
    storage_kind: String,
    target_ref: String,
    logical_size: u64,
    content_hash: String,
}

impl CoreStore {
    #[cfg(test)]
    pub(crate) async fn payload_reference_summaries_for_object(
        &self,
        object: &Object,
    ) -> Result<Vec<CorePayloadReferenceSummary>> {
        let Some(shard_map) = object.shard_map.as_ref() else {
            return Ok(Vec::new());
        };
        let descriptors = self
            .payload_reference_descriptors_from_shard_map(shard_map)
            .await?;
        let mut summaries = Vec::with_capacity(descriptors.len());
        for descriptor in descriptors {
            summaries.push(CorePayloadReferenceSummary {
                reference_count: self.payload_reference_count(&descriptor.payload_identity)?,
                payload_identity: descriptor.payload_identity,
                storage_kind: descriptor.storage_kind,
            });
        }
        Ok(summaries)
    }

    pub(super) async fn payload_reference_put_ops_for_object(
        &self,
        bucket: &Bucket,
        object: &Object,
        transaction_id: &str,
    ) -> Result<(Vec<OwnedCoreMetaBatchOp>, Vec<CoreMetaRootPublication>)> {
        if !object_has_live_payload_reference_edges(object) {
            return Ok((Vec::new(), Vec::new()));
        }
        validate_payload_reference_scope(bucket, object)?;
        let descriptors = self
            .payload_reference_descriptors_for_object(object)
            .await?;
        self.payload_reference_ops(bucket, object, descriptors, transaction_id, true)
            .await
    }

    pub(super) async fn payload_reference_delete_ops_for_object(
        &self,
        bucket: &Bucket,
        object: &Object,
        transaction_id: &str,
    ) -> Result<(Vec<OwnedCoreMetaBatchOp>, Vec<CoreMetaRootPublication>)> {
        if !object_has_payload_reference_target(object) {
            return Ok((Vec::new(), Vec::new()));
        }
        validate_payload_reference_scope(bucket, object)?;
        let descriptors = self
            .payload_reference_descriptors_for_object(object)
            .await?;
        self.payload_reference_ops(bucket, object, descriptors, transaction_id, false)
            .await
    }

    async fn payload_reference_descriptors_for_object(
        &self,
        object: &Object,
    ) -> Result<Vec<PayloadReferenceDescriptor>> {
        let Some(shard_map) = object.shard_map.as_ref() else {
            return Ok(Vec::new());
        };
        self.payload_reference_descriptors_from_shard_map(shard_map)
            .await
    }

    async fn payload_reference_descriptors_from_shard_map(
        &self,
        shard_map: &JsonValue,
    ) -> Result<Vec<PayloadReferenceDescriptor>> {
        let target = object_data_target_from_json(shard_map)?;
        let mut descriptors = BTreeMap::<String, PayloadReferenceDescriptor>::new();
        match target {
            PayloadDataTarget::ObjectRef { object_ref, target } => {
                let storage_kind = if is_inline_object_ref(&object_ref) {
                    "inline_payload"
                } else {
                    "erasure_block"
                };
                let payload_identity = object_ref_payload_identity(storage_kind, &object_ref);
                descriptors.insert(
                    payload_identity.clone(),
                    PayloadReferenceDescriptor {
                        payload_identity,
                        storage_kind: storage_kind.to_string(),
                        target_ref: target,
                        logical_size: object_ref.logical_size,
                        content_hash: object_ref.hash,
                    },
                );
            }
            PayloadDataTarget::LogicalFile { locator } => {
                let manifest = self.read_logical_file_manifest(&locator).await?;
                for block in &manifest.blocks {
                    let payload_identity = logical_block_payload_identity(block, &manifest);
                    descriptors.insert(
                        payload_identity.clone(),
                        PayloadReferenceDescriptor {
                            payload_identity,
                            storage_kind: "erasure_block".to_string(),
                            target_ref: format!(
                                "{}:{}",
                                manifest.erasure_profile_id, block.block_id
                            ),
                            logical_size: block.logical_length,
                            content_hash: block.content_hash.clone(),
                        },
                    );
                }
            }
        }
        Ok(descriptors.into_values().collect())
    }

    async fn payload_reference_ops(
        &self,
        bucket: &Bucket,
        object: &Object,
        descriptors: Vec<PayloadReferenceDescriptor>,
        transaction_id: &str,
        put: bool,
    ) -> Result<(Vec<OwnedCoreMetaBatchOp>, Vec<CoreMetaRootPublication>)> {
        let mut ops = Vec::with_capacity(descriptors.len());
        let mut publications = Vec::with_capacity(descriptors.len());
        let created_at = now_rfc3339();
        let created_at_nanos =
            u64::try_from(Utc::now().timestamp_nanos_opt().unwrap_or_default()).unwrap_or_default();
        for descriptor in descriptors {
            let root_anchor_key = payload_reference_root_anchor_key(&descriptor.payload_identity);
            let root_generation = self
                .next_payload_reference_root_generation(&descriptor.payload_identity)
                .await?;
            let common = payload_reference_common(
                &descriptor.payload_identity,
                root_generation,
                transaction_id,
                created_at_nanos,
            );
            let key = payload_reference_key(&descriptor.payload_identity, object)?;
            if put {
                let row = PayloadReferenceRowProto {
                    common: Some(common),
                    schema: CORE_PAYLOAD_REFERENCE_SCHEMA.to_string(),
                    payload_identity: descriptor.payload_identity,
                    storage_kind: descriptor.storage_kind,
                    reference_kind: "object_version".to_string(),
                    tenant_id: object.tenant_id,
                    bucket_id: object.bucket_id,
                    bucket_name: bucket.name.clone(),
                    object_key: object.key.clone(),
                    version_id: object.version_id.to_string(),
                    target_ref: descriptor.target_ref,
                    logical_size: descriptor.logical_size,
                    content_hash: descriptor.content_hash,
                    created_at: created_at.clone(),
                };
                let payload = encode_deterministic_proto(&row);
                validate_coremeta_operation_payload(
                    CF_REFCOUNTS,
                    TABLE_REFCOUNT_ROW,
                    &key,
                    &payload,
                )?;
                ops.push(OwnedCoreMetaBatchOp::Put {
                    cf: CF_REFCOUNTS,
                    table_id: TABLE_REFCOUNT_ROW,
                    tuple_key: key,
                    payload,
                    common: None,
                });
            } else {
                ops.push(OwnedCoreMetaBatchOp::Delete {
                    cf: CF_REFCOUNTS,
                    table_id: TABLE_REFCOUNT_ROW,
                    tuple_key: key,
                    common: Some(common),
                });
            }
            publications.push(CoreMetaRootPublication::new(
                root_anchor_key,
                WriterFamily::ObjectBlob,
            ));
        }
        Ok((ops, publications))
    }

    async fn next_payload_reference_root_generation(&self, payload_identity: &str) -> Result<u64> {
        let root_anchor_key = payload_reference_root_anchor_key(payload_identity);
        Ok(self
            .read_latest_root_anchor(&root_anchor_key)
            .await?
            .map(|anchor| anchor.root_generation.saturating_add(1))
            .unwrap_or(1))
    }

    #[cfg(test)]
    fn payload_reference_count(&self, payload_identity: &str) -> Result<usize> {
        let mut count = 0_usize;
        let prefix = payload_reference_prefix(payload_identity)?;
        let mut after = None;
        loop {
            let rows = self.scan_coremeta_prefix_page(
                CF_REFCOUNTS,
                TABLE_REFCOUNT_ROW,
                &prefix,
                after.as_deref(),
                TEST_PAYLOAD_REFERENCE_PAGE_ROWS,
            )?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let decoded = decode_payload_reference_row(&row.payload)?;
                if decoded.payload_identity == payload_identity {
                    count = count.saturating_add(1);
                }
            }
            after = rows
                .last()
                .map(|row| core_meta_record_tuple_key(&row.key).map(ToOwned::to_owned))
                .transpose()?;
            if rows.len() < TEST_PAYLOAD_REFERENCE_PAGE_ROWS {
                break;
            }
        }
        Ok(count)
    }
}

fn object_has_live_payload_reference_edges(object: &Object) -> bool {
    object_has_payload_reference_target(object) && object.deleted_at.is_none()
}

fn object_has_payload_reference_target(object: &Object) -> bool {
    object.kind == object_links::ObjectEntryKind::Blob && object.shard_map.is_some()
}

fn validate_payload_reference_scope(bucket: &Bucket, object: &Object) -> Result<()> {
    if bucket.tenant_id != object.tenant_id || bucket.id != object.bucket_id {
        bail!("CoreStore payload reference scope mismatch");
    }
    Ok(())
}

enum PayloadDataTarget {
    LogicalFile {
        locator: CoreManifestLocator,
    },
    ObjectRef {
        object_ref: CoreObjectRef,
        target: String,
    },
}

fn object_data_target_from_json(value: &JsonValue) -> Result<PayloadDataTarget> {
    if value.get("schema").and_then(JsonValue::as_str) != Some("anvil.core.object_data_target.v1") {
        bail!("CoreStore object metadata shard map is not canonical object data target");
    }
    let kind = value
        .get("kind")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("CoreStore object metadata shard map kind is missing"))?;
    let target = value
        .get("target")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("CoreStore object metadata shard map target is missing"))?;
    match kind {
        "logical_file" => {
            let bytes = URL_SAFE_NO_PAD
                .decode(target)
                .context("CoreStore object metadata logical-file target is not base64url")?;
            Ok(PayloadDataTarget::LogicalFile {
                locator: decode_manifest_locator_proto(&bytes)?,
            })
        }
        "object_ref" => Ok(PayloadDataTarget::ObjectRef {
            object_ref: decode_core_object_ref_target(target)?,
            target: target.to_string(),
        }),
        other => bail!("CoreStore object metadata shard-map kind {other:?} is unsupported"),
    }
}

fn object_ref_payload_identity(storage_kind: &str, object_ref: &CoreObjectRef) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        storage_kind,
        object_ref.encoding.profile_id,
        object_ref.encoding.block_id,
        object_ref.hash,
        object_ref.logical_size
    )
}

fn logical_block_payload_identity(
    block: &CoreLogicalBlockRef,
    manifest: &CoreLogicalFileManifest,
) -> String {
    format!(
        "erasure_block:{}:{}",
        manifest.erasure_profile_id, block.block_id
    )
}

#[cfg(test)]
fn payload_reference_prefix(payload_identity: &str) -> Result<Vec<u8>> {
    let hash = payload_reference_identity_hash(payload_identity);
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("payload-reference"),
        CoreMetaTuplePart::Hash(&hash),
    ])
}

fn payload_reference_key(payload_identity: &str, object: &Object) -> Result<Vec<u8>> {
    let hash = payload_reference_identity_hash(payload_identity);
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("payload-reference"),
        CoreMetaTuplePart::Hash(&hash),
        CoreMetaTuplePart::Utf8("object-version"),
        CoreMetaTuplePart::I64(object.tenant_id),
        CoreMetaTuplePart::I64(object.bucket_id),
        CoreMetaTuplePart::Utf8(&object.key),
        CoreMetaTuplePart::Utf8(&object.version_id.to_string()),
    ])
}

fn payload_reference_common(
    payload_identity: &str,
    root_generation: u64,
    transaction_id: &str,
    created_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        payload_reference_root_anchor_key(payload_identity),
        payload_reference_root_key_hash(payload_identity),
        root_generation,
        transaction_id,
        created_at_unix_nanos,
    )
}

fn payload_reference_root_key_hash(payload_identity: &str) -> String {
    core_meta_root_key_hash(&payload_reference_root_anchor_key(payload_identity))
}

fn payload_reference_root_anchor_key(payload_identity: &str) -> String {
    format!(
        "payload-reference/{}",
        payload_reference_identity_hash(payload_identity)
    )
}

fn payload_reference_identity_hash(payload_identity: &str) -> String {
    format!("sha256:{}", sha256_hex(payload_identity.as_bytes()))
}

#[cfg(test)]
fn decode_payload_reference_row(bytes: &[u8]) -> Result<PayloadReferenceRowProto> {
    let row = decode_deterministic_proto::<PayloadReferenceRowProto>(
        bytes,
        "CoreStore payload reference row",
    )?;
    if row.schema != CORE_PAYLOAD_REFERENCE_SCHEMA {
        bail!("CoreStore payload reference row has invalid schema");
    }
    if row.common.is_none() {
        bail!("CoreStore payload reference row is missing CoreMeta common");
    }
    Ok(row)
}
