use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value as JsonValue;

use crate::core_store::{decode_core_object_ref_target, decode_manifest_locator_proto};

pub(super) fn object_data_target_kind(value: &JsonValue) -> Result<String> {
    match value.get("schema").and_then(JsonValue::as_str) {
        Some("anvil.mvcc.local_object_manifest.v1") => return Ok("mvcc_local".to_string()),
        Some("anvil.mvcc.object_shard_manifest.v1") => return Ok("mvcc_shards".to_string()),
        _ => {}
    }
    if value.get("schema").and_then(JsonValue::as_str) != Some("anvil.core.object_data_target.v1") {
        return Err(anyhow!(
            "object metadata shard map is not a canonical CoreStore object data target"
        ));
    }
    let kind = value
        .get("kind")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("object metadata shard map kind is missing"))?;
    match kind {
        "logical_file" | "object_ref" => Ok(kind.to_string()),
        other => Err(anyhow!("unsupported object data target kind {other}")),
    }
}

pub(super) fn object_data_target_bytes(value: &JsonValue) -> Result<Vec<u8>> {
    let kind = object_data_target_kind(value)?;
    if matches!(kind.as_str(), "mvcc_local" | "mvcc_shards") {
        return serde_json::to_vec(&canonical_json(value)).map_err(Into::into);
    }
    let target = value
        .get("target")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("object metadata shard map target is missing"))?;
    match kind.as_str() {
        "logical_file" => {
            let bytes = URL_SAFE_NO_PAD
                .decode(target)
                .context("object metadata logical-file target is not base64url")?;
            decode_manifest_locator_proto(&bytes)?;
            Ok(bytes)
        }
        "object_ref" => {
            decode_core_object_ref_target(target)?;
            Ok(target.as_bytes().to_vec())
        }
        other => Err(anyhow!("unsupported object data target kind {other}")),
    }
}

pub(super) fn shard_map_from_object_data_target(kind: &str, target: &[u8]) -> Result<JsonValue> {
    match kind {
        "logical_file" => {
            decode_manifest_locator_proto(target)?;
            Ok(serde_json::json!({
                "schema": "anvil.core.object_data_target.v1",
                "kind": "logical_file",
                "target": URL_SAFE_NO_PAD.encode(target),
            }))
        }
        "mvcc_local" | "mvcc_shards" => {
            let value: JsonValue = serde_json::from_slice(target)?;
            if serde_json::to_vec(&canonical_json(&value))? != target {
                return Err(anyhow!("MVCC object data target is not canonical JSON"));
            }
            let expected = if kind == "mvcc_local" {
                "anvil.mvcc.local_object_manifest.v1"
            } else {
                "anvil.mvcc.object_shard_manifest.v1"
            };
            if value.get("schema").and_then(JsonValue::as_str) != Some(expected) {
                return Err(anyhow!(
                    "MVCC object data target schema does not match kind"
                ));
            }
            Ok(value)
        }
        "object_ref" => {
            let target = std::str::from_utf8(target)
                .context("object metadata object-ref target is not UTF-8")?;
            decode_core_object_ref_target(target)?;
            Ok(serde_json::json!({
                "schema": "anvil.core.object_data_target.v1",
                "kind": "object_ref",
                "target": target,
            }))
        }
        other => Err(anyhow!("unsupported object data target kind {other}")),
    }
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            JsonValue::Object(
                keys.into_iter()
                    .map(|key| (key.clone(), canonical_json(&map[key])))
                    .collect(),
            )
        }
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(canonical_json).collect()),
        scalar => scalar.clone(),
    }
}
