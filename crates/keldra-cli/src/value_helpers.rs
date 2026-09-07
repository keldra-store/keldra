use anyhow::{Context, Result, bail};
use keldra_storage::v1::clone_object_request::Operation as CloneOperation;
use keldra_storage::v1::put_header::Operation as PutOperation;
use keldra_storage::v1::{
    ObjectAddress, ObjectVersioning, PutIfAbsentOperation, PutIfVersionOperation,
    PutImmutableOperation, PutOperation as UnconditionalPutOperation,
};

pub(super) fn address(tenant: String, bucket: String, path: String) -> ObjectAddress {
    ObjectAddress {
        tenant,
        bucket,
        path,
    }
}

pub(super) fn versioning_name(value: i32) -> Result<&'static str> {
    match ObjectVersioning::try_from(value) {
        Ok(ObjectVersioning::Unversioned) => Ok("unversioned"),
        Ok(ObjectVersioning::Enabled) => Ok("enabled"),
        Err(_) => bail!("server returned an unknown object versioning mode"),
    }
}

pub(super) fn put_operation(
    if_absent: bool,
    if_version: Option<u64>,
    immutable: bool,
) -> Result<PutOperation> {
    match (if_absent, if_version, immutable) {
        (true, None, false) => Ok(PutOperation::PutIfAbsent(PutIfAbsentOperation {})),
        (false, Some(expected_version), false) => {
            Ok(PutOperation::PutIfVersion(PutIfVersionOperation {
                expected_version,
            }))
        }
        (false, None, true) => Ok(PutOperation::PutImmutable(PutImmutableOperation {})),
        (false, None, false) => Ok(PutOperation::Put(UnconditionalPutOperation {})),
        _ => bail!("--if-absent, --if-version, and --immutable are mutually exclusive"),
    }
}

pub(super) fn clone_operation(if_absent: bool, if_version: Option<u64>) -> CloneOperation {
    match (if_absent, if_version) {
        (true, None) => CloneOperation::PutIfAbsent(PutIfAbsentOperation {}),
        (false, Some(expected_version)) => {
            CloneOperation::PutIfVersion(PutIfVersionOperation { expected_version })
        }
        (false, None) => CloneOperation::Put(UnconditionalPutOperation {}),
        (true, Some(_)) => unreachable!("clap rejects conflicting clone conditions"),
    }
}

pub(super) fn parse_hex(value: &str) -> Result<Vec<u8>> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("program hash must be the 64-digit BLAKE3 hash of the stored definition bytes");
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("hexadecimal input is ASCII");
            u8::from_str_radix(pair, 16).context("invalid program hash")
        })
        .collect()
}

pub(super) fn lower_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

pub(super) fn present_head_line(
    version: u64,
    content_length: u64,
    content_hash: &[u8],
) -> Result<String> {
    if content_hash.len() != 32 {
        bail!("present object has an invalid content hash");
    }
    Ok(format!(
        "present version={version} bytes={content_length} blake3={}",
        lower_hex(content_hash)
    ))
}
