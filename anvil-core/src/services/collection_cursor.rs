use crate::anvil_api::{PageRequest, PageResponse};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Code, Status};

type HmacSha256 = Hmac<Sha256>;

const TOKEN_VERSION: u8 = 1;
const TOKEN_DOMAIN: &[u8] = b"anvil.collection.page-token.v1";
const TOKEN_TTL_SECONDS: u64 = 15 * 60;
const MAX_ENCODED_TOKEN_BYTES: usize = 32 * 1024;
const MAX_POSITION_TEXT_BYTES: usize = 16 * 1024;
const MAX_BINARY_POSITION_BYTES: usize = 8 * 1024;
pub(crate) const DEFAULT_PAGE_SIZE: usize = 100;
pub(crate) const MAX_PAGE_SIZE: usize = 1000;

#[derive(Debug, Clone, Copy)]
pub struct CollectionCursorBinding<'a> {
    pub service_method: &'static str,
    pub filters: &'a [(&'a str, &'a str)],
    pub principal_scope: &'a str,
    pub page_size: usize,
    pub revision: &'a str,
    pub sort: &'static str,
}

pub(crate) struct CollectionPage<T> {
    pub items: Vec<T>,
    pub next_position: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CollectionCursorToken {
    position: String,
    filter_hash: String,
    principal_hash: String,
    expires_at_unix_seconds: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct CollectionCursorTokenProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    service_method: String,
    #[prost(string, tag = "3")]
    position: String,
    #[prost(string, tag = "4")]
    filter_hash: String,
    #[prost(string, tag = "5")]
    principal_hash: String,
    #[prost(uint32, tag = "6")]
    page_size: u32,
    #[prost(string, tag = "7")]
    revision: String,
    #[prost(string, tag = "8")]
    sort: String,
    #[prost(uint64, tag = "9")]
    expires_at_unix_seconds: u64,
    #[prost(string, tag = "10")]
    signature: String,
}

pub fn page_size(page: Option<&PageRequest>) -> Result<usize, Status> {
    let requested = page.map(|page| page.page_size).unwrap_or_default();
    if requested == 0 {
        return Ok(DEFAULT_PAGE_SIZE);
    }
    let requested = usize::try_from(requested)
        .map_err(|_| Status::invalid_argument("page_size exceeds supported range"))?;
    if requested > MAX_PAGE_SIZE {
        return Err(Status::invalid_argument(format!(
            "page_size must not exceed {MAX_PAGE_SIZE}"
        )));
    }
    Ok(requested)
}

pub fn decode_page_token(
    page: Option<&PageRequest>,
    binding: &CollectionCursorBinding<'_>,
    signing_key: &[u8],
) -> Result<Option<String>, Status> {
    let Some(encoded) = page
        .map(|page| page.page_token.trim())
        .filter(|token| !token.is_empty())
    else {
        return Ok(None);
    };
    if encoded.len() > MAX_ENCODED_TOKEN_BYTES {
        return Err(invalid_token());
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| invalid_token())?;
    let proto = crate::core_store::decode_deterministic_proto::<CollectionCursorTokenProto>(
        &bytes,
        "collection page token",
    )
    .map_err(|_| invalid_token())?;
    validate_proto(&proto, binding, signing_key)?;
    Ok(Some(proto.position))
}

pub fn encode_next_page_token(
    position: &str,
    binding: &CollectionCursorBinding<'_>,
    signing_key: &[u8],
) -> Result<String, Status> {
    if position.is_empty() || position.len() > MAX_POSITION_TEXT_BYTES {
        return Err(Status::internal(
            "collection page position is outside supported bounds",
        ));
    }
    let token = CollectionCursorToken {
        position: position.to_string(),
        filter_hash: filter_hash(binding.filters),
        principal_hash: principal_hash(binding.principal_scope),
        expires_at_unix_seconds: now_unix_seconds()?.saturating_add(TOKEN_TTL_SECONDS),
    };
    let mut proto = token_proto(&token, binding)?;
    proto.signature = sign(&proto, signing_key)?;
    let bytes = crate::core_store::encode_deterministic_proto(&proto);
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

pub fn encode_binary_position(position: &[u8]) -> Result<String, Status> {
    if position.is_empty() || position.len() > MAX_BINARY_POSITION_BYTES {
        return Err(Status::internal(
            "collection binary page position is outside supported bounds",
        ));
    }
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(position))
}

pub fn decode_binary_position(position: Option<&str>) -> Result<Option<Vec<u8>>, Status> {
    position
        .map(|position| {
            if position.is_empty() || position.len() > MAX_POSITION_TEXT_BYTES {
                return Err(invalid_token());
            }
            let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(position)
                .map_err(|_| invalid_token())?;
            if decoded.is_empty() || decoded.len() > MAX_BINARY_POSITION_BYTES {
                return Err(invalid_token());
            }
            Ok(decoded)
        })
        .transpose()
}

pub(crate) fn collection_revision<'a>(records: impl IntoIterator<Item = (&'a str, u64)>) -> String {
    let mut records = records
        .into_iter()
        .map(|(key, generation)| (key.to_string(), generation))
        .collect::<Vec<_>>();
    records.sort_unstable();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.collection.revision.v1");
    for (key, generation) in records {
        update_hash_part(&mut hasher, key.as_bytes());
        hasher.update(&generation.to_le_bytes());
    }
    hex::encode(hasher.finalize().as_bytes())
}

pub(crate) fn content_generation(parts: &[&[u8]]) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.collection.item-generation.v1");
    for part in parts {
        update_hash_part(&mut hasher, part);
    }
    let mut generation = [0_u8; 8];
    generation.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    u64::from_le_bytes(generation)
}

pub(crate) fn paginate<T>(
    items: Vec<T>,
    page: Option<&PageRequest>,
    service_method: &'static str,
    filters: &[(&str, &str)],
    principal_scope: &str,
    sort: &'static str,
    signing_key: &[u8],
    key: impl Fn(&T) -> &str,
    generation: impl Fn(&T) -> u64,
) -> Result<(Vec<T>, PageResponse), Status> {
    let page_size = page_size(page)?;
    let revision = collection_revision(items.iter().map(|item| (key(item), generation(item))));
    let binding = CollectionCursorBinding {
        service_method,
        filters,
        principal_scope,
        page_size,
        revision: &revision,
        sort,
    };
    let after = decode_page_token(page, &binding, signing_key)?;
    let page = ordered_page(items, after.as_deref(), page_size, &key)?;
    let next_page_token = page
        .next_position
        .as_deref()
        .map(|position| encode_next_page_token(position, &binding, signing_key))
        .transpose()?
        .unwrap_or_default();
    Ok((page.items, PageResponse { next_page_token }))
}

pub(crate) fn ordered_page<T>(
    mut items: Vec<T>,
    after: Option<&str>,
    limit: usize,
    key: impl Fn(&T) -> &str,
) -> Result<CollectionPage<T>, Status> {
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(Status::invalid_argument(
            "page_size is outside supported bounds",
        ));
    }
    items.sort_by(|left, right| key(left).cmp(key(right)));
    let start = after
        .map(|position| items.partition_point(|item| key(item) <= position))
        .unwrap_or_default();
    let mut page = items
        .into_iter()
        .skip(start)
        .take(limit + 1)
        .collect::<Vec<_>>();
    let has_more = page.len() > limit;
    if has_more {
        page.truncate(limit);
    }
    let next_position = has_more
        .then(|| page.last().map(|item| key(item).to_string()))
        .flatten();
    Ok(CollectionPage {
        items: page,
        next_position,
    })
}

fn validate_proto(
    proto: &CollectionCursorTokenProto,
    binding: &CollectionCursorBinding<'_>,
    signing_key: &[u8],
) -> Result<(), Status> {
    if proto.version != u32::from(TOKEN_VERSION)
        || proto.service_method != binding.service_method
        || proto.filter_hash != filter_hash(binding.filters)
        || proto.principal_hash != principal_hash(binding.principal_scope)
        || proto.page_size != page_size_u32(binding.page_size)?
        || proto.revision != binding.revision
        || proto.sort != binding.sort
        || proto.expires_at_unix_seconds < now_unix_seconds()?
        || proto.position.is_empty()
        || proto.position.len() > MAX_POSITION_TEXT_BYTES
    {
        return Err(invalid_token());
    }
    let expected = sign(proto, signing_key)?;
    if !constant_time_eq::constant_time_eq(proto.signature.as_bytes(), expected.as_bytes()) {
        return Err(invalid_token());
    }
    Ok(())
}

fn token_proto(
    token: &CollectionCursorToken,
    binding: &CollectionCursorBinding<'_>,
) -> Result<CollectionCursorTokenProto, Status> {
    Ok(CollectionCursorTokenProto {
        version: u32::from(TOKEN_VERSION),
        service_method: binding.service_method.to_string(),
        position: token.position.clone(),
        filter_hash: token.filter_hash.clone(),
        principal_hash: token.principal_hash.clone(),
        page_size: page_size_u32(binding.page_size)?,
        revision: binding.revision.to_string(),
        sort: binding.sort.to_string(),
        expires_at_unix_seconds: token.expires_at_unix_seconds,
        signature: String::new(),
    })
}

fn sign(proto: &CollectionCursorTokenProto, signing_key: &[u8]) -> Result<String, Status> {
    let mut mac = HmacSha256::new_from_slice(signing_key)
        .map_err(|_| Status::internal("invalid page-token signing key"))?;
    mac.update(TOKEN_DOMAIN);
    mac.update(&proto.version.to_le_bytes());
    for part in [
        proto.service_method.as_bytes(),
        proto.position.as_bytes(),
        proto.filter_hash.as_bytes(),
        proto.principal_hash.as_bytes(),
        proto.revision.as_bytes(),
        proto.sort.as_bytes(),
    ] {
        update_mac_part(&mut mac, part);
    }
    mac.update(&proto.page_size.to_le_bytes());
    mac.update(&proto.expires_at_unix_seconds.to_le_bytes());
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn page_size_u32(page_size: usize) -> Result<u32, Status> {
    if !(1..=MAX_PAGE_SIZE).contains(&page_size) {
        return Err(Status::invalid_argument(
            "page_size is outside supported bounds",
        ));
    }
    u32::try_from(page_size).map_err(|_| Status::invalid_argument("page_size is invalid"))
}

fn filter_hash(filters: &[(&str, &str)]) -> String {
    let mut filters = filters.to_vec();
    filters.sort_unstable();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.collection.filters.v1");
    for (name, value) in filters {
        update_hash_part(&mut hasher, name.as_bytes());
        update_hash_part(&mut hasher, value.as_bytes());
    }
    hex::encode(hasher.finalize().as_bytes())
}

fn principal_hash(principal_scope: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.collection.principal.v1");
    update_hash_part(&mut hasher, principal_scope.as_bytes());
    hex::encode(hasher.finalize().as_bytes())
}

fn update_hash_part(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn update_mac_part(mac: &mut HmacSha256, value: &[u8]) {
    mac.update(&(value.len() as u64).to_le_bytes());
    mac.update(value);
}

fn now_unix_seconds() -> Result<u64, Status> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| Status::internal("system clock predates Unix epoch"))
}

fn invalid_token() -> Status {
    Status::new(Code::InvalidArgument, "invalid collection page token")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trip_binds_scope_principal_and_page_size() {
        let filters = [("tenant", "7")];
        let binding = CollectionCursorBinding {
            service_method: "anvil.BucketService/ListBuckets",
            filters: &filters,
            principal_scope: "tenant:7/user:alice",
            page_size: 25,
            revision: "root:9",
            sort: "bucket_name.asc",
        };
        let token = encode_next_page_token("photos", &binding, b"test-key").unwrap();
        let page = PageRequest {
            page_size: 25,
            page_token: token,
        };
        assert_eq!(
            decode_page_token(Some(&page), &binding, b"test-key").unwrap(),
            Some("photos".to_string())
        );

        let other = CollectionCursorBinding {
            principal_scope: "tenant:7/user:bob",
            ..binding
        };
        assert_eq!(
            decode_page_token(Some(&page), &other, b"test-key")
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );

        let changed_revision = CollectionCursorBinding {
            revision: "root:10",
            ..binding
        };
        assert_eq!(
            decode_page_token(Some(&page), &changed_revision, b"test-key")
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn page_size_defaults_and_rejects_oversized_requests() {
        assert_eq!(page_size(None).unwrap(), DEFAULT_PAGE_SIZE);
        assert_eq!(
            page_size(Some(&PageRequest {
                page_size: 0,
                page_token: String::new(),
            }))
            .unwrap(),
            DEFAULT_PAGE_SIZE
        );
        assert_eq!(
            page_size(Some(&PageRequest {
                page_size: 1001,
                page_token: String::new(),
            }))
            .unwrap_err()
            .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn ordered_page_seeks_after_the_bound_position() {
        let page = ordered_page(vec!["c", "a", "b"], Some("a"), 1, |item| *item).unwrap();
        assert_eq!(page.items, vec!["b"]);
        assert_eq!(page.next_position.as_deref(), Some("b"));
    }

    #[test]
    fn binary_positions_round_trip_and_reject_oversized_values() {
        let position = vec![0, 1, 2, 0xff];
        let encoded = encode_binary_position(&position).unwrap();
        assert_eq!(
            decode_binary_position(Some(&encoded)).unwrap(),
            Some(position)
        );
        assert!(encode_binary_position(&vec![0; MAX_BINARY_POSITION_BYTES + 1]).is_err());
    }
}
